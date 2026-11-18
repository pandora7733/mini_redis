//! 영속성 테스트: 정상 재시작, 크래시 복구, 잘린 로그, 손상 로그, 멱등 재실행, AOF rewrite

use std::fs;
use std::io::Write;
use std::time::Duration;

use mini_redis::engine::Engine;
use mini_redis::now_ms;
use mini_redis::persistence::{
    apply_record, FsyncPolicy, LogRecord, Persistence, PersistenceConfig, AOF_FILE, SNAPSHOT_FILE,
};
use mini_redis::store::{SetCondition, Ttl};
use tempfile::tempdir;

fn cfg(dir: &std::path::Path) -> PersistenceConfig {
    PersistenceConfig::new(dir)
}

async fn value(engine: &Engine, key: &str) -> Option<String> {
    engine.get(key.to_string()).await.unwrap().map(|v| v.value)
}

#[tokio::test]
async fn data_survives_graceful_restart() {
    let dir = tempdir().unwrap();
    {
        let (engine, report) = Engine::open(cfg(dir.path())).unwrap();
        assert_eq!(report.live_keys, 0);
        engine.set("a".into(), "1".into(), None, SetCondition::Always).await.unwrap();
        engine.set("ttl".into(), "x".into(), Some(60_000), SetCondition::Always).await.unwrap();
        engine.set("gone".into(), "x".into(), None, SetCondition::Always).await.unwrap();
        engine.del("gone".into()).await.unwrap();
        engine.set("short".into(), "x".into(), Some(50), SetCondition::Always).await.unwrap();
        engine.incr("counter".into(), 41).await.unwrap().unwrap();
        engine.incr("counter".into(), 1).await.unwrap().unwrap();
        engine.shutdown().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (engine, report) = Engine::open(cfg(dir.path())).unwrap();
    assert!(report.snapshot_keys >= 3, "정상 종료 시 스냅샷 저장: {report:?}");
    assert_eq!(report.aof_records, 0, "스냅샷 후 AOF 는 비워짐");
    assert_eq!(value(&engine, "a").await.as_deref(), Some("1"));
    assert_eq!(value(&engine, "counter").await.as_deref(), Some("42"));
    assert_eq!(value(&engine, "gone").await, None);
    assert_eq!(value(&engine, "short").await, None, "재시작 중 만료된 키는 복구되지 않음");
    match engine.ttl("ttl".into()).await.unwrap() {
        Ttl::ExpiresIn(ms) => assert!(ms > 50_000 && ms <= 60_000, "TTL 이 이어짐: {ms}"),
        other => panic!("unexpected ttl {other:?}"),
    }
}

#[tokio::test]
async fn crash_recovers_from_aof() {
    let dir = tempdir().unwrap();
    {
        let (engine, _) = Engine::open(cfg(dir.path())).unwrap();
        for i in 0..100 {
            engine
                .set(format!("k{i}"), format!("v{i}"), None, SetCondition::Always)
                .await
                .unwrap();
        }
        engine.del("k0".into()).await.unwrap();
        engine.expire("k1".into(), 60_000).await.unwrap();
        engine.delete_matching("k9*".into()).await.unwrap();
        engine.simulate_crash().await.unwrap(); // 스냅샷 없이 종료 (kill -9)
    }
    assert!(!dir.path().join(SNAPSHOT_FILE).exists());

    let (engine, report) = Engine::open(cfg(dir.path())).unwrap();
    assert_eq!(report.snapshot_keys, 0);
    assert!(report.aof_records > 100);
    assert_eq!(value(&engine, "k0").await, None);
    assert_eq!(value(&engine, "k2").await.as_deref(), Some("v2"));
    assert_eq!(value(&engine, "k95").await, None, "패턴 삭제도 복구됨");
    assert!(matches!(engine.ttl("k1".into()).await.unwrap(), Ttl::ExpiresIn(_)));
    // k0 삭제, k9 + k90..k99 (11개) 삭제 → 88개
    assert_eq!(engine.stats().await.unwrap().store.keys, 88);
}

#[tokio::test]
async fn flush_is_persisted() {
    let dir = tempdir().unwrap();
    {
        let (engine, _) = Engine::open(cfg(dir.path())).unwrap();
        engine.set("a".into(), "1".into(), None, SetCondition::Always).await.unwrap();
        engine.flush().await.unwrap();
        engine.set("b".into(), "2".into(), None, SetCondition::Always).await.unwrap();
        engine.simulate_crash().await.unwrap();
    }
    let (engine, _) = Engine::open(cfg(dir.path())).unwrap();
    assert_eq!(value(&engine, "a").await, None);
    assert_eq!(value(&engine, "b").await.as_deref(), Some("2"));
}

#[test]
fn torn_tail_write_is_truncated() {
    let dir = tempdir().unwrap();
    let aof = dir.path().join(AOF_FILE);
    let good = "{\"op\":\"set\",\"key\":\"a\",\"value\":\"1\"}\n";
    fs::write(&aof, format!("{good}{{\"op\":\"set\",\"key\":\"b\",\"va")).unwrap();

    let (mut p, mut store) = Persistence::open(cfg(dir.path())).unwrap();
    assert!(p.load_report().truncated_bytes > 0);
    assert_eq!(store.get("a", now_ms()).map(|e| e.value.clone()), Some("1".into()));
    assert!(store.get("b", now_ms()).is_none());
    assert_eq!(fs::metadata(&aof).unwrap().len(), good.len() as u64, "잘린 꼬리 제거");

    // 이후 append 는 깨끗한 줄 경계에서 이어짐
    p.append(&LogRecord::Set {
        key: "c".into(),
        value: "3".into(),
        expire_at: None,
    })
    .unwrap();
    drop(p);
    let (_, mut store) = Persistence::open(cfg(dir.path())).unwrap();
    assert!(store.get("a", now_ms()).is_some());
    assert!(store.get("c", now_ms()).is_some());
}

#[test]
fn corrupted_middle_record_refuses_to_start() {
    let dir = tempdir().unwrap();
    let mut f = fs::File::create(dir.path().join(AOF_FILE)).unwrap();
    writeln!(f, "{{\"op\":\"set\",\"key\":\"a\",\"value\":\"1\"}}").unwrap();
    writeln!(f, "garbage-line").unwrap();
    writeln!(f, "{{\"op\":\"set\",\"key\":\"b\",\"value\":\"2\"}}").unwrap();
    drop(f);
    let err = Persistence::open(cfg(dir.path())).err().expect("must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains(":2"), "몇 번째 줄인지 알려줌: {err}");
}

#[test]
fn replay_after_snapshot_is_idempotent() {
    // 스냅샷 rename 후 AOF 를 비우기 전에 죽은 상황: 스냅샷 + (이미 반영된) AOF 가 함께 남음
    let dir = tempdir().unwrap();
    let (mut p, mut store) = Persistence::open(cfg(dir.path())).unwrap();
    let now = now_ms();
    let records = vec![
        LogRecord::Set { key: "a".into(), value: "1".into(), expire_at: None },
        LogRecord::Set { key: "b".into(), value: "2".into(), expire_at: Some(now + 60_000) },
        LogRecord::Del { key: "a".into() },
        LogRecord::Set { key: "a".into(), value: "3".into(), expire_at: None },
        LogRecord::Flush,
        LogRecord::Set { key: "c".into(), value: "4".into(), expire_at: None },
        LogRecord::Expire { key: "c".into(), expire_at: now + 60_000 },
        LogRecord::Persist { key: "c".into() },
        LogRecord::Set { key: "d".into(), value: "5".into(), expire_at: None },
    ];
    for r in &records {
        p.append(r).unwrap();
        apply_record(&mut store, r.clone(), now);
    }
    let aof_bytes = fs::read(dir.path().join(AOF_FILE)).unwrap();
    p.rewrite(&store, now).unwrap();
    drop(p);
    fs::write(dir.path().join(AOF_FILE), &aof_bytes).unwrap(); // AOF 비우기 "실패"

    let (_, mut restored) = Persistence::open(cfg(dir.path())).unwrap();
    let mut expected: Vec<_> = store.iter().map(|(k, e)| (k.clone(), e.clone())).collect();
    let mut actual: Vec<_> = restored.iter().map(|(k, e)| (k.clone(), e.clone())).collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    actual.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(actual, expected);
    assert_eq!(restored.ttl("c", now), Ttl::Persistent);
    assert!(restored.get("a", now).is_none());
}

#[tokio::test]
async fn aof_is_compacted_when_threshold_exceeded() {
    let dir = tempdir().unwrap();
    let mut config = cfg(dir.path());
    config.rewrite_threshold_bytes = 2_048;
    let (engine, _) = Engine::open(config.clone()).unwrap();
    for i in 0..200 {
        // 같은 키를 계속 덮어써서 로그만 커지는 상황
        engine
            .set("hot".into(), format!("value-{i}"), None, SetCondition::Always)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(400)).await; // 백그라운드 tick 대기
    let stats = engine.stats().await.unwrap();
    let aof_bytes = stats.persistence.unwrap().aof_bytes;
    assert!(aof_bytes < 2_048, "rewrite 후 AOF 가 줄어야 함: {aof_bytes}");
    assert!(dir.path().join(SNAPSHOT_FILE).exists());
    engine.simulate_crash().await.unwrap();

    let (engine, _) = Engine::open(config).unwrap();
    assert_eq!(value(&engine, "hot").await.as_deref(), Some("value-199"));
}

#[tokio::test]
async fn manual_save_endpoint_logic() {
    let dir = tempdir().unwrap();
    let (engine, _) = Engine::open(cfg(dir.path())).unwrap();
    engine.set("a".into(), "1".into(), None, SetCondition::Always).await.unwrap();
    engine.set("b".into(), "2".into(), Some(60_000), SetCondition::Always).await.unwrap();
    assert_eq!(engine.save().await.unwrap(), 2);
    let stats = engine.stats().await.unwrap();
    assert_eq!(stats.persistence.unwrap().aof_bytes, 0);
}

#[test]
fn fsync_policy_parsing() {
    assert_eq!("always".parse::<FsyncPolicy>().unwrap(), FsyncPolicy::Always);
    assert_eq!("EverySec".parse::<FsyncPolicy>().unwrap(), FsyncPolicy::EverySec);
    assert_eq!("no".parse::<FsyncPolicy>().unwrap(), FsyncPolicy::No);
    assert!("sometimes".parse::<FsyncPolicy>().is_err());
}

#[tokio::test]
async fn fsync_always_mode_works() {
    let dir = tempdir().unwrap();
    let mut config = cfg(dir.path());
    config.fsync = FsyncPolicy::Always;
    {
        let (engine, _) = Engine::open(config.clone()).unwrap();
        engine.set("a".into(), "1".into(), None, SetCondition::Always).await.unwrap();
        engine.simulate_crash().await.unwrap();
    }
    let (engine, _) = Engine::open(config).unwrap();
    assert_eq!(value(&engine, "a").await.as_deref(), Some("1"));
}
