//! 단일 스레드 실행 엔진 (Actor 모델) — 동시성 설계의 핵심
//!
//! ```text
//!   HTTP 요청 (tokio 멀티스레드)            엔진 스레드 (단 1개)
//!   ┌──────────┐                          ┌───────────────────────────┐
//!   │ handler1 │──┐   Job(클로저)          │ loop {                    │
//!   ├──────────┤  ├─▶ [ mpsc 큐 ] ───────▶│   job = recv()            │
//!   │ handler2 │──┤   (FIFO, 순서 보장)    │   job(&mut store) ◀ 독점 │
//!   ├──────────┤  │                       │   reply via oneshot       │
//!   │ handlerN │──┘ ◀──── oneshot 응답 ────│   100ms 마다 만료 정리     │
//!   └──────────┘                          │   1초마다 fsync           │
//!                                         └───────────────────────────┘
//! ```
//!
//! * `Store` 는 엔진 스레드만 소유합니다. 다른 스레드는 메시지로 "부탁"만 할 수 있습니다.
//!   → 공유 가변 상태가 없으므로 **락이 필요 없고 데이터 경쟁이 원천적으로 불가능**합니다.
//!   (Rust 타입 시스템이 컴파일 타임에 보장: `Store` 가 `Arc<Mutex<..>>` 로 공유되지 않음)
//! * 명령은 큐에 들어온 순서대로 하나씩 끝까지 실행되므로 각 명령이 원자적입니다.
//!   예) 1,000개의 동시 INCR 도 정확히 1,000 증가. "읽고-수정-쓰기" 경쟁이 없습니다.
//! * AOF 에 기록되는 순서 = 실제 실행 순서이므로 복구 결과가 항상 일관됩니다.
//! * 네트워크 I/O·JSON 파싱은 tokio 워커들이 병렬로 처리하고, 엔진은 메모리 연산만 하므로
//!   (실제 Redis 와 같은 구조) 단일 스레드여도 충분히 빠릅니다.

use std::io;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::now_ms;
use crate::persistence::{LoadReport, LogRecord, Persistence, PersistenceConfig, PersistenceStats};
use crate::store::{SetCondition, Store, StoreError, StoreStats, Ttl};

/// 백그라운드 작업(active expiration 등) 주기
const TICK_INTERVAL: Duration = Duration::from_millis(100);
/// everysec fsync 주기
const FSYNC_INTERVAL: Duration = Duration::from_secs(1);
/// tick 한 번에 검사할 만료 항목 상한 (엔진을 오래 붙잡지 않도록)
const ACTIVE_EXPIRE_BUDGET: usize = 2_000;

type Job = Box<dyn FnOnce(&mut EngineState) + Send + 'static>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    #[error("storage engine is not running")]
    Closed,
    #[error("persistence is disabled on this server")]
    PersistenceDisabled,
    #[error("I/O error: {0}")]
    Io(String),
}

/// GET 결과
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueInfo {
    pub value: String,
    /// 남은 TTL (ms), 영구 키면 None
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStats {
    #[serde(flatten)]
    pub store: StoreStats,
    pub processed_commands: u64,
    pub persistence: Option<PersistenceStats>,
}

/// 엔진 핸들. `Clone` 해서 여러 태스크/스레드가 공유합니다 (내부는 채널 송신자뿐).
#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Job>,
}

impl Engine {
    /// 영속성 없이 메모리에서만 동작하는 엔진
    pub fn in_memory() -> Self {
        Self::spawn(Store::new(), None)
    }

    /// 데이터 디렉터리에서 복구한 뒤 AOF 를 기록하며 동작하는 엔진
    pub fn open(config: PersistenceConfig) -> io::Result<(Self, LoadReport)> {
        let (persistence, store) = Persistence::open(config)?;
        let report = persistence.load_report().clone();
        Ok((Self::spawn(store, Some(persistence)), report))
    }

    fn spawn(store: Store, persistence: Option<Persistence>) -> Self {
        let (tx, rx) = mpsc::channel::<Job>();
        let state = EngineState {
            store,
            persistence,
            shutdown_requested: false,
            finalized: false,
            processed: 0,
        };
        thread::Builder::new()
            .name("mini-redis-engine".into())
            .spawn(move || run_loop(rx, state))
            .expect("failed to spawn engine thread");
        Self { tx }
    }

    /// 클로저를 엔진 스레드로 보내 실행하고 결과를 기다립니다.
    async fn call<R, F>(&self, f: F) -> Result<R, EngineError>
    where
        R: Send + 'static,
        F: FnOnce(&mut EngineState) -> R + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job: Job = Box::new(move |state: &mut EngineState| {
            // 요청자가 이미 떠났으면(타임아웃 등) 응답은 버림
            let _ = reply_tx.send(f(state));
        });
        self.tx.send(job).map_err(|_| EngineError::Closed)?;
        reply_rx.await.map_err(|_| EngineError::Closed)
    }

    pub async fn ping(&self) -> Result<(), EngineError> {
        self.call(|_| ()).await
    }

    pub async fn get(&self, key: String) -> Result<Option<ValueInfo>, EngineError> {
        self.call(move |s| s.get(&key)).await
    }

    /// `ttl_ms` 가 Some 이면 그 시간 뒤 만료. 조건을 만족해 저장했으면 true.
    pub async fn set(
        &self,
        key: String,
        value: String,
        ttl_ms: Option<u64>,
        condition: SetCondition,
    ) -> Result<bool, EngineError> {
        self.call(move |s| s.set(key, value, ttl_ms, condition)).await
    }

    pub async fn del(&self, key: String) -> Result<bool, EngineError> {
        self.call(move |s| s.del(key)).await
    }

    pub async fn exists(&self, key: String) -> Result<bool, EngineError> {
        self.call(move |s| s.store.exists(&key, now_ms())).await
    }

    pub async fn expire(&self, key: String, ttl_ms: u64) -> Result<bool, EngineError> {
        self.call(move |s| s.expire(key, ttl_ms)).await
    }

    /// 키가 없으면 None, 있으면 TTL 을 제거했는지 여부
    pub async fn persist(&self, key: String) -> Result<Option<bool>, EngineError> {
        self.call(move |s| s.persist(key)).await
    }

    pub async fn ttl(&self, key: String) -> Result<Ttl, EngineError> {
        self.call(move |s| s.store.ttl(&key, now_ms())).await
    }

    pub async fn incr(&self, key: String, delta: i64) -> Result<Result<i64, StoreError>, EngineError> {
        self.call(move |s| s.incr(key, delta)).await
    }

    pub async fn keys(&self, pattern: String) -> Result<Vec<String>, EngineError> {
        self.call(move |s| s.store.keys(&pattern, now_ms())).await
    }

    /// 패턴에 맞는 키를 모두 삭제(무효화)하고 개수를 반환
    pub async fn delete_matching(&self, pattern: String) -> Result<usize, EngineError> {
        self.call(move |s| s.delete_matching(&pattern)).await
    }

    pub async fn flush(&self) -> Result<usize, EngineError> {
        self.call(|s| s.flush()).await
    }

    pub async fn stats(&self) -> Result<EngineStats, EngineError> {
        self.call(|s| s.stats()).await
    }

    /// 즉시 스냅샷 저장 (AOF 압축). 저장한 키 수 반환.
    pub async fn save(&self) -> Result<usize, EngineError> {
        self.call(|s| s.save()).await?
    }

    /// 정상 종료: 스냅샷을 저장하고 엔진 스레드를 멈춥니다. 이후 호출은 `Closed`.
    pub async fn shutdown(&self) -> Result<(), EngineError> {
        self.call(|s| {
            s.shutdown_requested = true;
            s.finalize()
        })
        .await?
    }

    /// 테스트용: 스냅샷 없이 엔진을 멈춰 `kill -9` 같은 프로세스 크래시를 흉내냅니다.
    #[doc(hidden)]
    pub async fn simulate_crash(&self) -> Result<(), EngineError> {
        self.call(|s| {
            s.finalized = true;
            s.shutdown_requested = true;
        })
        .await
    }
}

/// 엔진 스레드가 독점 소유하는 상태
pub struct EngineState {
    store: Store,
    persistence: Option<Persistence>,
    shutdown_requested: bool,
    finalized: bool,
    processed: u64,
}

impl EngineState {
    fn append(&mut self, record: &LogRecord) {
        if let Some(p) = self.persistence.as_mut() {
            if let Err(err) = p.append(record) {
                tracing::error!(%err, "failed to append to AOF");
            }
        }
    }

    fn get(&mut self, key: &str) -> Option<ValueInfo> {
        let now = now_ms();
        self.store.get(key, now).map(|e| ValueInfo {
            value: e.value.clone(),
            ttl_ms: e.expire_at.map(|at| at.saturating_sub(now)),
        })
    }

    fn set(&mut self, key: String, value: String, ttl_ms: Option<u64>, condition: SetCondition) -> bool {
        let now = now_ms();
        let expire_at = ttl_ms.map(|ttl| now.saturating_add(ttl));
        // 영속성이 켜져 있을 때만 로그용 복사본을 만듦
        let record = self.persistence.is_some().then(|| LogRecord::Set {
            key: key.clone(),
            value: value.clone(),
            expire_at,
        });
        let stored = self.store.set(key, value, expire_at, condition, now);
        if stored {
            if let Some(record) = record {
                self.append(&record);
            }
        }
        stored
    }

    fn del(&mut self, key: String) -> bool {
        let removed = self.store.del(&key, now_ms());
        if removed {
            self.append(&LogRecord::Del { key });
        }
        removed
    }

    fn expire(&mut self, key: String, ttl_ms: u64) -> bool {
        let now = now_ms();
        let expire_at = now.saturating_add(ttl_ms);
        let ok = self.store.expire(&key, expire_at, now);
        if ok {
            self.append(&LogRecord::Expire { key, expire_at });
        }
        ok
    }

    fn persist(&mut self, key: String) -> Option<bool> {
        let result = self.store.persist(&key, now_ms());
        if result == Some(true) {
            self.append(&LogRecord::Persist { key });
        }
        result
    }

    fn incr(&mut self, key: String, delta: i64) -> Result<i64, StoreError> {
        let next = self.store.incr(&key, delta, now_ms())?;
        // INCR 은 "결과값 SET" 으로 기록 → 재실행해도 멱등
        if self.persistence.is_some() {
            if let Some(entry) = self.store.peek(&key) {
                let record = LogRecord::Set {
                    key: key.clone(),
                    value: entry.value.clone(),
                    expire_at: entry.expire_at,
                };
                self.append(&record);
            }
        }
        Ok(next)
    }

    fn delete_matching(&mut self, pattern: &str) -> usize {
        let deleted = self.store.delete_matching(pattern, now_ms());
        let count = deleted.len();
        for key in deleted {
            self.append(&LogRecord::Del { key });
        }
        count
    }

    fn flush(&mut self) -> usize {
        let count = self.store.flush();
        self.append(&LogRecord::Flush);
        count
    }

    fn stats(&self) -> EngineStats {
        EngineStats {
            store: self.store.stats(),
            processed_commands: self.processed,
            persistence: self.persistence.as_ref().map(Persistence::stats),
        }
    }

    fn save(&mut self) -> Result<usize, EngineError> {
        let now = now_ms();
        match self.persistence.as_mut() {
            None => Err(EngineError::PersistenceDisabled),
            Some(p) => p
                .rewrite(&self.store, now)
                .map_err(|e| EngineError::Io(e.to_string())),
        }
    }

    /// 주기 작업: active expiration, fsync, AOF rewrite
    fn background_tick(&mut self, fsync_due: bool) {
        let now = now_ms();
        self.store.active_expire(now, ACTIVE_EXPIRE_BUDGET);
        if let Some(p) = self.persistence.as_mut() {
            if fsync_due {
                if let Err(err) = p.sync_if_needed() {
                    tracing::error!(%err, "AOF fsync failed");
                }
            }
            if p.needs_rewrite() {
                match p.rewrite(&self.store, now) {
                    Ok(keys) => tracing::info!(keys, "AOF rewritten into snapshot"),
                    Err(err) => tracing::error!(%err, "AOF rewrite failed"),
                }
            }
        }
    }

    /// 종료 시 마지막 스냅샷 (한 번만 실행)
    fn finalize(&mut self) -> Result<(), EngineError> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;
        if let Some(p) = self.persistence.as_mut() {
            p.rewrite(&self.store, now_ms())
                .map_err(|e| EngineError::Io(e.to_string()))?;
        }
        Ok(())
    }
}

fn run_loop(rx: mpsc::Receiver<Job>, mut state: EngineState) {
    let mut next_tick = Instant::now() + TICK_INTERVAL;
    let mut next_fsync = Instant::now() + FSYNC_INTERVAL;
    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        match rx.recv_timeout(wait) {
            Ok(job) => {
                job(&mut state);
                state.processed += 1;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            // 모든 핸들이 사라짐 → 종료
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        if state.shutdown_requested {
            break;
        }
        // 요청이 끊임없이 들어와도 주기 작업이 밀리지 않도록 매 반복마다 시간 확인
        let now = Instant::now();
        if now >= next_tick {
            let fsync_due = now >= next_fsync;
            state.background_tick(fsync_due);
            if fsync_due {
                next_fsync = now + FSYNC_INTERVAL;
            }
            next_tick = now + TICK_INTERVAL;
        }
    }
    if let Err(err) = state.finalize() {
        tracing::error!(%err, "final snapshot failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_increments_are_atomic() {
        let engine = Engine::in_memory();
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let engine = engine.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..100 {
                    engine.incr("counter".into(), 1).await.unwrap().unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let v = engine.get("counter".into()).await.unwrap().unwrap();
        assert_eq!(v.value, "10000");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_setnx_has_exactly_one_winner() {
        let engine = Engine::in_memory();
        let mut tasks = Vec::new();
        for i in 0..200 {
            let engine = engine.clone();
            tasks.push(tokio::spawn(async move {
                engine
                    .set("lock".into(), format!("owner-{i}"), None, SetCondition::IfNotExists)
                    .await
                    .unwrap()
            }));
        }
        let mut winners = 0;
        for t in tasks {
            if t.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1);
    }

    #[tokio::test]
    async fn active_expiration_runs_in_background() {
        let engine = Engine::in_memory();
        for i in 0..50 {
            engine
                .set(format!("k{i}"), "v".into(), Some(30), SetCondition::Always)
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.store.keys, 0, "조회하지 않아도 백그라운드에서 삭제");
        assert_eq!(stats.store.expired_active, 50);
    }

    #[tokio::test]
    async fn calls_fail_after_shutdown() {
        let engine = Engine::in_memory();
        engine.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(engine.get("k".into()).await, Err(EngineError::Closed));
    }

    #[tokio::test]
    async fn save_without_persistence_is_an_error() {
        let engine = Engine::in_memory();
        assert_eq!(engine.save().await, Err(EngineError::PersistenceDisabled));
    }
}
