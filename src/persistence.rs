//! 영속성: AOF(Append-Only File) + 스냅샷
//!
//! ```text
//!  data/
//!   ├─ dump.jsonl        스냅샷: 특정 시점의 전체 키 (한 줄에 한 키)
//!   └─ appendonly.aof    AOF: 스냅샷 이후의 변경 명령 로그 (한 줄에 한 명령)
//! ```
//!
//! * **쓰기**: 상태를 바꾸는 명령(SET/DEL/EXPIRE/PERSIST/FLUSH, INCR 결과)을 실행할 때마다
//!   AOF 끝에 JSON 한 줄을 `write` 합니다. 프로세스가 죽어도 OS 페이지 캐시에 남아 있으므로
//!   **프로세스 크래시(kill -9)에서는 유실이 없고**, 정전/OS 크래시 대비는 fsync 정책으로 조절합니다.
//!   - `always`   : 매 명령마다 fsync (가장 안전, 가장 느림)
//!   - `everysec` : 1초마다 fsync (기본값, 최대 1초 유실 — Redis 기본값과 동일)
//!   - `no`       : OS 에 맡김
//! * **만료 시각은 절대 시각(Unix ms)으로 기록**하므로 재시작 후에도 TTL 이 이어집니다.
//! * **복구**: 스냅샷 로드 → AOF 재실행(replay). 이미 만료된 키는 건너뜁니다.
//! * **AOF rewrite**: AOF 가 임계치를 넘으면 현재 상태를 새 스냅샷으로 저장(임시 파일 → fsync →
//!   rename 으로 원자적 교체)한 뒤 AOF 를 비웁니다. rename 직후 AOF 를 비우기 전에 죽더라도,
//!   모든 로그 레코드가 "절대값" 연산(멱등)이라 스냅샷 위에 다시 재실행해도 결과가 같습니다.
//! * **깨진 로그**: 쓰는 도중 죽어 마지막 줄이 잘린 경우(개행 없음)는 잘라내고 기동합니다.
//!   중간 줄이 손상된 경우는 데이터 손실을 숨기지 않도록 기동을 거부합니다.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::now_ms;
use crate::store::{Entry, SetCondition, Store};

pub const AOF_FILE: &str = "appendonly.aof";
pub const SNAPSHOT_FILE: &str = "dump.jsonl";

/// fsync 정책
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::EverySec => "everysec",
            Self::No => "no",
        }
    }
}

impl FromStr for FsyncPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Ok(Self::Always),
            "everysec" => Ok(Self::EverySec),
            "no" => Ok(Self::No),
            other => Err(format!(
                "unknown fsync policy '{other}' (expected always | everysec | no)"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub dir: PathBuf,
    pub fsync: FsyncPolicy,
    /// AOF 가 이 크기(바이트)를 넘으면 스냅샷으로 압축
    pub rewrite_threshold_bytes: u64,
}

impl PersistenceConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            fsync: FsyncPolicy::EverySec,
            rewrite_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

/// AOF 한 줄 = 한 레코드. 모든 레코드는 멱등(같은 순서로 다시 적용해도 결과 동일)입니다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum LogRecord {
    Set {
        key: String,
        value: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expire_at: Option<u64>,
    },
    Del {
        key: String,
    },
    Expire {
        key: String,
        expire_at: u64,
    },
    Persist {
        key: String,
    },
    Flush,
}

#[derive(Serialize)]
struct SnapshotRef<'a> {
    key: &'a str,
    value: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expire_at: Option<u64>,
}

#[derive(Deserialize)]
struct SnapshotRecord {
    key: String,
    value: String,
    #[serde(default)]
    expire_at: Option<u64>,
}

/// 기동 시 복구 결과
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadReport {
    pub snapshot_keys: usize,
    pub aof_records: usize,
    pub truncated_bytes: u64,
    pub live_keys: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceStats {
    pub dir: String,
    pub fsync: String,
    pub aof_bytes: u64,
}

pub struct Persistence {
    config: PersistenceConfig,
    aof: File,
    aof_size: u64,
    unsynced: bool,
    report: LoadReport,
}

impl Persistence {
    /// 데이터 디렉터리를 열고 스냅샷 + AOF 로 저장소를 복원합니다.
    pub fn open(config: PersistenceConfig) -> io::Result<(Self, Store)> {
        fs::create_dir_all(&config.dir)?;
        let now = now_ms();
        let mut store = Store::new();

        let snapshot_keys = load_snapshot(&config.dir.join(SNAPSHOT_FILE), &mut store, now)?;

        let aof_path = config.dir.join(AOF_FILE);
        let replay = replay_aof(&aof_path, &mut store, now)?;
        let truncated_bytes = replay.total_len - replay.valid_len;
        if truncated_bytes > 0 {
            tracing::warn!(
                bytes = truncated_bytes,
                "AOF ends with an incomplete record (torn write); truncating it"
            );
            let file = OpenOptions::new().write(true).open(&aof_path)?;
            file.set_len(replay.valid_len)?;
            file.sync_all()?;
        }

        store.reset_stats();
        let aof = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&aof_path)?;
        let report = LoadReport {
            snapshot_keys,
            aof_records: replay.records,
            truncated_bytes,
            live_keys: store.len(),
        };
        Ok((
            Self {
                config,
                aof,
                aof_size: replay.valid_len,
                unsynced: false,
                report,
            },
            store,
        ))
    }

    pub fn load_report(&self) -> &LoadReport {
        &self.report
    }

    pub fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    pub fn aof_size(&self) -> u64 {
        self.aof_size
    }

    /// AOF 끝에 레코드 한 줄을 추가
    pub fn append(&mut self, record: &LogRecord) -> io::Result<()> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        self.aof.write_all(&line)?;
        self.aof_size += line.len() as u64;
        if self.config.fsync == FsyncPolicy::Always {
            self.aof.sync_data()?;
        } else {
            self.unsynced = true;
        }
        Ok(())
    }

    /// everysec 정책: 엔진이 1초마다 호출
    pub fn sync_if_needed(&mut self) -> io::Result<()> {
        if self.unsynced && self.config.fsync == FsyncPolicy::EverySec {
            self.aof.sync_data()?;
            self.unsynced = false;
        }
        Ok(())
    }

    pub fn needs_rewrite(&self) -> bool {
        self.aof_size >= self.config.rewrite_threshold_bytes
    }

    /// 현재 상태를 스냅샷으로 원자적으로 저장하고 AOF 를 비웁니다. 저장한 키 수를 반환.
    pub fn rewrite(&mut self, store: &Store, now: u64) -> io::Result<usize> {
        let tmp_path = self.config.dir.join(format!("{SNAPSHOT_FILE}.tmp"));
        let mut count = 0;
        {
            let mut writer = BufWriter::new(File::create(&tmp_path)?);
            for (key, entry) in store.iter() {
                if entry.is_expired(now) {
                    continue;
                }
                let record = SnapshotRef {
                    key: key.as_str(),
                    value: entry.value.as_str(),
                    expire_at: entry.expire_at,
                };
                serde_json::to_writer(&mut writer, &record)?;
                writer.write_all(b"\n")?;
                count += 1;
            }
            let file = writer.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
        }
        // rename 은 같은 파일시스템 안에서 원자적: 읽는 쪽은 옛 스냅샷 or 새 스냅샷만 봄
        fs::rename(&tmp_path, self.config.dir.join(SNAPSHOT_FILE))?;
        sync_dir(&self.config.dir);

        self.aof.set_len(0)?;
        self.aof.sync_all()?;
        self.aof_size = 0;
        self.unsynced = false;
        Ok(count)
    }

    pub fn stats(&self) -> PersistenceStats {
        PersistenceStats {
            dir: self.config.dir.display().to_string(),
            fsync: self.config.fsync.as_str().to_string(),
            aof_bytes: self.aof_size,
        }
    }
}

/// 디렉터리 엔트리(rename 결과)까지 디스크에 반영 (unix 에서만 의미 있음, 실패는 무시)
fn sync_dir(dir: &Path) {
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
}

fn load_snapshot(path: &Path, store: &mut Store, now: u64) -> io::Result<usize> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut loaded = 0;
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: SnapshotRecord = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupted snapshot {} at line {}: {e}", path.display(), idx + 1),
            )
        })?;
        if record.expire_at.is_some_and(|at| at <= now) {
            continue;
        }
        store.restore(record.key, Entry::new(record.value, record.expire_at));
        loaded += 1;
    }
    Ok(loaded)
}

struct AofReplay {
    records: usize,
    valid_len: u64,
    total_len: u64,
}

fn replay_aof(path: &Path, store: &mut Store, now: u64) -> io::Result<AofReplay> {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(AofReplay {
                records: 0,
                valid_len: 0,
                total_len: 0,
            })
        }
        Err(e) => return Err(e),
    };
    let mut offset = 0usize;
    let mut records = 0usize;
    let mut line_no = 0usize;
    while offset < data.len() {
        // 개행이 없는 마지막 조각 = 쓰다가 중단된 레코드 → 여기서 멈추고 잘라냄
        let Some(rel_end) = data[offset..].iter().position(|&b| b == b'\n') else {
            break;
        };
        line_no += 1;
        let line = &data[offset..offset + rel_end];
        if !line.iter().all(u8::is_ascii_whitespace) {
            let record: LogRecord = serde_json::from_slice(line).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "corrupted AOF record at {}:{line_no}: {e} — inspect or repair the file before starting",
                        path.display()
                    ),
                )
            })?;
            apply_record(store, record, now);
            records += 1;
        }
        offset += rel_end + 1;
    }
    Ok(AofReplay {
        records,
        valid_len: offset as u64,
        total_len: data.len() as u64,
    })
}

/// AOF 레코드 하나를 저장소에 적용 (복구 시 사용)
pub fn apply_record(store: &mut Store, record: LogRecord, now: u64) {
    match record {
        LogRecord::Set {
            key,
            value,
            expire_at,
        } => {
            if expire_at.is_some_and(|at| at <= now) {
                // 이미 만료된 값으로 덮어쓴 것 = 키가 없는 것과 같음
                store.del(&key, now);
            } else {
                store.set(key, value, expire_at, SetCondition::Always, now);
            }
        }
        LogRecord::Del { key } => {
            store.del(&key, now);
        }
        LogRecord::Expire { key, expire_at } => {
            if expire_at <= now {
                store.del(&key, now);
            } else {
                store.expire(&key, expire_at, now);
            }
        }
        LogRecord::Persist { key } => {
            store.persist(&key, now);
        }
        LogRecord::Flush => {
            store.flush();
        }
    }
}
