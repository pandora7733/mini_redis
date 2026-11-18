//! # Mini Redis
//!
//! 직접 구현한 해시 테이블([`hashtable::HashTable`]) 위에 만든 인메모리 키-값 저장소입니다.
//!
//! 계층 구조 (아래에서 위로):
//!
//! | 모듈 | 역할 |
//! |------|------|
//! | [`hashtable`]   | Separate chaining 해시 테이블 (FNV-1a 해시, 자동 확장/축소) |
//! | [`store`]       | 키-값 + TTL(만료) + 무효화 로직. 순수 동기 코드, 시간은 인자로 주입 |
//! | [`persistence`] | AOF(append-only file) + 스냅샷으로 재시작/크래시 시 데이터 복구 |
//! | [`engine`]      | 단일 스레드 Actor. 모든 명령을 한 줄로 세워 처리 → 락 없이 동시성 안전 |
//! | [`api`]         | 외부에서 쓰는 HTTP REST API (axum) |
//! | [`client`]      | HTTP API를 감싼 Rust 클라이언트 |
//! | [`demo`]        | 느린 상품 API + Mini Redis 캐시(cache-aside) 예제 |

pub mod api;
pub mod client;
pub mod demo;
pub mod engine;
pub mod glob;
pub mod hashtable;
pub mod persistence;
pub mod store;

use std::time::{SystemTime, UNIX_EPOCH};

/// 키 최대 길이 (바이트)
pub const MAX_KEY_BYTES: usize = 1024;
/// 값 최대 길이 (바이트)
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
/// TTL 최대값: 100년 (ms). `now + ttl` 계산 시 overflow 를 막기 위한 상한.
pub const MAX_TTL_MS: u64 = 100 * 365 * 24 * 60 * 60 * 1000;

/// 현재 시각 (Unix epoch 기준 밀리초).
///
/// 만료 시각을 "절대 시각"으로 저장해야 디스크에 기록한 뒤 재시작해도 TTL 이 이어지므로
/// 단조 시계(Instant)가 아닌 벽시계(SystemTime)를 사용합니다.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
