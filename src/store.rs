//! 키-값 저장소 (TTL/만료, 조건부 저장, 무효화)
//!
//! 이 모듈은 **순수 동기 코드**이며 현재 시각(`now`, ms)을 인자로 받습니다.
//! 덕분에 테스트에서 `sleep` 없이 시간을 마음대로 진행시켜 만료 로직을 검증할 수 있습니다.
//!
//! ## 만료 처리 전략 (Redis 와 동일한 2단계)
//!
//! 1. **Lazy expiration (지연 삭제)**: 모든 읽기/쓰기 명령은 먼저 대상 키가 만료됐는지 확인하고,
//!    만료됐으면 그 자리에서 삭제한 뒤 "없는 키"로 처리합니다.
//!    → 만료된 값이 응답으로 나가는 일은 절대 없습니다.
//! 2. **Active expiration (주기적 삭제)**: 아무도 다시 읽지 않는 만료 키가 메모리를 계속
//!    차지하지 않도록, 엔진이 100ms 마다 [`Store::active_expire`] 를 호출합니다.
//!    만료 시각 기준 min-heap 에서 시간이 지난 항목만 꺼내므로 전체 스캔이 필요 없습니다.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use serde::{Deserialize, Serialize};

use crate::glob::glob_match;
use crate::hashtable::HashTable;

/// 힙 정리를 고려하기 시작하는 최소 크기
const QUEUE_COMPACT_MIN: usize = 1024;

/// 저장되는 값과 만료 시각
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub value: String,
    /// 만료 시각 (Unix ms). `None` 이면 영구 보관.
    pub expire_at: Option<u64>,
}

impl Entry {
    pub fn new(value: impl Into<String>, expire_at: Option<u64>) -> Self {
        Self {
            value: value.into(),
            expire_at,
        }
    }

    /// `now` 시점에 만료되었는가 (만료 시각과 같으면 만료로 봄)
    pub fn is_expired(&self, now: u64) -> bool {
        self.expire_at.is_some_and(|at| at <= now)
    }
}

/// SET 조건 (Redis 의 NX / XX)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetCondition {
    /// 항상 저장
    #[default]
    Always,
    /// 키가 없을 때만 저장 (NX) — 분산 락, 중복 방지 등에 사용
    IfNotExists,
    /// 키가 있을 때만 저장 (XX) — 존재하는 값 갱신만 허용
    IfExists,
}

/// TTL 조회 결과
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// 키가 없음 (Redis: -2)
    Missing,
    /// 만료 없음 (Redis: -1)
    Persistent,
    /// 남은 시간 (ms)
    ExpiresIn(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("value is not an integer or out of range")]
    NotAnInteger,
    #[error("increment or decrement would overflow")]
    Overflow,
}

/// 저장소 통계
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreStats {
    pub keys: usize,
    pub hits: u64,
    pub misses: u64,
    pub expired_lazy: u64,
    pub expired_active: u64,
    pub capacity: usize,
    pub load_factor: f64,
    pub longest_chain: usize,
    pub pending_expiry_entries: usize,
}

/// 키-값 저장소
pub struct Store {
    table: HashTable<String, Entry>,
    /// (만료 시각, 키) min-heap. 키를 덮어쓰거나 삭제해도 힙에서 즉시 빼지 않고(stale),
    /// 꺼낼 때 테이블의 실제 만료 시각과 비교해 다르면 무시합니다.
    expiry_queue: BinaryHeap<Reverse<(u64, String)>>,
    hits: u64,
    misses: u64,
    expired_lazy: u64,
    expired_active: u64,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    pub fn new() -> Self {
        Self {
            table: HashTable::new(),
            expiry_queue: BinaryHeap::new(),
            hits: 0,
            misses: 0,
            expired_lazy: 0,
            expired_active: 0,
        }
    }

    /// 테이블에 들어 있는 키 수 (아직 정리되지 않은 만료 키 포함)
    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Lazy expiration: 키가 만료됐으면 삭제하고 true
    fn purge_if_expired(&mut self, key: &str, now: u64) -> bool {
        let expired = matches!(self.table.get(key), Some(e) if e.is_expired(now));
        if expired {
            self.table.remove(key);
            self.expired_lazy += 1;
        }
        expired
    }

    fn schedule_expiry(&mut self, key: String, at: u64) {
        self.expiry_queue.push(Reverse((at, key)));
        self.maybe_compact_queue();
    }

    /// stale 항목이 너무 많이 쌓이면 (실제 키 수의 2배 초과) 테이블 기준으로 힙을 재구성.
    /// 같은 키에 TTL 을 계속 갱신하는 패턴에서도 메모리가 무한히 늘지 않게 합니다.
    fn maybe_compact_queue(&mut self) {
        let queued = self.expiry_queue.len();
        if queued > QUEUE_COMPACT_MIN && queued > self.table.len() * 2 {
            self.expiry_queue = self
                .table
                .iter()
                .filter_map(|(k, e)| e.expire_at.map(|at| Reverse((at, k.clone()))))
                .collect();
        }
    }

    /// GET: 만료된 키는 삭제 후 None
    pub fn get(&mut self, key: &str, now: u64) -> Option<&Entry> {
        self.purge_if_expired(key, now);
        let found = self.table.get(key);
        if found.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        found
    }

    /// 만료 검사/통계 없이 엔트리를 들여다봄 (내부용)
    pub fn peek(&self, key: &str) -> Option<&Entry> {
        self.table.get(key)
    }

    /// SET. 조건을 만족해 저장했으면 true.
    /// Redis 와 같이 TTL 없이 SET 하면 기존 TTL 은 사라집니다.
    pub fn set(
        &mut self,
        key: String,
        value: String,
        expire_at: Option<u64>,
        condition: SetCondition,
        now: u64,
    ) -> bool {
        self.purge_if_expired(&key, now);
        let exists = self.table.contains_key(key.as_str());
        let allowed = match condition {
            SetCondition::Always => true,
            SetCondition::IfNotExists => !exists,
            SetCondition::IfExists => exists,
        };
        if !allowed {
            return false;
        }
        self.restore(key, Entry { value, expire_at });
        true
    }

    /// 조건/통계 없이 그대로 넣음 (스냅샷 복원에도 사용)
    pub fn restore(&mut self, key: String, entry: Entry) {
        // 테이블을 먼저 갱신한 뒤 힙에 넣어야, 힙 재구성(compaction)이 새 만료 시각을 반영합니다.
        match entry.expire_at {
            Some(at) => {
                self.table.insert(key.clone(), entry);
                self.schedule_expiry(key, at);
            }
            None => {
                self.table.insert(key, entry);
            }
        }
    }

    /// DEL: 살아 있는 키를 지웠으면 true (이미 만료된 키는 false)
    pub fn del(&mut self, key: &str, now: u64) -> bool {
        if self.purge_if_expired(key, now) {
            return false;
        }
        self.table.remove(key).is_some()
    }

    pub fn exists(&mut self, key: &str, now: u64) -> bool {
        self.purge_if_expired(key, now);
        self.table.contains_key(key)
    }

    /// EXPIRE: 키가 있으면 만료 시각을 설정하고 true
    pub fn expire(&mut self, key: &str, at: u64, now: u64) -> bool {
        self.purge_if_expired(key, now);
        match self.table.get_mut(key) {
            Some(entry) => {
                entry.expire_at = Some(at);
                self.schedule_expiry(key.to_owned(), at);
                true
            }
            None => false,
        }
    }

    /// PERSIST: 키가 없으면 None, 있으면 TTL 을 제거했는지 여부
    pub fn persist(&mut self, key: &str, now: u64) -> Option<bool> {
        self.purge_if_expired(key, now);
        let entry = self.table.get_mut(key)?;
        Some(entry.expire_at.take().is_some())
    }

    pub fn ttl(&mut self, key: &str, now: u64) -> Ttl {
        self.purge_if_expired(key, now);
        match self.table.get(key) {
            None => Ttl::Missing,
            Some(Entry {
                expire_at: None, ..
            }) => Ttl::Persistent,
            Some(Entry {
                expire_at: Some(at),
                ..
            }) => Ttl::ExpiresIn(at.saturating_sub(now)),
        }
    }

    /// INCRBY: 원자적 증가. 키가 없으면 0 에서 시작. 기존 TTL 은 유지.
    pub fn incr(&mut self, key: &str, delta: i64, now: u64) -> Result<i64, StoreError> {
        self.purge_if_expired(key, now);
        match self.table.get_mut(key) {
            Some(entry) => {
                let current: i64 = entry
                    .value
                    .parse()
                    .map_err(|_| StoreError::NotAnInteger)?;
                let next = current.checked_add(delta).ok_or(StoreError::Overflow)?;
                entry.value = next.to_string();
                Ok(next)
            }
            None => {
                self.table
                    .insert(key.to_owned(), Entry::new(delta.to_string(), None));
                Ok(delta)
            }
        }
    }

    /// KEYS pattern: 살아 있는 키 중 패턴에 맞는 것 (정렬됨). 전체 순회 O(n).
    pub fn keys(&self, pattern: &str, now: u64) -> Vec<String> {
        let mut keys: Vec<String> = self
            .table
            .iter()
            .filter_map(|(k, e)| (!e.is_expired(now) && glob_match(pattern, k)).then(|| k.clone()))
            .collect();
        keys.sort_unstable();
        keys
    }

    /// 패턴 무효화: 패턴에 맞는 키를 모두 삭제하고, 삭제된(살아 있던) 키 목록을 반환.
    /// 예) 상품 정보가 바뀌면 `product:42:*` 캐시를 한 번에 무효화.
    pub fn delete_matching(&mut self, pattern: &str, now: u64) -> Vec<String> {
        let mut deleted = Vec::new();
        self.table.retain(|key, entry| {
            if !glob_match(pattern, key) {
                return true;
            }
            if !entry.is_expired(now) {
                deleted.push(key.clone());
            }
            false
        });
        deleted
    }

    /// FLUSHALL: 전체 삭제, 삭제한 키 수 반환
    pub fn flush(&mut self) -> usize {
        let n = self.table.len();
        self.table.clear();
        self.expiry_queue.clear();
        n
    }

    /// Active expiration: 만료 시각이 지난 키를 최대 `budget` 개의 힙 항목까지 검사해 삭제.
    /// 한 번에 너무 오래 엔진을 붙잡지 않도록 상한을 둡니다 (남은 건 다음 tick 에 처리).
    pub fn active_expire(&mut self, now: u64, budget: usize) -> usize {
        let mut removed = 0;
        for _ in 0..budget {
            match self.expiry_queue.peek() {
                Some(Reverse((at, _))) if *at <= now => {}
                _ => break,
            }
            let Some(Reverse((at, key))) = self.expiry_queue.pop() else {
                break;
            };
            // 힙 항목이 현재 엔트리의 만료 시각과 같을 때만 유효 (아니면 stale 항목)
            let current = matches!(self.table.get(key.as_str()), Some(e) if e.expire_at == Some(at));
            if current {
                self.table.remove(key.as_str());
                removed += 1;
            }
        }
        self.expired_active += removed as u64;
        removed
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Entry)> + '_ {
        self.table.iter()
    }

    pub fn reset_stats(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.expired_lazy = 0;
        self.expired_active = 0;
    }

    pub fn stats(&self) -> StoreStats {
        StoreStats {
            keys: self.table.len(),
            hits: self.hits,
            misses: self.misses,
            expired_lazy: self.expired_lazy,
            expired_active: self.expired_active,
            capacity: self.table.capacity(),
            load_factor: self.table.load_factor(),
            longest_chain: self.table.longest_chain(),
            pending_expiry_entries: self.expiry_queue.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SetCondition::*;

    fn s(v: &str) -> String {
        v.to_string()
    }

    fn value(store: &mut Store, key: &str, now: u64) -> Option<String> {
        store.get(key, now).map(|e| e.value.clone())
    }

    #[test]
    fn set_get_delete() {
        let mut st = Store::new();
        assert!(st.set(s("k"), s("v"), None, Always, 0));
        assert_eq!(value(&mut st, "k", 0), Some(s("v")));
        assert!(st.del("k", 0));
        assert!(!st.del("k", 0));
        assert_eq!(value(&mut st, "k", 0), None);
        let stats = st.stats();
        assert_eq!((stats.hits, stats.misses), (1, 1));
    }

    #[test]
    fn lazy_expiration_on_read() {
        let mut st = Store::new();
        st.set(s("k"), s("v"), Some(100), Always, 0);
        assert_eq!(value(&mut st, "k", 99), Some(s("v")));
        assert_eq!(value(&mut st, "k", 100), None, "만료 시각 도달 시 만료");
        assert_eq!(st.len(), 0, "조회 시점에 물리적으로 삭제");
        assert_eq!(st.stats().expired_lazy, 1);
    }

    #[test]
    fn active_expiration_without_access() {
        let mut st = Store::new();
        st.set(s("a"), s("1"), Some(100), Always, 0);
        st.set(s("b"), s("2"), Some(200), Always, 0);
        st.set(s("c"), s("3"), None, Always, 0);
        assert_eq!(st.active_expire(150, 100), 1);
        assert_eq!(st.len(), 2);
        assert_eq!(st.active_expire(250, 100), 1);
        assert_eq!(st.len(), 1);
        assert_eq!(st.stats().expired_active, 2);
    }

    #[test]
    fn active_expiration_respects_budget() {
        let mut st = Store::new();
        for i in 0..10 {
            st.set(format!("k{i}"), s("v"), Some(10), Always, 0);
        }
        assert_eq!(st.active_expire(20, 3), 3);
        assert_eq!(st.active_expire(20, 100), 7);
    }

    #[test]
    fn stale_queue_entries_are_ignored() {
        let mut st = Store::new();
        st.set(s("k"), s("v1"), Some(100), Always, 0);
        st.set(s("k"), s("v2"), Some(500), Always, 0); // TTL 연장
        assert_eq!(st.active_expire(150, 100), 0, "옛 만료 시각(100) 항목은 무시");
        assert_eq!(value(&mut st, "k", 150), Some(s("v2")));
        assert_eq!(st.active_expire(600, 100), 1);
    }

    #[test]
    fn set_without_ttl_clears_previous_ttl() {
        let mut st = Store::new();
        st.set(s("k"), s("v"), Some(100), Always, 0);
        st.set(s("k"), s("v"), None, Always, 0);
        assert_eq!(st.active_expire(200, 100), 0);
        assert_eq!(st.ttl("k", 200), Ttl::Persistent);
    }

    #[test]
    fn nx_and_xx_conditions() {
        let mut st = Store::new();
        assert!(!st.set(s("k"), s("v"), None, IfExists, 0), "XX: 없는 키는 실패");
        assert!(st.set(s("k"), s("v"), None, IfNotExists, 0));
        assert!(!st.set(s("k"), s("v2"), None, IfNotExists, 0), "NX: 있는 키는 실패");
        assert!(st.set(s("k"), s("v3"), None, IfExists, 0));
        assert_eq!(value(&mut st, "k", 0), Some(s("v3")));
    }

    #[test]
    fn nx_succeeds_when_old_value_expired() {
        let mut st = Store::new();
        st.set(s("lock"), s("a"), Some(100), Always, 0);
        assert!(!st.set(s("lock"), s("b"), None, IfNotExists, 50));
        assert!(st.set(s("lock"), s("b"), None, IfNotExists, 100), "만료된 키는 없는 것으로 취급");
    }

    #[test]
    fn ttl_expire_persist() {
        let mut st = Store::new();
        assert_eq!(st.ttl("k", 0), Ttl::Missing);
        assert!(!st.expire("k", 100, 0), "없는 키 EXPIRE 는 실패");
        st.set(s("k"), s("v"), None, Always, 0);
        assert_eq!(st.ttl("k", 0), Ttl::Persistent);
        assert!(st.expire("k", 1_000, 0));
        assert_eq!(st.ttl("k", 400), Ttl::ExpiresIn(600));
        assert_eq!(st.persist("k", 500), Some(true));
        assert_eq!(st.persist("k", 500), Some(false), "이미 영구 키");
        assert_eq!(st.ttl("k", 5_000), Ttl::Persistent);
        assert_eq!(st.persist("missing", 0), None);
        assert_eq!(st.active_expire(10_000, 100), 0, "PERSIST 후 힙 항목은 stale");
    }

    #[test]
    fn incr_behaviour() {
        let mut st = Store::new();
        assert_eq!(st.incr("n", 1, 0), Ok(1));
        assert_eq!(st.incr("n", 5, 0), Ok(6));
        assert_eq!(st.incr("n", -10, 0), Ok(-4));
        st.set(s("text"), s("abc"), None, Always, 0);
        assert_eq!(st.incr("text", 1, 0), Err(StoreError::NotAnInteger));
        st.set(s("max"), i64::MAX.to_string(), None, Always, 0);
        assert_eq!(st.incr("max", 1, 0), Err(StoreError::Overflow));
        assert_eq!(value(&mut st, "max", 0), Some(i64::MAX.to_string()), "실패 시 값 불변");
        st.set(s("ttl"), s("1"), Some(1_000), Always, 0);
        st.incr("ttl", 1, 0).unwrap();
        assert_eq!(st.ttl("ttl", 0), Ttl::ExpiresIn(1_000), "INCR 은 TTL 유지");
    }

    #[test]
    fn incr_on_expired_key_restarts_from_zero() {
        let mut st = Store::new();
        st.set(s("n"), s("100"), Some(10), Always, 0);
        assert_eq!(st.incr("n", 1, 10), Ok(1));
    }

    #[test]
    fn keys_and_pattern_invalidation() {
        let mut st = Store::new();
        for k in ["user:1", "user:2", "user:3:profile", "post:1"] {
            st.set(s(k), s("v"), None, Always, 0);
        }
        st.set(s("user:old"), s("v"), Some(10), Always, 0);
        assert_eq!(st.keys("user:*", 20), vec!["user:1", "user:2", "user:3:profile"]);
        assert_eq!(st.keys("user:?", 20), vec!["user:1", "user:2"]);
        let deleted = st.delete_matching("user:*", 20);
        assert_eq!(deleted.len(), 3, "만료된 user:old 는 개수에서 제외");
        assert_eq!(st.keys("*", 20), vec!["post:1"]);
        assert_eq!(st.len(), 1);
    }

    #[test]
    fn delete_of_expired_key_returns_false() {
        let mut st = Store::new();
        st.set(s("k"), s("v"), Some(10), Always, 0);
        assert!(!st.del("k", 10));
        assert!(!st.exists("k", 10));
    }

    #[test]
    fn expiry_queue_does_not_grow_unbounded() {
        let mut st = Store::new();
        for i in 0..10_000u64 {
            st.set(s("hot"), s("v"), Some(1_000_000 + i), Always, 0);
        }
        assert!(st.stats().pending_expiry_entries <= QUEUE_COMPACT_MIN + 1);
        assert_eq!(st.ttl("hot", 0), Ttl::ExpiresIn(1_000_000 + 9_999));
        // 재구성 후에도 최신 만료 시각으로 active expiration 이 동작해야 함
        assert_eq!(st.active_expire(1_000_000 + 9_998, 100_000), 0);
        assert_eq!(st.active_expire(1_000_000 + 9_999, 100_000), 1);
    }

    #[test]
    fn flush_clears_everything() {
        let mut st = Store::new();
        st.set(s("a"), s("1"), Some(100), Always, 0);
        st.set(s("b"), s("2"), None, Always, 0);
        assert_eq!(st.flush(), 2);
        assert!(st.is_empty());
        assert_eq!(st.stats().pending_expiry_entries, 0);
    }

    #[test]
    fn empty_and_unicode_values() {
        let mut st = Store::new();
        st.set(s("empty"), s(""), None, Always, 0);
        st.set(s("한글키"), s("값 🚀"), None, Always, 0);
        assert_eq!(value(&mut st, "empty", 0), Some(s("")));
        assert_eq!(value(&mut st, "한글키", 0), Some(s("값 🚀")));
    }
}
