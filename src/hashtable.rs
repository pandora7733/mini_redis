//! 직접 구현한 해시 테이블 (Separate Chaining 방식)
//!
//! ```text
//!  buckets (길이는 항상 2의 거듭제곱)
//!  ┌───┐
//!  │ 0 │ → [ ]
//!  │ 1 │ → [ (hash, "user:1", v) , (hash, "cart:9", v) ]   ← 충돌 시 같은 버킷 Vec 에 이어 붙임
//!  │ 2 │ → [ (hash, "a", v) ]
//!  │...│
//!  └───┘
//!  index = hash & (capacity - 1)      (capacity 가 2^n 이라 % 대신 비트 AND)
//! ```
//!
//! * 해시 함수: FNV-1a (64bit) + murmur3 finalizer(fmix64) 로 하위 비트 분포 보강
//! * 충돌 해결: Separate chaining (버킷마다 `Vec<Entry>`; 연결 리스트보다 캐시 친화적)
//! * 확장: `len / capacity > 0.75` 가 되면 capacity 를 2배로 늘리고 전체 rehash
//! * 축소: `len / capacity < 0.125` 가 되면 절반으로 줄임 (대량 삭제 후 메모리 반환)
//! * 각 Entry 는 해시값을 캐시해 두어 rehash 시 해시를 다시 계산하지 않고,
//!   비교 시에도 해시가 다르면 키 비교(문자열 비교)를 건너뜁니다.

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 해시 함수.
///
/// 바이트마다 `hash ^= byte; hash *= FNV_PRIME` 을 반복하는 단순하고 빠른 해시입니다.
#[derive(Debug, Clone)]
pub struct Fnv1aHasher {
    state: u64,
}

impl Default for Fnv1aHasher {
    fn default() -> Self {
        Self { state: FNV_OFFSET_BASIS }
    }
}

impl Hasher for Fnv1aHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= u64::from(byte);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }

    fn finish(&self) -> u64 {
        // 버킷 인덱스는 하위 비트만 사용하므로, 상위 비트를 하위로 섞어주는 finalizer 적용
        let mut h = self.state;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        h ^= h >> 33;
        h
    }
}

/// [`Fnv1aHasher`] 를 만들어 주는 BuildHasher (HashTable 의 기본 해셔)
#[derive(Debug, Clone, Copy, Default)]
pub struct FnvBuildHasher;

impl BuildHasher for FnvBuildHasher {
    type Hasher = Fnv1aHasher;

    fn build_hasher(&self) -> Fnv1aHasher {
        Fnv1aHasher::default()
    }
}

/// 최소(초기) 버킷 수
pub const INITIAL_CAPACITY: usize = 16;
/// 최대 load factor = 3/4
const GROW_NUMERATOR: usize = 3;
const GROW_DENOMINATOR: usize = 4;
/// load factor 가 1/8 미만이면 축소
const SHRINK_DIVISOR: usize = 8;

struct Entry<K, V> {
    hash: u64,
    key: K,
    value: V,
}

/// Separate chaining 해시 테이블.
///
/// `S` 로 해시 함수를 교체할 수 있습니다 (테스트에서 강제 충돌 해셔를 넣어 검증).
pub struct HashTable<K, V, S = FnvBuildHasher> {
    buckets: Vec<Vec<Entry<K, V>>>,
    len: usize,
    hash_builder: S,
}

fn empty_buckets<K, V>(capacity: usize) -> Vec<Vec<Entry<K, V>>> {
    (0..capacity).map(|_| Vec::new()).collect()
}

/// `n` 개를 넣어도 load factor 0.75 를 넘지 않는 2의 거듭제곱 버킷 수
fn capacity_for(n: usize) -> usize {
    let needed = n.saturating_mul(GROW_DENOMINATOR) / GROW_NUMERATOR + 1;
    needed.next_power_of_two().max(INITIAL_CAPACITY)
}

impl<K, V> HashTable<K, V, FnvBuildHasher> {
    pub fn new() -> Self {
        Self::with_hasher(FnvBuildHasher)
    }

    pub fn with_capacity(n: usize) -> Self {
        Self::with_capacity_and_hasher(n, FnvBuildHasher)
    }
}

impl<K, V> Default for HashTable<K, V, FnvBuildHasher> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> HashTable<K, V, S> {
    pub fn with_hasher(hash_builder: S) -> Self {
        Self::with_capacity_and_hasher(0, hash_builder)
    }

    pub fn with_capacity_and_hasher(n: usize, hash_builder: S) -> Self {
        Self {
            buckets: empty_buckets(capacity_for(n)),
            len: 0,
            hash_builder,
        }
    }

    /// 저장된 키 개수
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 버킷 수
    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    /// load factor = 키 개수 / 버킷 수
    pub fn load_factor(&self) -> f64 {
        self.len as f64 / self.buckets.len() as f64
    }

    /// 가장 긴 체인 길이 (해시 분포 품질 지표)
    pub fn longest_chain(&self) -> usize {
        self.buckets.iter().map(Vec::len).max().unwrap_or(0)
    }

    /// 모든 (키, 값) 순회. 순서는 보장하지 않습니다.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.iter().map(|e| (&e.key, &e.value)))
    }

    /// 모든 데이터 삭제 후 초기 크기로 되돌림
    pub fn clear(&mut self) {
        self.buckets = empty_buckets(INITIAL_CAPACITY);
        self.len = 0;
    }

    fn index_for(&self, hash: u64) -> usize {
        // capacity 가 2^n 이므로 `hash % capacity` == `hash & (capacity - 1)`
        (hash as usize) & (self.buckets.len() - 1)
    }

    /// 새 버킷 배열을 만들고 모든 엔트리를 다시 배치 (rehash).
    /// 엔트리에 해시를 캐시해 두었기 때문에 해시 재계산이 필요 없습니다.
    fn resize(&mut self, new_capacity: usize) {
        debug_assert!(new_capacity.is_power_of_two());
        let old = mem::replace(&mut self.buckets, empty_buckets(new_capacity));
        for bucket in old {
            for entry in bucket {
                let idx = self.index_for(entry.hash);
                self.buckets[idx].push(entry);
            }
        }
    }

    fn shrink_if_sparse(&mut self) {
        let mut new_capacity = self.buckets.len();
        while new_capacity > INITIAL_CAPACITY && self.len * SHRINK_DIVISOR < new_capacity {
            new_capacity /= 2;
        }
        if new_capacity != self.buckets.len() {
            self.resize(new_capacity);
        }
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> HashTable<K, V, S> {
    fn hash_key<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.hash_builder.hash_one(key)
    }

    /// 키 위치 (버킷 인덱스, 버킷 내 위치) 찾기
    fn find<Q>(&self, key: &Q) -> Option<(usize, usize)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_key(key);
        let idx = self.index_for(hash);
        self.buckets[idx]
            .iter()
            .position(|e| e.hash == hash && e.key.borrow() == key)
            .map(|pos| (idx, pos))
    }

    /// 삽입. 이미 있던 키면 값을 교체하고 이전 값을 돌려줍니다.
    ///
    /// 1. 해시 계산 → 버킷 인덱스
    /// 2. 버킷에서 같은 키를 찾으면 값 교체 (len 변화 없음)
    /// 3. 없으면 버킷 끝에 추가, len += 1
    /// 4. load factor 가 0.75 를 넘으면 2배로 확장
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let hash = self.hash_key(&key);
        let idx = self.index_for(hash);
        if let Some(entry) = self.buckets[idx]
            .iter_mut()
            .find(|e| e.hash == hash && e.key == key)
        {
            return Some(mem::replace(&mut entry.value, value));
        }
        self.buckets[idx].push(Entry { hash, key, value });
        self.len += 1;
        if self.len * GROW_DENOMINATOR > self.buckets.len() * GROW_NUMERATOR {
            self.resize(self.buckets.len() * 2);
        }
        None
    }

    /// 조회: 해시 → 버킷 → 체인 선형 탐색. 평균 O(1)
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key).map(|(b, p)| &self.buckets[b][p].value)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (b, p) = self.find(key)?;
        Some(&mut self.buckets[b][p].value)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key).is_some()
    }

    /// 삭제. 버킷 내 순서는 의미가 없으므로 `swap_remove` 로 O(1) 제거합니다.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (b, p) = self.find(key)?;
        let entry = self.buckets[b].swap_remove(p);
        self.len -= 1;
        self.shrink_if_sparse();
        Some(entry.value)
    }

    /// `keep` 이 false 를 돌려준 엔트리를 모두 삭제하고 삭제 개수를 반환합니다.
    pub fn retain<F>(&mut self, mut keep: F) -> usize
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        let before = self.len;
        for bucket in &mut self.buckets {
            bucket.retain_mut(|e| keep(&e.key, &mut e.value));
        }
        self.len = self.buckets.iter().map(Vec::len).sum();
        self.shrink_if_sparse();
        before - self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// 모든 키를 같은 버킷으로 보내는 최악의 해셔 (충돌 처리 검증용)
    #[derive(Default)]
    struct ConstHasher;
    impl Hasher for ConstHasher {
        fn write(&mut self, _bytes: &[u8]) {}
        fn finish(&self) -> u64 {
            42
        }
    }
    #[derive(Default, Clone)]
    struct ConstBuild;
    impl BuildHasher for ConstBuild {
        type Hasher = ConstHasher;
        fn build_hasher(&self) -> ConstHasher {
            ConstHasher
        }
    }

    #[test]
    fn insert_get_overwrite() {
        let mut t = HashTable::new();
        assert_eq!(t.insert("a".to_string(), 1), None);
        assert_eq!(t.insert("b".to_string(), 2), None);
        assert_eq!(t.get("a"), Some(&1));
        assert_eq!(t.insert("a".to_string(), 10), Some(1));
        assert_eq!(t.get("a"), Some(&10));
        assert_eq!(t.len(), 2);
        assert_eq!(t.get("missing"), None);
    }

    #[test]
    fn get_mut_and_contains() {
        let mut t = HashTable::new();
        t.insert("k".to_string(), 1);
        *t.get_mut("k").unwrap() += 41;
        assert_eq!(t.get("k"), Some(&42));
        assert!(t.contains_key("k"));
        assert!(!t.contains_key("x"));
    }

    #[test]
    fn remove_works() {
        let mut t = HashTable::new();
        t.insert("a".to_string(), 1);
        t.insert("b".to_string(), 2);
        assert_eq!(t.remove("a"), Some(1));
        assert_eq!(t.remove("a"), None);
        assert_eq!(t.get("a"), None);
        assert_eq!(t.get("b"), Some(&2));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn grows_and_keeps_load_factor() {
        let mut t = HashTable::new();
        for i in 0..10_000 {
            t.insert(format!("key:{i}"), i);
            assert!(t.load_factor() <= 0.75, "load factor exceeded at {i}");
        }
        assert!(t.capacity().is_power_of_two());
        assert!(t.capacity() >= 10_000 * 4 / 3);
        for i in 0..10_000 {
            assert_eq!(t.get(format!("key:{i}").as_str()), Some(&i));
        }
    }

    #[test]
    fn shrinks_after_mass_delete() {
        let mut t = HashTable::new();
        for i in 0..10_000 {
            t.insert(i, i);
        }
        let big = t.capacity();
        for i in 0..9_990 {
            t.remove(&i);
        }
        assert!(t.capacity() < big);
        assert!(t.capacity() >= INITIAL_CAPACITY);
        for i in 9_990..10_000 {
            assert_eq!(t.get(&i), Some(&i));
        }
    }

    #[test]
    fn handles_full_collisions() {
        let mut t = HashTable::with_hasher(ConstBuild);
        for i in 0..200 {
            t.insert(format!("k{i}"), i);
        }
        assert_eq!(t.longest_chain(), 200, "모든 키가 한 버킷에 모여야 함");
        for i in 0..200 {
            assert_eq!(t.get(format!("k{i}").as_str()), Some(&i));
        }
        assert_eq!(t.remove("k7"), Some(7));
        assert_eq!(t.get("k7"), None);
        assert_eq!(t.len(), 199);
    }

    #[test]
    fn hash_distribution_is_reasonable() {
        let mut t = HashTable::new();
        for i in 0..50_000 {
            t.insert(format!("user:{i}"), ());
        }
        // 균등 분포라면 load factor 0.75 근처에서 가장 긴 체인은 한 자릿수
        assert!(t.longest_chain() < 12, "longest chain = {}", t.longest_chain());
    }

    #[test]
    fn retain_and_clear() {
        let mut t = HashTable::new();
        for i in 0..100 {
            t.insert(i, i);
        }
        let removed = t.retain(|k, _| k % 2 == 0);
        assert_eq!(removed, 50);
        assert_eq!(t.len(), 50);
        assert!(t.iter().all(|(k, _)| k % 2 == 0));
        t.clear();
        assert!(t.is_empty());
        assert_eq!(t.capacity(), INITIAL_CAPACITY);
    }

    /// std HashMap 과 무작위 연산 결과를 비교하는 모델 기반 테스트
    #[test]
    fn matches_std_hashmap_under_random_ops() {
        let mut ours = HashTable::new();
        let mut reference = HashMap::new();
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..100_000 {
            let key = format!("k{}", next() % 2_000);
            match next() % 3 {
                0 | 1 => {
                    let v = next();
                    assert_eq!(ours.insert(key.clone(), v), reference.insert(key, v));
                }
                _ => assert_eq!(ours.remove(key.as_str()), reference.remove(&key)),
            }
            assert_eq!(ours.len(), reference.len());
        }
        for (k, v) in &reference {
            assert_eq!(ours.get(k.as_str()), Some(v));
        }
        assert_eq!(ours.iter().count(), reference.len());
    }
}
