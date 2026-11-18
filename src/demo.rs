//! 기능 테스트/성능 비교용 데모: "느린 상품 API" + Mini Redis 캐시 (cache-aside 패턴)
//!
//! ```text
//!  GET /products/42
//!     │
//!     ├─▶ Mini Redis GET demo:product:42 ──HIT──▶ 바로 응답 (x-cache: HIT)
//!     │           │ MISS
//!     │           ▼
//!     │   (single-flight 락: 같은 키는 한 요청만 DB 로)
//!     │           ▼
//!     ├─▶ ProductDb.find(42)  (일부러 느림: latency + 커넥션 풀 제한)
//!     │           ▼
//!     └─▶ Mini Redis SET demo:product:42 (TTL)  → 응답 (x-cache: MISS)
//!
//!  PUT /products/42  → DB 갱신 후 demo:product:42, demo:products:* 무효화
//! ```
//!
//! 고려한 엣지 케이스
//! * **캐시 서버 장애**: Mini Redis 에 연결할 수 없으면 DB 로 우회해서 정상 응답 (x-cache: ERROR)
//! * **캐시 관통(penetration)**: 없는 상품 조회도 `null` 을 짧은 TTL 로 캐시 → DB 반복 조회 방지
//! * **캐시 쇄도(stampede)**: 같은 키 동시 MISS 시 하나만 DB 조회, 나머지는 결과를 기다렸다 HIT
//! * **손상된 캐시 값**: 역직렬화 실패 시 해당 키 삭제 후 MISS 로 처리
//! * **데이터 변경**: 쓰기 시 관련 키를 삭제(write-invalidate) + TTL 로 최대 불일치 시간 제한

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

use crate::client::MiniRedisClient;

pub const PRODUCT_LIST_KEY: &str = "demo:products:all";
pub const PRODUCT_LIST_PATTERN: &str = "demo:products:*";

pub fn product_key(id: u64) -> String {
    format!("demo:product:{id}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Product {
    pub id: u64,
    pub name: String,
    pub price: u64,
    pub stock: u32,
    pub version: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductPatch {
    pub name: Option<String>,
    pub price: Option<u64>,
    pub stock: Option<u32>,
}

/// 느린 "데이터베이스" 시뮬레이터
pub struct ProductDb {
    rows: RwLock<HashMap<u64, Product>>,
    latency: Duration,
    /// 실제 DB 처럼 동시 커넥션 수 제한
    pool: Semaphore,
    queries: AtomicU64,
}

impl ProductDb {
    pub fn seeded(count: u64, latency: Duration, pool_size: usize) -> Self {
        let rows = (1..=count)
            .map(|id| {
                (
                    id,
                    Product {
                        id,
                        name: format!("Product #{id}"),
                        price: 1_000 + id * 10,
                        stock: (id * 7 % 50) as u32,
                        version: 1,
                    },
                )
            })
            .collect();
        Self {
            rows: RwLock::new(rows),
            latency,
            pool: Semaphore::new(pool_size.max(1)),
            queries: AtomicU64::new(0),
        }
    }

    /// 지금까지 DB 에 도달한 쿼리 수
    pub fn query_count(&self) -> u64 {
        self.queries.load(Ordering::Relaxed)
    }

    async fn simulate_query(&self) -> tokio::sync::SemaphorePermit<'_> {
        let permit = self.pool.acquire().await.expect("db pool closed");
        self.queries.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.latency).await;
        permit
    }

    pub async fn find(&self, id: u64) -> Option<Product> {
        let _conn = self.simulate_query().await;
        self.rows.read().expect("db lock poisoned").get(&id).cloned()
    }

    pub async fn list(&self) -> Vec<Product> {
        let _conn = self.simulate_query().await;
        let mut products: Vec<Product> = self
            .rows
            .read()
            .expect("db lock poisoned")
            .values()
            .cloned()
            .collect();
        products.sort_by_key(|p| p.id);
        products
    }

    pub async fn update(&self, id: u64, patch: ProductPatch) -> Option<Product> {
        let _conn = self.simulate_query().await;
        let mut rows = self.rows.write().expect("db lock poisoned");
        let product = rows.get_mut(&id)?;
        if let Some(name) = patch.name {
            product.name = name;
        }
        if let Some(price) = patch.price {
            product.price = price;
        }
        if let Some(stock) = patch.stock {
            product.stock = stock;
        }
        product.version += 1;
        Some(product.clone())
    }
}

/// 응답 헤더 `x-cache` 값
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    Hit,
    Miss,
    Bypass,
    /// 캐시 서버 장애 → DB 로 우회
    Error,
}

impl CacheStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "HIT",
            Self::Miss => "MISS",
            Self::Bypass => "BYPASS",
            Self::Error => "ERROR",
        }
    }
}

enum Lookup<T> {
    /// 캐시에 있음 (None = "없는 상품"이라는 사실이 캐시됨)
    Hit(Option<T>),
    Miss,
    Unavailable,
}

#[derive(Clone)]
pub struct DemoState {
    pub db: Arc<ProductDb>,
    cache: Option<MiniRedisClient>,
    cache_ttl: Duration,
    negative_ttl: Duration,
    inflight: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

impl DemoState {
    /// `cache` 가 None 이면 캐시 없이 동작
    pub fn new(db: Arc<ProductDb>, cache: Option<MiniRedisClient>) -> Self {
        Self {
            db,
            cache,
            cache_ttl: Duration::from_secs(30),
            negative_ttl: Duration::from_secs(5),
            inflight: Arc::default(),
        }
    }

    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = ttl;
        self
    }

    pub fn with_negative_ttl(mut self, ttl: Duration) -> Self {
        self.negative_ttl = ttl;
        self
    }

    /// Cache-aside 조회: 캐시 → (single-flight) → 원본 로드 → 캐시 저장
    pub async fn cached<T, F, Fut>(&self, key: &str, use_cache: bool, load: F) -> (Option<T>, CacheStatus)
    where
        T: Serialize + DeserializeOwned + Send,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Option<T>> + Send,
    {
        let cache = match (&self.cache, use_cache) {
            (Some(cache), true) => cache,
            _ => return (load().await, CacheStatus::Bypass),
        };

        match lookup::<T>(cache, key).await {
            Lookup::Hit(value) => return (value, CacheStatus::Hit),
            Lookup::Unavailable => return (load().await, CacheStatus::Error),
            Lookup::Miss => {}
        }

        // 같은 키에 대한 동시 MISS 는 한 요청만 원본을 조회 (나머지는 대기)
        let lock = self.inflight_lock(key);
        let guard = lock.lock().await;
        // 대기하는 동안 다른 요청이 채웠을 수 있으므로 다시 확인 (double-checked)
        if let Lookup::Hit(value) = lookup::<T>(cache, key).await {
            drop(guard);
            self.release_inflight(key, lock);
            return (value, CacheStatus::Hit);
        }

        let value = load().await;
        let (payload, ttl) = match &value {
            Some(v) => (
                serde_json::to_string(v).expect("value must serialize"),
                self.cache_ttl,
            ),
            None => ("null".to_string(), self.negative_ttl),
        };
        let status = match cache.set(key, &payload, Some(ttl)).await {
            Ok(()) => CacheStatus::Miss,
            Err(err) => {
                tracing::warn!(%err, key, "failed to populate cache");
                CacheStatus::Error
            }
        };
        drop(guard);
        self.release_inflight(key, lock);
        (value, status)
    }

    fn inflight_lock(&self, key: &str) -> Arc<AsyncMutex<()>> {
        let mut map = self.inflight.lock().expect("inflight map poisoned");
        map.entry(key.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn release_inflight(&self, key: &str, lock: Arc<AsyncMutex<()>>) {
        let mut map = self.inflight.lock().expect("inflight map poisoned");
        // map 이 1개, 우리가 1개를 쥐고 있으면 기다리는 요청이 없으므로 정리
        if Arc::strong_count(&lock) <= 2 {
            map.remove(key);
        }
    }

    /// 상품 변경 시 관련 캐시 무효화. 모두 성공하면 true
    pub async fn invalidate_product(&self, id: u64) -> bool {
        let Some(cache) = &self.cache else {
            return false;
        };
        let single = cache.del(&product_key(id)).await;
        let lists = cache.delete_pattern(PRODUCT_LIST_PATTERN).await;
        match (single, lists) {
            (Ok(_), Ok(_)) => true,
            (Err(err), _) | (_, Err(err)) => {
                tracing::warn!(%err, id, "cache invalidation failed; stale data lives until TTL");
                false
            }
        }
    }
}

async fn lookup<T: DeserializeOwned>(cache: &MiniRedisClient, key: &str) -> Lookup<T> {
    match cache.get(key).await {
        Ok(Some(raw)) => match serde_json::from_str::<Option<T>>(&raw) {
            Ok(value) => Lookup::Hit(value),
            Err(err) => {
                tracing::warn!(%err, key, "corrupted cache entry; deleting");
                let _ = cache.del(key).await;
                Lookup::Miss
            }
        },
        Ok(None) => Lookup::Miss,
        Err(err) => {
            tracing::warn!(%err, key, "cache unavailable; falling back to origin");
            Lookup::Unavailable
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct CacheQuery {
    cache: Option<bool>,
}

fn with_cache_header(mut resp: Response, status: CacheStatus) -> Response {
    resp.headers_mut()
        .insert("x-cache", HeaderValue::from_static(status.as_str()));
    resp
}

async fn get_product(
    State(state): State<DemoState>,
    Path(id): Path<u64>,
    Query(query): Query<CacheQuery>,
) -> Response {
    let db = state.db.clone();
    let (product, status) = state
        .cached(&product_key(id), query.cache.unwrap_or(true), move || async move {
            db.find(id).await
        })
        .await;
    let resp = match product {
        Some(p) => Json(p).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "product_not_found", "id": id })),
        )
            .into_response(),
    };
    with_cache_header(resp, status)
}

async fn list_products(State(state): State<DemoState>, Query(query): Query<CacheQuery>) -> Response {
    let db = state.db.clone();
    let (products, status) = state
        .cached(PRODUCT_LIST_KEY, query.cache.unwrap_or(true), move || async move {
            Some(db.list().await)
        })
        .await;
    with_cache_header(Json(products.unwrap_or_default()).into_response(), status)
}

async fn update_product(
    State(state): State<DemoState>,
    Path(id): Path<u64>,
    Json(patch): Json<ProductPatch>,
) -> Response {
    let Some(updated) = state.db.update(id, patch).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "product_not_found", "id": id })),
        )
            .into_response();
    };
    // DB 반영 "후" 캐시 삭제 (반대 순서면 삭제 직후 옛 값이 다시 캐시될 수 있음)
    let invalidated = state.invalidate_product(id).await;
    let mut resp = Json(updated).into_response();
    resp.headers_mut().insert(
        "x-cache-invalidated",
        HeaderValue::from_static(if invalidated { "true" } else { "false" }),
    );
    resp
}

async fn demo_stats(State(state): State<DemoState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "db_queries": state.db.query_count() }))
}

pub fn router(state: DemoState) -> Router {
    Router::new()
        .route("/products", get(list_products))
        .route("/products/{id}", get(get_product).put(update_product))
        .route("/stats", get(demo_stats))
        .with_state(state)
}
