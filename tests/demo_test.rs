//! 기능 테스트: "간단한 API 의 응답을 Mini Redis 에 저장해 활용" 시나리오
//!
//! 실제 구성과 같이 [데모 API 서버] --HTTP--> [Mini Redis 서버] 로 띄워서 검증합니다.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use mini_redis::client::MiniRedisClient;
use mini_redis::demo::{self, product_key, DemoState, Product, ProductDb};
use mini_redis::engine::Engine;
use mini_redis::store::SetCondition;
use reqwest::StatusCode;
use serde_json::json;

struct Setup {
    base: String,
    db: Arc<ProductDb>,
    engine: Engine,
    http: reqwest::Client,
}

async fn setup_with(latency_ms: u64, configure: impl FnOnce(DemoState) -> DemoState) -> Setup {
    let engine = Engine::in_memory();
    let redis_url = common::spawn_mini_redis(engine.clone()).await;
    let cache = MiniRedisClient::with_timeout(&redis_url, Duration::from_millis(500)).unwrap();
    let db = Arc::new(ProductDb::seeded(20, Duration::from_millis(latency_ms), 10));
    let state = configure(DemoState::new(db.clone(), Some(cache)));
    let addr = common::spawn_router(demo::router(state)).await;
    Setup {
        base: format!("http://{addr}"),
        db,
        engine,
        http: reqwest::Client::new(),
    }
}

async fn setup(latency_ms: u64) -> Setup {
    setup_with(latency_ms, |s| s).await
}

async fn get(s: &Setup, path: &str) -> (StatusCode, String, Option<Product>) {
    let resp = s.http.get(format!("{}{path}", s.base)).send().await.unwrap();
    let status = resp.status();
    let cache = resp
        .headers()
        .get("x-cache")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let product = if status == StatusCode::OK {
        Some(resp.json().await.unwrap())
    } else {
        None
    };
    (status, cache, product)
}

#[tokio::test]
async fn miss_then_hit_and_value_is_stored_in_mini_redis() {
    let s = setup(20).await;
    let (status, cache, first) = get(&s, "/products/1").await;
    assert_eq!((status, cache.as_str()), (StatusCode::OK, "MISS"));
    let (_, cache, second) = get(&s, "/products/1").await;
    assert_eq!(cache, "HIT");
    assert_eq!(first, second);
    assert_eq!(s.db.query_count(), 1, "두 번째 요청은 DB 에 가지 않음");

    // Mini Redis 에 실제로 API 응답이 TTL 과 함께 저장되어 있는지 확인
    let stored = s.engine.get(product_key(1)).await.unwrap().unwrap();
    let cached: Product = serde_json::from_str(&stored.value).unwrap();
    assert_eq!(Some(cached), first);
    assert!(stored.ttl_ms.is_some());
}

#[tokio::test]
async fn bypass_always_hits_db() {
    let s = setup(5).await;
    for _ in 0..3 {
        let (_, cache, _) = get(&s, "/products/2?cache=false").await;
        assert_eq!(cache, "BYPASS");
    }
    assert_eq!(s.db.query_count(), 3);
}

#[tokio::test]
async fn update_invalidates_cached_entries() {
    let s = setup(5).await;
    get(&s, "/products/3").await;
    get(&s, "/products").await; // 목록 캐시
    assert_eq!(get(&s, "/products/3").await.1, "HIT");

    let resp = s
        .http
        .put(format!("{}/products/3", s.base))
        .json(&json!({ "price": 777 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["x-cache-invalidated"], "true");

    let (_, cache, product) = get(&s, "/products/3").await;
    assert_eq!(cache, "MISS", "무효화 후 첫 조회는 DB 에서 새로 읽음");
    let product = product.unwrap();
    assert_eq!(product.price, 777);
    assert_eq!(product.version, 2);

    let list_resp = s.http.get(format!("{}/products", s.base)).send().await.unwrap();
    assert_eq!(list_resp.headers()["x-cache"], "MISS", "목록 캐시도 패턴으로 무효화");
    let list: Vec<Product> = list_resp.json().await.unwrap();
    assert_eq!(list.iter().find(|p| p.id == 3).unwrap().price, 777);
}

#[tokio::test]
async fn missing_product_is_negatively_cached() {
    let s = setup(5).await;
    let (status, cache, _) = get(&s, "/products/9999").await;
    assert_eq!((status, cache.as_str()), (StatusCode::NOT_FOUND, "MISS"));
    let (status, cache, _) = get(&s, "/products/9999").await;
    assert_eq!((status, cache.as_str()), (StatusCode::NOT_FOUND, "HIT"));
    assert_eq!(s.db.query_count(), 1, "없는 상품 반복 조회가 DB 를 때리지 않음");
}

#[tokio::test]
async fn negative_cache_expires_quickly() {
    let s = setup_with(5, |st| st.with_negative_ttl(Duration::from_millis(150))).await;
    get(&s, "/products/9999").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (_, cache, _) = get(&s, "/products/9999").await;
    assert_eq!(cache, "MISS");
    assert_eq!(s.db.query_count(), 2);
}

#[tokio::test]
async fn cache_ttl_expiry_triggers_refetch() {
    let s = setup_with(5, |st| st.with_cache_ttl(Duration::from_millis(200))).await;
    assert_eq!(get(&s, "/products/4").await.1, "MISS");
    assert_eq!(get(&s, "/products/4").await.1, "HIT");
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(get(&s, "/products/4").await.1, "MISS", "만료된 캐시는 사용하지 않음");
    assert_eq!(s.db.query_count(), 2);
}

#[tokio::test]
async fn corrupted_cache_entry_is_recovered() {
    let s = setup(5).await;
    s.engine
        .set(product_key(5), "{not valid json".into(), None, SetCondition::Always)
        .await
        .unwrap();
    let (status, cache, product) = get(&s, "/products/5").await;
    assert_eq!((status, cache.as_str()), (StatusCode::OK, "MISS"));
    assert_eq!(product.unwrap().id, 5);
    assert_eq!(get(&s, "/products/5").await.1, "HIT");
}

#[tokio::test]
async fn works_when_cache_server_is_down() {
    let dead_url = format!("http://127.0.0.1:{}", common::unused_port());
    let cache = MiniRedisClient::with_timeout(&dead_url, Duration::from_millis(300)).unwrap();
    let db = Arc::new(ProductDb::seeded(5, Duration::from_millis(5), 10));
    let addr = common::spawn_router(demo::router(DemoState::new(db.clone(), Some(cache)))).await;
    let http = reqwest::Client::new();

    let resp = http.get(format!("http://{addr}/products/1")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "캐시 장애여도 서비스는 정상 응답");
    assert_eq!(resp.headers()["x-cache"], "ERROR");
    let product: Product = resp.json().await.unwrap();
    assert_eq!(product.id, 1);

    let resp = http
        .put(format!("http://{addr}/products/1"))
        .json(&json!({ "stock": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["x-cache-invalidated"], "false");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_misses_query_db_only_once() {
    let s = Arc::new(setup(100).await);
    let mut tasks = Vec::new();
    for _ in 0..30 {
        let s = s.clone();
        tasks.push(tokio::spawn(async move { get(&s, "/products/6").await }));
    }
    for t in tasks {
        let (status, _, product) = t.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(product.unwrap().id, 6);
    }
    assert_eq!(s.db.query_count(), 1, "cache stampede 방지 (single-flight)");
}

#[tokio::test]
async fn cached_responses_are_much_faster() {
    let s = setup(30).await;
    const N: u32 = 10;

    let start = Instant::now();
    for _ in 0..N {
        get(&s, "/products/7?cache=false").await;
    }
    let uncached = start.elapsed();

    let start = Instant::now();
    for _ in 0..N {
        get(&s, "/products/7").await; // 1 MISS + 9 HIT
    }
    let cached = start.elapsed();

    println!("uncached {uncached:?} vs cached {cached:?} ({N} requests)");
    assert!(uncached >= Duration::from_millis(30) * N);
    assert!(cached * 3 < uncached, "cached={cached:?}, uncached={uncached:?}");
}
