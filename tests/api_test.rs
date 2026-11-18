//! 기능 테스트: 실제 HTTP 서버를 띄우고 외부 클라이언트 입장에서 API 를 검증

mod common;

use std::time::Duration;

use mini_redis::client::{MiniRedisClient, SetMode};
use mini_redis::engine::Engine;
use mini_redis::store::Ttl;
use reqwest::StatusCode;
use serde_json::{json, Value};

async fn setup() -> (String, MiniRedisClient, reqwest::Client) {
    let base = common::spawn_mini_redis(Engine::in_memory()).await;
    let client = MiniRedisClient::new(&base).unwrap();
    (base, client, reqwest::Client::new())
}

#[tokio::test]
async fn health_and_route_not_found() {
    let (base, client, http) = setup().await;
    assert!(client.health().await.unwrap());
    let resp = http.get(format!("{base}/nope")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "route_not_found");
}

#[tokio::test]
async fn set_get_delete_roundtrip() {
    let (_, redis, _) = setup().await;
    assert_eq!(redis.get("user:1").await.unwrap(), None);
    redis.set("user:1", "alice", None).await.unwrap();
    assert_eq!(redis.get("user:1").await.unwrap().as_deref(), Some("alice"));
    redis.set("user:1", "bob", None).await.unwrap();
    assert_eq!(redis.get("user:1").await.unwrap().as_deref(), Some("bob"), "덮어쓰기");
    assert!(redis.del("user:1").await.unwrap());
    assert!(!redis.del("user:1").await.unwrap(), "두 번째 삭제는 false");
    assert_eq!(redis.get("user:1").await.unwrap(), None);
}

#[tokio::test]
async fn raw_http_contract() {
    let (base, _, http) = setup().await;
    let resp = http
        .put(format!("{base}/keys/greeting"))
        .json(&json!({ "value": "hello", "ex": 60 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "key": "greeting", "ttl_ms": 60000 }));

    let body: Value = http
        .get(format!("{base}/keys/greeting"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["value"], "hello");
    let ttl = body["ttl_ms"].as_u64().unwrap();
    assert!(ttl > 59_000 && ttl <= 60_000);

    let resp = http.get(format!("{base}/keys/missing")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "not_found");
}

#[tokio::test]
async fn expired_value_is_never_returned() {
    let (_, redis, _) = setup().await;
    redis
        .set("session", "token", Some(Duration::from_millis(150)))
        .await
        .unwrap();
    assert_eq!(redis.get("session").await.unwrap().as_deref(), Some("token"));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(redis.get("session").await.unwrap(), None, "만료 후 조회 → 없음");
    assert_eq!(redis.ttl("session").await.unwrap(), Ttl::Missing);
    assert!(!redis.del("session").await.unwrap());
}

#[tokio::test]
async fn ttl_expire_persist() {
    let (_, redis, _) = setup().await;
    redis.set("k", "v", None).await.unwrap();
    assert_eq!(redis.ttl("k").await.unwrap(), Ttl::Persistent);
    assert!(redis.expire("k", Duration::from_secs(10)).await.unwrap());
    match redis.ttl("k").await.unwrap() {
        Ttl::ExpiresIn(ms) => assert!(ms > 9_000 && ms <= 10_000),
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(redis.persist("k").await.unwrap(), Some(true));
    assert_eq!(redis.persist("k").await.unwrap(), Some(false));
    assert_eq!(redis.ttl("k").await.unwrap(), Ttl::Persistent);
    assert!(!redis.expire("nope", Duration::from_secs(1)).await.unwrap());
    assert_eq!(redis.persist("nope").await.unwrap(), None);
}

#[tokio::test]
async fn nx_and_xx() {
    let (base, redis, http) = setup().await;
    assert!(!redis.set_with("lock", "a", None, SetMode::Xx).await.unwrap());
    assert!(redis.set_with("lock", "a", None, SetMode::Nx).await.unwrap());
    assert!(!redis.set_with("lock", "b", None, SetMode::Nx).await.unwrap());
    assert!(redis.set_with("lock", "c", None, SetMode::Xx).await.unwrap());
    assert_eq!(redis.get("lock").await.unwrap().as_deref(), Some("c"));

    let resp = http
        .put(format!("{base}/keys/lock"))
        .json(&json!({ "value": "x", "nx": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn incr_semantics() {
    let (base, redis, http) = setup().await;
    assert_eq!(redis.incr("views", 1).await.unwrap(), 1);
    assert_eq!(redis.incr("views", 10).await.unwrap(), 11);
    assert_eq!(redis.incr("views", -20).await.unwrap(), -9);

    // 바디 없이 POST → by=1
    let body: Value = http
        .post(format!("{base}/keys/views/incr"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["value"], -8);

    redis.set("name", "alice", None).await.unwrap();
    let err = redis.incr("name", 1).await.unwrap_err();
    assert_eq!(err.code(), Some("not_an_integer"));

    redis.set("big", &i64::MAX.to_string(), None).await.unwrap();
    assert_eq!(redis.incr("big", 1).await.unwrap_err().code(), Some("overflow"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_http_increments_lose_no_updates() {
    let (_, redis, _) = setup().await;
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let redis = redis.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..20 {
                redis.incr("counter", 1).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(redis.get("counter").await.unwrap().as_deref(), Some("1000"));
}

#[tokio::test]
async fn pattern_listing_and_invalidation() {
    let (base, redis, http) = setup().await;
    for k in ["user:1", "user:2", "user:2:profile", "post:1"] {
        redis.set(k, "v", None).await.unwrap();
    }
    assert_eq!(redis.keys("user:*").await.unwrap(), vec!["user:1", "user:2", "user:2:profile"]);
    assert_eq!(redis.keys("user:?").await.unwrap(), vec!["user:1", "user:2"]);

    let body: Value = http
        .get(format!("{base}/keys?pattern=*&limit=2"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["count"], 2);
    assert_eq!(body["truncated"], true);

    assert_eq!(redis.delete_pattern("user:*").await.unwrap(), 3);
    assert_eq!(redis.keys("*").await.unwrap(), vec!["post:1"]);

    // pattern 없는 일괄 삭제는 거부 (실수로 전체 삭제 방지)
    let resp = http.delete(format!("{base}/keys")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    assert_eq!(redis.flush().await.unwrap(), 1);
    assert!(redis.keys("*").await.unwrap().is_empty());
}

#[tokio::test]
async fn special_character_keys() {
    let (_, redis, _) = setup().await;
    let keys = [
        "a/b/c",
        "공백 있는 키",
        "query?x=1&y=2",
        "hash#frag",
        "percent%20",
        "emoji🚀",
        "colon:and;semicolon",
    ];
    for (i, k) in keys.iter().enumerate() {
        redis.set(k, &format!("v{i}"), None).await.unwrap();
    }
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(redis.get(k).await.unwrap(), Some(format!("v{i}")), "key {k:?}");
    }
    assert_eq!(redis.keys("*").await.unwrap().len(), keys.len());
}

#[tokio::test]
async fn empty_and_large_values() {
    let (base, redis, http) = setup().await;
    redis.set("empty", "", None).await.unwrap();
    assert_eq!(redis.get("empty").await.unwrap().as_deref(), Some(""));

    let big = "x".repeat(800 * 1024);
    redis.set("big", &big, None).await.unwrap();
    assert_eq!(redis.get("big").await.unwrap().map(|v| v.len()), Some(big.len()));

    // 값 상한(1MB) 초과
    let too_big = "x".repeat(1024 * 1024 + 1);
    let resp = http
        .put(format!("{base}/keys/huge"))
        .json(&json!({ "value": too_big }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // 바디 상한 초과: 서버가 413 으로 응답하거나, 바디를 다 받기 전에 연결을 끊음
    let result = http
        .put(format!("{base}/keys/huge"))
        .header("content-type", "application/json")
        .body(vec![b' '; 3 * 1024 * 1024])
        .send()
        .await;
    if let Ok(resp) = result {
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
    assert_eq!(redis.get("huge").await.unwrap(), None, "어느 경우든 저장되면 안 됨");
}

#[tokio::test]
async fn invalid_requests_are_rejected_with_json_errors() {
    let (base, _, http) = setup().await;
    let put = |body: Value| {
        let http = http.clone();
        let url = format!("{base}/keys/k");
        async move { http.put(url).json(&body).send().await.unwrap() }
    };

    let cases = [
        (json!({ "value": "v", "ex": 1, "px": 1000 }), "ex 와 px 동시 사용"),
        (json!({ "value": "v", "ex": 0 }), "0 TTL"),
        (json!({ "value": "v", "px": 0 }), "0 TTL(ms)"),
        (json!({ "value": "v", "ex": u64::MAX }), "TTL overflow"),
        (json!({ "value": "v", "nx": true, "xx": true }), "nx 와 xx 동시 사용"),
        (json!({ "value": 123 }), "문자열이 아닌 값"),
        (json!({ "ttl": 10 }), "value 누락 / 알 수 없는 필드"),
        (json!({ "value": "v", "ex": -1 }), "음수 TTL"),
    ];
    for (body, why) in cases {
        let resp = put(body).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{why}");
        let err: Value = resp.json().await.unwrap();
        assert_eq!(err["error"], "bad_request", "{why}");
    }

    // 깨진 JSON / 빈 바디
    for raw in ["{not json", ""] {
        let resp = http
            .put(format!("{base}/keys/k"))
            .header("content-type", "application/json")
            .body(raw)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "body {raw:?}");
    }

    // 너무 긴 키
    let long_key = "k".repeat(2000);
    let resp = http.get(format!("{base}/keys/{long_key}")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // expire 에 ex/px 가 없음
    let resp = http
        .put(format!("{base}/keys/k/expire"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 지원하지 않는 메서드
    let resp = http.post(format!("{base}/keys/k")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn stats_reflect_activity() {
    let (_, redis, _) = setup().await;
    redis.set("a", "1", None).await.unwrap();
    redis.get("a").await.unwrap();
    redis.get("missing").await.unwrap();
    let stats = redis.stats().await.unwrap();
    assert_eq!(stats["keys"], 1);
    assert_eq!(stats["hits"], 1);
    assert_eq!(stats["misses"], 1);
    assert!(stats["persistence"].is_null());
}

#[tokio::test]
async fn returns_503_when_engine_is_down() {
    let engine = Engine::in_memory();
    let base = common::spawn_mini_redis(engine.clone()).await;
    engine.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let resp = reqwest::get(format!("{base}/keys/a")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}
