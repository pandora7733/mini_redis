//! 성능 비교 벤치마크
//!
//! 1. 해시 테이블 마이크로 벤치마크: 직접 구현한 HashTable vs std::HashMap
//! 2. 데모 API 부하 테스트: 캐시 없음 vs Mini Redis 캐시 (cold / warm)
//!
//! ```bash
//! cargo run --release --bin bench
//! cargo run --release --bin bench -- --requests 5000 --concurrency 100 --db-latency-ms 30
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use mini_redis::client::MiniRedisClient;
use mini_redis::demo::{self, DemoState, ProductDb};
use mini_redis::engine::Engine;
use mini_redis::hashtable::{FnvBuildHasher, HashTable};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "bench", about = "Mini Redis 성능 비교")]
struct Args {
    /// 시나리오별 총 요청 수
    #[arg(long, default_value_t = 3_000)]
    requests: usize,
    /// 동시 요청 수
    #[arg(long, default_value_t = 50)]
    concurrency: usize,
    /// DB 쿼리 1회 지연 (ms)
    #[arg(long, default_value_t = 20)]
    db_latency_ms: u64,
    /// DB 동시 커넥션 수
    #[arg(long, default_value_t = 10)]
    db_pool: usize,
    /// 상품 수 (요청은 1..=products 중 무작위)
    #[arg(long, default_value_t = 100)]
    products: u64,
    /// 해시 테이블 마이크로 벤치마크 키 수
    #[arg(long, default_value_t = 1_000_000)]
    hashtable_keys: usize,
}

macro_rules! bench_map {
    ($label:expr, $map:expr, $keys:expr) => {{
        let keys: &[String] = $keys;
        let mut map = $map;
        let start = Instant::now();
        for (i, k) in keys.iter().enumerate() {
            map.insert(k.clone(), i);
        }
        let insert = start.elapsed();
        let start = Instant::now();
        let mut sum = 0usize;
        for k in keys {
            sum = sum.wrapping_add(*map.get(k.as_str()).unwrap());
        }
        let get = start.elapsed();
        black_box(sum);
        let start = Instant::now();
        for k in keys {
            black_box(map.remove(k.as_str()));
        }
        let remove = start.elapsed();
        let per = |d: Duration| d.as_nanos() as f64 / keys.len() as f64;
        println!(
            "{:<28} insert {:>7.1} ns/op   get {:>7.1} ns/op   remove {:>7.1} ns/op",
            $label,
            per(insert),
            per(get),
            per(remove)
        );
    }};
}

fn bench_hashtables(n: usize) {
    println!("\n== 1. 해시 테이블 마이크로 벤치마크 ({n} keys, 단일 스레드) ==");
    let keys: Vec<String> = (0..n).map(|i| format!("user:{i}")).collect();
    bench_map!("mini_redis::HashTable(FNV)", HashTable::new(), &keys);
    bench_map!("std::HashMap(FNV)", HashMap::with_hasher(FnvBuildHasher), &keys);
    bench_map!("std::HashMap(SipHash)", HashMap::new(), &keys);
}

async fn spawn_router(router: axum::Router) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("server failed");
    });
    Ok(addr)
}

struct Report {
    name: &'static str,
    latencies: Vec<Duration>,
    elapsed: Duration,
    hits: usize,
    db_queries: u64,
}

impl Report {
    fn percentile(&self, q: f64) -> f64 {
        let idx = ((self.latencies.len() - 1) as f64 * q).round() as usize;
        self.latencies[idx].as_secs_f64() * 1_000.0
    }

    fn avg_ms(&self) -> f64 {
        self.latencies.iter().map(Duration::as_secs_f64).sum::<f64>() / self.latencies.len() as f64 * 1_000.0
    }

    fn rps(&self) -> f64 {
        self.latencies.len() as f64 / self.elapsed.as_secs_f64()
    }

    fn print(&self) {
        println!(
            "{:<22} {:>9.0} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>8.1}% {:>10}",
            self.name,
            self.rps(),
            self.avg_ms(),
            self.percentile(0.50),
            self.percentile(0.95),
            self.percentile(0.99),
            self.hits as f64 / self.latencies.len() as f64 * 100.0,
            self.db_queries
        );
    }
}

async fn run_scenario(
    name: &'static str,
    http: &reqwest::Client,
    base: &str,
    query: &'static str,
    args: &Args,
    db: &ProductDb,
) -> Report {
    let before = db.query_count();
    let workers = args.concurrency.max(1);
    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let n = args.requests / workers + usize::from(w < args.requests % workers);
        let http = http.clone();
        let base = base.to_string();
        let products = args.products.max(1);
        handles.push(tokio::spawn(async move {
            let mut seed = (w as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut latencies = Vec::with_capacity(n);
            let mut hits = 0;
            for _ in 0..n {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let id = seed % products + 1;
                let t = Instant::now();
                let resp = http
                    .get(format!("{base}/products/{id}{query}"))
                    .send()
                    .await
                    .expect("request failed");
                if resp.headers().get("x-cache").map(|v| v.as_bytes()) == Some(b"HIT".as_slice()) {
                    hits += 1;
                }
                resp.bytes().await.expect("body failed");
                latencies.push(t.elapsed());
            }
            (latencies, hits)
        }));
    }
    let mut latencies = Vec::with_capacity(args.requests);
    let mut hits = 0;
    for h in handles {
        let (l, hit) = h.await.expect("worker panicked");
        latencies.extend(l);
        hits += hit;
    }
    let elapsed = start.elapsed();
    latencies.sort_unstable();
    Report {
        name,
        latencies,
        elapsed,
        hits,
        db_queries: db.query_count() - before,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.requests == 0 {
        return Err("--requests must be greater than 0".into());
    }

    bench_hashtables(args.hashtable_keys);

    // Mini Redis 서버 + 데모 API 를 같은 프로세스에서 띄움 (통신은 실제 HTTP)
    let engine = Engine::in_memory();
    let redis_addr = spawn_router(mini_redis::api::router(engine.clone())).await?;
    let cache = MiniRedisClient::with_timeout(&format!("http://{redis_addr}"), Duration::from_secs(2))?;
    let db = Arc::new(ProductDb::seeded(
        args.products,
        Duration::from_millis(args.db_latency_ms),
        args.db_pool,
    ));
    let state = DemoState::new(db.clone(), Some(cache.clone())).with_cache_ttl(Duration::from_secs(300));
    let demo_addr = spawn_router(demo::router(state)).await?;
    let base = format!("http://{demo_addr}");
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency)
        .build()?;

    println!(
        "\n== 2. 데모 API 부하 테스트 (요청 {} / 동시성 {} / DB 지연 {}ms, 커넥션 {}개 / 상품 {}개) ==",
        args.requests, args.concurrency, args.db_latency_ms, args.db_pool, args.products
    );
    println!(
        "{:<22} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10}",
        "scenario", "req/s", "avg(ms)", "p50(ms)", "p95(ms)", "p99(ms)", "hit", "db_query"
    );
    let no_cache = run_scenario("no cache (DB only)", &http, &base, "?cache=false", &args, &db).await;
    no_cache.print();
    let cold = run_scenario("mini redis (cold)", &http, &base, "", &args, &db).await;
    cold.print();
    let warm = run_scenario("mini redis (warm)", &http, &base, "", &args, &db).await;
    warm.print();

    println!(
        "\n→ warm 캐시: 처리량 {:.1}배, 평균 지연 {:.1}배 감소, DB 쿼리 {} → {}",
        warm.rps() / no_cache.rps(),
        no_cache.avg_ms() / warm.avg_ms(),
        no_cache.db_queries,
        warm.db_queries
    );

    // Mini Redis 자체 왕복 지연 (HTTP GET 1회)
    cache.set("bench:ping", "pong", None).await?;
    let mut samples = Vec::with_capacity(1_000);
    for _ in 0..1_000 {
        let t = Instant::now();
        black_box(cache.get("bench:ping").await?);
        samples.push(t.elapsed());
    }
    samples.sort_unstable();
    println!(
        "\n== 3. Mini Redis HTTP GET 왕복 지연 (순차 1000회) ==\np50 {:.3}ms / p99 {:.3}ms",
        samples[499].as_secs_f64() * 1e3,
        samples[989].as_secs_f64() * 1e3
    );
    let stats = engine.stats().await?;
    println!(
        "engine: keys={} hits={} misses={} processed={}",
        stats.store.keys, stats.store.hits, stats.store.misses, stats.processed_commands
    );
    Ok(())
}
