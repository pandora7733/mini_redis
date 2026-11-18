//! 데모 상품 API 서버 (Mini Redis 를 캐시로 사용)
//!
//! ```bash
//! cargo run --release --bin mini-redis-server -- --no-persist &
//! cargo run --release --bin demo_api -- --redis-url http://127.0.0.1:6380
//! curl -i http://127.0.0.1:3000/products/1            # x-cache: MISS (느림)
//! curl -i http://127.0.0.1:3000/products/1            # x-cache: HIT  (빠름)
//! curl -i "http://127.0.0.1:3000/products/1?cache=false"   # 캐시 우회
//! curl -i -X PUT http://127.0.0.1:3000/products/1 -H 'content-type: application/json' -d '{"price":500}'
//! ```

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use mini_redis::client::MiniRedisClient;
use mini_redis::demo::{self, DemoState, ProductDb};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "demo_api", about = "느린 상품 API + Mini Redis 캐시 데모")]
struct Args {
    #[arg(long, default_value_t = 3000)]
    port: u16,
    #[arg(long, default_value = "http://127.0.0.1:6380")]
    redis_url: String,
    /// 캐시 끄기
    #[arg(long)]
    no_cache: bool,
    /// DB 쿼리 1회 지연 (ms)
    #[arg(long, default_value_t = 50)]
    db_latency_ms: u64,
    /// DB 동시 커넥션 수
    #[arg(long, default_value_t = 10)]
    db_pool: usize,
    #[arg(long, default_value_t = 100)]
    products: u64,
    #[arg(long, default_value_t = 30)]
    cache_ttl_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let args = Args::parse();

    let db = Arc::new(ProductDb::seeded(
        args.products,
        Duration::from_millis(args.db_latency_ms),
        args.db_pool,
    ));
    let cache = if args.no_cache {
        None
    } else {
        // 캐시가 느려지면 서비스 전체가 느려지지 않도록 짧은 타임아웃
        Some(MiniRedisClient::with_timeout(&args.redis_url, Duration::from_millis(500))?)
    };
    let state = DemoState::new(db, cache).with_cache_ttl(Duration::from_secs(args.cache_ttl_secs));

    let listener = TcpListener::bind(("127.0.0.1", args.port)).await?;
    tracing::info!(
        "demo API on http://{} (cache: {})",
        listener.local_addr()?,
        if args.no_cache { "off" } else { args.redis_url.as_str() }
    );
    axum::serve(listener, demo::router(state)).await?;
    Ok(())
}
