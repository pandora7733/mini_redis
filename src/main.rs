//! Mini Redis 서버 실행 파일
//!
//! ```bash
//! cargo run --release --bin mini-redis-server -- --port 6380 --data-dir ./data --fsync everysec
//! ```

use std::path::PathBuf;

use clap::Parser;
use mini_redis::engine::Engine;
use mini_redis::persistence::{FsyncPolicy, PersistenceConfig};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "mini-redis-server", about = "직접 구현한 해시 테이블 기반 Mini Redis (HTTP API)")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, short, default_value_t = 6380)]
    port: u16,
    /// AOF/스냅샷 저장 디렉터리
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,
    /// fsync 정책: always | everysec | no
    #[arg(long, default_value = "everysec")]
    fsync: FsyncPolicy,
    /// 영속성 끄기 (순수 인메모리)
    #[arg(long)]
    no_persist: bool,
    /// AOF 가 이 크기(MB)를 넘으면 스냅샷으로 압축
    #[arg(long, default_value_t = 64)]
    aof_rewrite_mb: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let args = Args::parse();

    let engine = if args.no_persist {
        tracing::info!("persistence disabled (in-memory only)");
        Engine::in_memory()
    } else {
        let config = PersistenceConfig {
            dir: args.data_dir.clone(),
            fsync: args.fsync,
            rewrite_threshold_bytes: args.aof_rewrite_mb.max(1) * 1024 * 1024,
        };
        let (engine, report) = Engine::open(config)?;
        tracing::info!(
            dir = %args.data_dir.display(),
            fsync = args.fsync.as_str(),
            snapshot_keys = report.snapshot_keys,
            aof_records = report.aof_records,
            truncated_bytes = report.truncated_bytes,
            live_keys = report.live_keys,
            "data restored"
        );
        engine
    };

    let listener = TcpListener::bind((args.host.as_str(), args.port)).await?;
    tracing::info!("Mini Redis listening on http://{}", listener.local_addr()?);

    mini_redis::api::serve(listener, engine.clone(), shutdown_signal()).await?;

    tracing::info!("shutting down: writing final snapshot");
    engine.shutdown().await?;
    tracing::info!("bye");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
