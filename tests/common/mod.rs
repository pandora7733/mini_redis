#![allow(dead_code)]

use std::net::SocketAddr;

use tokio::net::TcpListener;

/// 라우터를 임의 포트에 띄우고 주소를 반환
pub async fn spawn_router(router: axum::Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

/// Mini Redis API 서버를 띄우고 base URL 반환
pub async fn spawn_mini_redis(engine: mini_redis::engine::Engine) -> String {
    let addr = spawn_router(mini_redis::api::router(engine)).await;
    format!("http://{addr}")
}

/// 아무도 듣고 있지 않은 포트 (캐시 서버 다운 시뮬레이션)
pub fn unused_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}
