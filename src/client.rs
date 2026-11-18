//! Mini Redis HTTP API 용 Rust 클라이언트
//!
//! ```no_run
//! # async fn demo() -> Result<(), mini_redis::client::ClientError> {
//! use std::time::Duration;
//! use mini_redis::client::MiniRedisClient;
//!
//! let redis = MiniRedisClient::new("http://127.0.0.1:6380")?;
//! redis.set("greeting", "hello", Some(Duration::from_secs(60))).await?;
//! assert_eq!(redis.get("greeting").await?, Some("hello".to_string()));
//! # Ok(()) }
//! ```

use std::time::Duration;

use reqwest::{StatusCode, Url};
use serde::de::DeserializeOwned;

use crate::api::{
    DeleteResponse, ErrorBody, ExpireRequest, IncrRequest, IncrResponse, KeysResponse,
    PersistResponse, SetRequest, TtlResponse, ValueResponse,
};
use crate::store::Ttl;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("invalid base url: {0}")]
    InvalidUrl(String),
    #[error("invalid key: {0}")]
    InvalidKey(&'static str),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server returned {status}: {code} - {message}")]
    Server {
        status: u16,
        code: String,
        message: String,
    },
}

impl ClientError {
    /// 서버가 돌려준 에러 코드 (예: "not_an_integer")
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Server { code, .. } => Some(code),
            _ => None,
        }
    }
}

/// SET 조건
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetMode {
    #[default]
    Always,
    /// 없을 때만
    Nx,
    /// 있을 때만
    Xx,
}

#[derive(Debug, Clone)]
pub struct MiniRedisClient {
    base: Url,
    http: reqwest::Client,
}

fn to_millis(d: Duration) -> u64 {
    // 1ms 미만 TTL 은 0 이 되어 거절되므로 최소 1ms 로 올림
    (d.as_millis() as u64).max(u64::from(!d.is_zero()))
}

impl MiniRedisClient {
    /// 기본 타임아웃 3초
    pub fn new(base_url: &str) -> Result<Self, ClientError> {
        Self::with_timeout(base_url, Duration::from_secs(3))
    }

    pub fn with_timeout(base_url: &str, timeout: Duration) -> Result<Self, ClientError> {
        let base = Url::parse(base_url).map_err(|e| ClientError::InvalidUrl(e.to_string()))?;
        if base.cannot_be_a_base() {
            return Err(ClientError::InvalidUrl(base_url.to_string()));
        }
        let http = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self { base, http })
    }

    /// path segment 단위로 URL 을 만들어 키의 `/`, `?`, `#`, 한글 등을 안전하게 퍼센트 인코딩
    fn url(&self, segments: &[&str]) -> Url {
        let mut url = self.base.clone();
        {
            let mut path = url.path_segments_mut().expect("base url validated in constructor");
            path.pop_if_empty().extend(segments);
        }
        url
    }

    fn key_url(&self, key: &str, suffix: Option<&str>) -> Result<Url, ClientError> {
        if key.is_empty() {
            return Err(ClientError::InvalidKey("key must not be empty"));
        }
        Ok(match suffix {
            Some(s) => self.url(&["keys", key, s]),
            None => self.url(&["keys", key]),
        })
    }

    async fn error_from(resp: reqwest::Response) -> ClientError {
        let status = resp.status().as_u16();
        match resp.json::<ErrorBody>().await {
            Ok(body) => ClientError::Server {
                status,
                code: body.error,
                message: body.message,
            },
            Err(_) => ClientError::Server {
                status,
                code: "unknown".into(),
                message: String::new(),
            },
        }
    }

    /// 200 → Some(T), 404 → None, 그 외 → Err
    async fn optional<T: DeserializeOwned>(resp: reqwest::Response) -> Result<Option<T>, ClientError> {
        match resp.status() {
            s if s.is_success() => Ok(Some(resp.json().await?)),
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(Self::error_from(resp).await),
        }
    }

    async fn required<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T, ClientError> {
        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            Err(Self::error_from(resp).await)
        }
    }

    pub async fn health(&self) -> Result<bool, ClientError> {
        let resp = self.http.get(self.url(&["health"])).send().await?;
        Ok(resp.status().is_success())
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, ClientError> {
        Ok(self.get_entry(key).await?.map(|r| r.value))
    }

    /// 값과 남은 TTL 을 함께 조회
    pub async fn get_entry(&self, key: &str) -> Result<Option<ValueResponse>, ClientError> {
        let resp = self.http.get(self.key_url(key, None)?).send().await?;
        Self::optional(resp).await
    }

    pub async fn set(&self, key: &str, value: &str, ttl: Option<Duration>) -> Result<(), ClientError> {
        self.set_with(key, value, ttl, SetMode::Always).await.map(|_| ())
    }

    /// 조건부 SET. 조건 불충족(409)이면 Ok(false)
    pub async fn set_with(
        &self,
        key: &str,
        value: &str,
        ttl: Option<Duration>,
        mode: SetMode,
    ) -> Result<bool, ClientError> {
        let body = SetRequest {
            value: value.to_string(),
            ex: None,
            px: ttl.map(to_millis),
            nx: mode == SetMode::Nx,
            xx: mode == SetMode::Xx,
        };
        let resp = self.http.put(self.key_url(key, None)?).json(&body).send().await?;
        match resp.status() {
            s if s.is_success() => Ok(true),
            StatusCode::CONFLICT => Ok(false),
            _ => Err(Self::error_from(resp).await),
        }
    }

    /// 삭제했으면 true, 없던 키면 false
    pub async fn del(&self, key: &str) -> Result<bool, ClientError> {
        let resp = self.http.delete(self.key_url(key, None)?).send().await?;
        Ok(Self::optional::<DeleteResponse>(resp).await?.is_some())
    }

    pub async fn ttl(&self, key: &str) -> Result<Ttl, ClientError> {
        let resp = self.http.get(self.key_url(key, Some("ttl"))?).send().await?;
        Ok(match Self::optional::<TtlResponse>(resp).await? {
            None => Ttl::Missing,
            Some(TtlResponse { ttl_ms: None, .. }) => Ttl::Persistent,
            Some(TtlResponse { ttl_ms: Some(ms), .. }) => Ttl::ExpiresIn(ms),
        })
    }

    /// TTL 설정. 키가 없으면 false
    pub async fn expire(&self, key: &str, ttl: Duration) -> Result<bool, ClientError> {
        let body = ExpireRequest {
            ex: None,
            px: Some(to_millis(ttl)),
        };
        let resp = self
            .http
            .put(self.key_url(key, Some("expire"))?)
            .json(&body)
            .send()
            .await?;
        Ok(Self::optional::<TtlResponse>(resp).await?.is_some())
    }

    /// TTL 제거. 키가 없으면 None, 있으면 TTL 이 있었는지 여부
    pub async fn persist(&self, key: &str) -> Result<Option<bool>, ClientError> {
        let resp = self.http.delete(self.key_url(key, Some("expire"))?).send().await?;
        Ok(Self::optional::<PersistResponse>(resp).await?.map(|r| r.persisted))
    }

    pub async fn incr(&self, key: &str, by: i64) -> Result<i64, ClientError> {
        let resp = self
            .http
            .post(self.key_url(key, Some("incr"))?)
            .json(&IncrRequest { by: Some(by) })
            .send()
            .await?;
        Ok(Self::required::<IncrResponse>(resp).await?.value)
    }

    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>, ClientError> {
        let resp = self
            .http
            .get(self.url(&["keys"]))
            .query(&[("pattern", pattern), ("limit", "100000")])
            .send()
            .await?;
        Ok(Self::required::<KeysResponse>(resp).await?.keys)
    }

    /// 패턴에 맞는 키를 모두 무효화하고 삭제 개수 반환
    pub async fn delete_pattern(&self, pattern: &str) -> Result<u64, ClientError> {
        let resp = self
            .http
            .delete(self.url(&["keys"]))
            .query(&[("pattern", pattern)])
            .send()
            .await?;
        Ok(Self::required::<DeleteResponse>(resp).await?.deleted)
    }

    pub async fn flush(&self) -> Result<u64, ClientError> {
        let resp = self.http.post(self.url(&["admin", "flush"])).send().await?;
        Ok(Self::required::<DeleteResponse>(resp).await?.deleted)
    }

    pub async fn stats(&self) -> Result<serde_json::Value, ClientError> {
        let resp = self.http.get(self.url(&["stats"])).send().await?;
        Self::required(resp).await
    }
}
