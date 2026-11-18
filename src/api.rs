//! HTTP REST API — 외부에서 Mini Redis 를 사용하는 창구
//!
//! | Method | Path | 설명 |
//! |--------|------|------|
//! | GET    | `/health` | 헬스 체크 |
//! | GET    | `/stats` | 통계 (키 수, hit/miss, 만료 수, load factor …) |
//! | GET    | `/keys?pattern=user:*&limit=100` | 패턴으로 키 목록 조회 |
//! | DELETE | `/keys?pattern=user:*` | 패턴에 맞는 키 일괄 무효화 |
//! | GET    | `/keys/{key}` | 값 조회 |
//! | PUT    | `/keys/{key}` | 값 저장 `{"value":"..","ex":60}` / `px`(ms) / `nx` / `xx` |
//! | DELETE | `/keys/{key}` | 삭제 |
//! | GET    | `/keys/{key}/ttl` | 남은 TTL |
//! | PUT    | `/keys/{key}/expire` | TTL 설정 `{"ex":10}` |
//! | DELETE | `/keys/{key}/expire` | TTL 제거 (영구 보관) |
//! | POST   | `/keys/{key}/incr` | 원자적 증가 `{"by":5}` |
//! | POST   | `/admin/flush` | 전체 삭제 |
//! | POST   | `/admin/save` | 즉시 스냅샷 저장 |
//!
//! 키에 `/`, 공백, 한글 등이 들어가면 URL 퍼센트 인코딩해서 보내면 됩니다 (`a/b` → `a%2Fb`).
//! 모든 에러는 `{"error": "<code>", "message": "<설명>"}` 형식의 JSON 으로 응답합니다.

use std::future::Future;

use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::engine::{Engine, EngineError, EngineStats};
use crate::store::{SetCondition, StoreError, Ttl};
use crate::{MAX_KEY_BYTES, MAX_TTL_MS, MAX_VALUE_BYTES};

/// 요청 바디 상한. JSON 이스케이프로 값이 커질 수 있어 값 상한보다 넉넉하게 둡니다.
pub const MAX_BODY_BYTES: usize = MAX_VALUE_BYTES * 2 + 4096;
/// KEYS 응답 기본/최대 개수
const DEFAULT_KEYS_LIMIT: usize = 1_000;
const MAX_KEYS_LIMIT: usize = 100_000;

// ───────────────────────────── 요청/응답 타입 ─────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetRequest {
    pub value: String,
    /// TTL (초)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ex: Option<u64>,
    /// TTL (밀리초)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub px: Option<u64>,
    /// 키가 없을 때만 저장
    #[serde(default, skip_serializing_if = "is_false")]
    pub nx: bool,
    /// 키가 있을 때만 저장
    #[serde(default, skip_serializing_if = "is_false")]
    pub xx: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpireRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ex: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub px: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<i64>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueResponse {
    pub key: String,
    pub value: String,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetResponse {
    pub key: String,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlResponse {
    pub key: String,
    /// 남은 시간 (ms). 영구 키면 null
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistResponse {
    pub key: String,
    /// TTL 이 있었고 제거되었으면 true
    pub persisted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrResponse {
    pub key: String,
    pub value: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeysResponse {
    pub keys: Vec<String>,
    pub count: usize,
    /// limit 때문에 잘렸는지
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaveResponse {
    pub saved_keys: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
struct KeysQuery {
    pattern: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct PatternQuery {
    pattern: Option<String>,
}

// ───────────────────────────── 에러 ─────────────────────────────

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    fn key_not_found(key: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("key '{key}' does not exist or has expired"),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: self.code.to_string(),
            message: self.message,
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<EngineError> for ApiError {
    fn from(err: EngineError) -> Self {
        let status = match &err {
            EngineError::Closed => StatusCode::SERVICE_UNAVAILABLE,
            EngineError::PersistenceDisabled => StatusCode::BAD_REQUEST,
            EngineError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = match &err {
            EngineError::Closed => "unavailable",
            EngineError::PersistenceDisabled => "persistence_disabled",
            EngineError::Io(_) => "io_error",
        };
        Self::new(status, code, err.to_string())
    }
}

impl From<PathRejection> for ApiError {
    fn from(err: PathRejection) -> Self {
        Self::bad_request(err.body_text())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(err: QueryRejection) -> Self {
        Self::bad_request(err.body_text())
    }
}

impl From<BytesRejection> for ApiError {
    fn from(err: BytesRejection) -> Self {
        let code = if err.status() == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "bad_request"
        };
        Self::new(err.status(), code, err.body_text())
    }
}

type ApiResult<T> = Result<T, ApiError>;

// ───────────────────────────── 검증 헬퍼 ─────────────────────────────

fn validate_key(key: &str) -> ApiResult<()> {
    if key.is_empty() {
        return Err(ApiError::bad_request("key must not be empty"));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(ApiError::bad_request(format!(
            "key is too long ({} bytes, max {MAX_KEY_BYTES})",
            key.len()
        )));
    }
    Ok(())
}

fn key_from(path: Result<Path<String>, PathRejection>) -> ApiResult<String> {
    let Path(key) = path?;
    validate_key(&key)?;
    Ok(key)
}

fn is_blank(body: &[u8]) -> bool {
    body.iter().all(u8::is_ascii_whitespace)
}

fn parse_json<T: DeserializeOwned>(body: &[u8]) -> ApiResult<T> {
    if is_blank(body) {
        return Err(ApiError::bad_request("request body (JSON) is required"));
    }
    serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("invalid JSON body: {e}")))
}

fn parse_optional_json<T: DeserializeOwned + Default>(body: &[u8]) -> ApiResult<T> {
    if is_blank(body) {
        Ok(T::default())
    } else {
        parse_json(body)
    }
}

/// ex(초) / px(밀리초) → 밀리초. 둘 다 있거나 0 이거나 너무 크면 에러.
fn resolve_ttl(ex: Option<u64>, px: Option<u64>) -> ApiResult<Option<u64>> {
    let ttl = match (ex, px) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "use only one of 'ex' (seconds) or 'px' (milliseconds)",
            ))
        }
        (Some(secs), None) => Some(secs.checked_mul(1000).unwrap_or(u64::MAX)),
        (None, Some(ms)) => Some(ms),
        (None, None) => None,
    };
    match ttl {
        Some(0) => Err(ApiError::bad_request("expire time must be greater than 0")),
        Some(ms) if ms > MAX_TTL_MS => Err(ApiError::bad_request(format!(
            "expire time is too large (max {MAX_TTL_MS} ms)"
        ))),
        other => Ok(other),
    }
}

// ───────────────────────────── 라우터 ─────────────────────────────

pub fn router(engine: Engine) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/stats", get(stats))
        .route("/keys", get(list_keys).delete(delete_by_pattern))
        .route("/keys/{key}", get(get_key).put(set_key).delete(delete_key))
        .route("/keys/{key}/ttl", get(get_ttl))
        .route("/keys/{key}/expire", put(set_expire).delete(persist_key))
        .route("/keys/{key}/incr", post(incr_key))
        .route("/admin/flush", post(flush_all))
        .route("/admin/save", post(save_snapshot))
        .fallback(route_not_found)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(engine)
}

/// 리스너에 API 서버를 띄우고 `shutdown` 이 완료되면 정상 종료합니다.
pub async fn serve<F>(listener: tokio::net::TcpListener, engine: Engine, shutdown: F) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, router(engine))
        .with_graceful_shutdown(shutdown)
        .await
}

// ───────────────────────────── 핸들러 ─────────────────────────────

async fn health(State(engine): State<Engine>) -> ApiResult<Json<serde_json::Value>> {
    engine.ping().await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn stats(State(engine): State<Engine>) -> ApiResult<Json<EngineStats>> {
    Ok(Json(engine.stats().await?))
}

async fn get_key(
    State(engine): State<Engine>,
    path: Result<Path<String>, PathRejection>,
) -> ApiResult<Json<ValueResponse>> {
    let key = key_from(path)?;
    match engine.get(key.clone()).await? {
        Some(info) => Ok(Json(ValueResponse {
            key,
            value: info.value,
            ttl_ms: info.ttl_ms,
        })),
        None => Err(ApiError::key_not_found(&key)),
    }
}

async fn set_key(
    State(engine): State<Engine>,
    path: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Json<SetResponse>> {
    let key = key_from(path)?;
    let request: SetRequest = parse_json(&body?)?;
    if request.value.len() > MAX_VALUE_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            format!(
                "value is too large ({} bytes, max {MAX_VALUE_BYTES})",
                request.value.len()
            ),
        ));
    }
    let ttl_ms = resolve_ttl(request.ex, request.px)?;
    let condition = match (request.nx, request.xx) {
        (true, true) => return Err(ApiError::bad_request("'nx' and 'xx' are mutually exclusive")),
        (true, false) => SetCondition::IfNotExists,
        (false, true) => SetCondition::IfExists,
        (false, false) => SetCondition::Always,
    };
    if engine.set(key.clone(), request.value, ttl_ms, condition).await? {
        Ok(Json(SetResponse { key, ttl_ms }))
    } else {
        let reason = if condition == SetCondition::IfNotExists {
            "key already exists (nx)"
        } else {
            "key does not exist (xx)"
        };
        Err(ApiError::new(StatusCode::CONFLICT, "condition_not_met", reason))
    }
}

async fn delete_key(
    State(engine): State<Engine>,
    path: Result<Path<String>, PathRejection>,
) -> ApiResult<Json<DeleteResponse>> {
    let key = key_from(path)?;
    if engine.del(key.clone()).await? {
        Ok(Json(DeleteResponse { deleted: 1 }))
    } else {
        Err(ApiError::key_not_found(&key))
    }
}

async fn get_ttl(
    State(engine): State<Engine>,
    path: Result<Path<String>, PathRejection>,
) -> ApiResult<Json<TtlResponse>> {
    let key = key_from(path)?;
    match engine.ttl(key.clone()).await? {
        Ttl::Missing => Err(ApiError::key_not_found(&key)),
        Ttl::Persistent => Ok(Json(TtlResponse { key, ttl_ms: None })),
        Ttl::ExpiresIn(ms) => Ok(Json(TtlResponse { key, ttl_ms: Some(ms) })),
    }
}

async fn set_expire(
    State(engine): State<Engine>,
    path: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Json<TtlResponse>> {
    let key = key_from(path)?;
    let request: ExpireRequest = parse_json(&body?)?;
    let ttl_ms = resolve_ttl(request.ex, request.px)?
        .ok_or_else(|| ApiError::bad_request("either 'ex' or 'px' is required"))?;
    if engine.expire(key.clone(), ttl_ms).await? {
        Ok(Json(TtlResponse {
            key,
            ttl_ms: Some(ttl_ms),
        }))
    } else {
        Err(ApiError::key_not_found(&key))
    }
}

async fn persist_key(
    State(engine): State<Engine>,
    path: Result<Path<String>, PathRejection>,
) -> ApiResult<Json<PersistResponse>> {
    let key = key_from(path)?;
    match engine.persist(key.clone()).await? {
        Some(persisted) => Ok(Json(PersistResponse { key, persisted })),
        None => Err(ApiError::key_not_found(&key)),
    }
}

async fn incr_key(
    State(engine): State<Engine>,
    path: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Json<IncrResponse>> {
    let key = key_from(path)?;
    let request: IncrRequest = parse_optional_json(&body?)?;
    let by = request.by.unwrap_or(1);
    match engine.incr(key.clone(), by).await? {
        Ok(value) => Ok(Json(IncrResponse { key, value })),
        Err(err @ StoreError::NotAnInteger) => {
            Err(ApiError::new(StatusCode::CONFLICT, "not_an_integer", err.to_string()))
        }
        Err(err @ StoreError::Overflow) => {
            Err(ApiError::new(StatusCode::CONFLICT, "overflow", err.to_string()))
        }
    }
}

async fn list_keys(
    State(engine): State<Engine>,
    query: Result<Query<KeysQuery>, QueryRejection>,
) -> ApiResult<Json<KeysResponse>> {
    let Query(query) = query?;
    let pattern = query.pattern.unwrap_or_else(|| "*".to_string());
    let limit = query.limit.unwrap_or(DEFAULT_KEYS_LIMIT).clamp(1, MAX_KEYS_LIMIT);
    let mut keys = engine.keys(pattern).await?;
    let truncated = keys.len() > limit;
    keys.truncate(limit);
    Ok(Json(KeysResponse {
        count: keys.len(),
        keys,
        truncated,
    }))
}

async fn delete_by_pattern(
    State(engine): State<Engine>,
    query: Result<Query<PatternQuery>, QueryRejection>,
) -> ApiResult<Json<DeleteResponse>> {
    let Query(query) = query?;
    // 실수로 전체 삭제하지 않도록 pattern 을 필수로 요구 (전체 삭제는 /admin/flush)
    let pattern = query
        .pattern
        .filter(|p| !p.is_empty())
        .ok_or_else(|| ApiError::bad_request("query parameter 'pattern' is required"))?;
    let deleted = engine.delete_matching(pattern).await?;
    Ok(Json(DeleteResponse {
        deleted: deleted as u64,
    }))
}

async fn flush_all(State(engine): State<Engine>) -> ApiResult<Json<DeleteResponse>> {
    let deleted = engine.flush().await?;
    Ok(Json(DeleteResponse {
        deleted: deleted as u64,
    }))
}

async fn save_snapshot(State(engine): State<Engine>) -> ApiResult<Json<SaveResponse>> {
    let saved_keys = engine.save().await?;
    Ok(Json(SaveResponse { saved_keys }))
}

async fn route_not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "route_not_found", "no such endpoint")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_resolution() {
        assert_eq!(resolve_ttl(None, None).unwrap(), None);
        assert_eq!(resolve_ttl(Some(2), None).unwrap(), Some(2_000));
        assert_eq!(resolve_ttl(None, Some(150)).unwrap(), Some(150));
        assert!(resolve_ttl(Some(1), Some(1)).is_err());
        assert!(resolve_ttl(Some(0), None).is_err());
        assert!(resolve_ttl(None, Some(0)).is_err());
        assert!(resolve_ttl(Some(u64::MAX), None).is_err(), "overflow 방지");
        assert!(resolve_ttl(None, Some(MAX_TTL_MS + 1)).is_err());
    }

    #[test]
    fn key_validation() {
        assert!(validate_key("").is_err());
        assert!(validate_key(&"k".repeat(MAX_KEY_BYTES + 1)).is_err());
        assert!(validate_key(&"k".repeat(MAX_KEY_BYTES)).is_ok());
        assert!(validate_key("user:1/프로필").is_ok());
    }
}
