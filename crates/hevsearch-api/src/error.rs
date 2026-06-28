//! API error type with axum `IntoResponse` integration.
//!
//! Promoted from a newtype-over-`HevSearchError` to an enum in 0.5.0.
//! API-layer-only conditions — bearer-token rejection, scope
//! mismatch, rate-limit shedding — are real variants now rather than
//! synthetic `HevSearchError`s squeezed through a wrapper.
//! `From<HevSearchError>` is preserved so handlers continue to use
//! `?` on core calls.

use std::time::Duration;

use axum::http::header;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use hevsearch_core::HevSearchError;

/// API error variants. `Core` wraps a `hevsearch-core` error and
/// inherits its existing 4xx/5xx mapping. `Unauthorized`,
/// `Forbidden`, `NotFound`, and `RateLimited` are API-layer-only.
pub enum ApiError {
    Core(HevSearchError),
    Unauthorized,
    Forbidden,
    /// The addressed resource does not exist (e.g. metadata for a
    /// namespace that has never been written). Renders as 404.
    NotFound(String),
    RateLimited(Duration),
}

impl From<HevSearchError> for ApiError {
    fn from(e: HevSearchError) -> Self {
        Self::Core(e)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Core(err) => core_response(err),
            ApiError::Unauthorized => {
                let mut response = (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorBody {
                        error: "unauthorized".into(),
                    }),
                )
                    .into_response();
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer realm=\"hevsearch\""),
                );
                response
            }
            ApiError::Forbidden => (
                StatusCode::FORBIDDEN,
                Json(ErrorBody {
                    error: "forbidden".into(),
                }),
            )
                .into_response(),
            ApiError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, Json(ErrorBody { error: msg })).into_response()
            }
            ApiError::RateLimited(wait) => {
                let secs = wait.as_secs().max(1);
                let mut response = (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(ErrorBody {
                        error: "rate limited".into(),
                    }),
                )
                    .into_response();
                if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                    response.headers_mut().insert(header::RETRY_AFTER, v);
                }
                response
            }
        }
    }
}

fn core_response(err: HevSearchError) -> Response {
    let (status, msg) = match err {
        HevSearchError::InvalidNamespace(name) => (
            StatusCode::BAD_REQUEST,
            format!("invalid namespace: {name}"),
        ),
        HevSearchError::InvalidRequest(msg) => {
            (StatusCode::BAD_REQUEST, format!("invalid request: {msg}"))
        }
        HevSearchError::Unsupported(msg) => {
            (StatusCode::NOT_IMPLEMENTED, format!("not supported: {msg}"))
        }
        err @ (HevSearchError::Backend(_)
        | HevSearchError::Cache(_)
        | HevSearchError::Io(_)
        | HevSearchError::Metrics(_)) => {
            tracing::error!(error = %err, "internal error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            )
        }
    };
    (status, Json(ErrorBody { error: msg })).into_response()
}
