//! API error type with axum `IntoResponse` integration.
//!
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use hevsearch_core::HevSearchError;

pub enum ApiError {
    Core(HevSearchError),
    /// The addressed resource does not exist (e.g. metadata for a
    /// namespace that has never been written). Renders as 404.
    NotFound(String),
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
            ApiError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, Json(ErrorBody { error: msg })).into_response()
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
