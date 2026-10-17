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
        HevSearchError::MutationConflict(message) => {
            tracing::warn!(error = %message, "native mutation conflict");
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "mutation_conflict", "outcome": "rejected"})),
            )
                .into_response();
        }
        HevSearchError::MutationUnknown(message) => {
            tracing::warn!(error = %message, "native mutation acceptance unknown");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    serde_json::json!({"error": "mutation_outcome_unknown", "outcome": "unknown"}),
                ),
            )
                .into_response();
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

#[cfg(test)]
mod mutation_tests {
    use super::*;
    #[tokio::test]
    async fn conflict_and_unknown_responses_do_not_leak_storage_details() {
        for (error, status, outcome) in [
            (
                HevSearchError::MutationConflict("private storage path".into()),
                StatusCode::CONFLICT,
                "rejected",
            ),
            (
                HevSearchError::MutationUnknown("private storage path".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "unknown",
            ),
        ] {
            let response = core_response(error);
            assert_eq!(response.status(), status);
            let bytes = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["outcome"], outcome);
            assert!(!String::from_utf8(bytes.to_vec())
                .unwrap()
                .contains("private"));
        }
    }
}
