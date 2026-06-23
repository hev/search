//! API error type with axum `IntoResponse` integration.
//!
//! Promoted from a newtype-over-`FirnflowError` to an enum in 0.5.0.
//! API-layer-only conditions — bearer-token rejection, scope
//! mismatch, rate-limit shedding — are real variants now rather than
//! synthetic `FirnflowError`s squeezed through a wrapper.
//! `From<FirnflowError>` is preserved so handlers continue to use
//! `?` on core calls.

use std::time::Duration;

use axum::http::header;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use firnflow_core::FirnflowError;

/// API error variants. `Core` wraps a `firnflow-core` error and
/// inherits its existing 4xx/5xx mapping. `Unauthorized`,
/// `Forbidden`, `NotFound`, and `RateLimited` are API-layer-only.
pub enum ApiError {
    Core(FirnflowError),
    Unauthorized,
    Forbidden,
    /// The addressed resource does not exist (e.g. metadata for a
    /// namespace that has never been written). Renders as 404.
    NotFound(String),
    RateLimited(Duration),
}

impl From<FirnflowError> for ApiError {
    fn from(e: FirnflowError) -> Self {
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
                    HeaderValue::from_static("Bearer realm=\"firnflow\""),
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

fn core_response(err: FirnflowError) -> Response {
    let (status, msg) = match err {
        FirnflowError::InvalidNamespace(name) => (
            StatusCode::BAD_REQUEST,
            format!("invalid namespace: {name}"),
        ),
        FirnflowError::InvalidRequest(msg) => {
            (StatusCode::BAD_REQUEST, format!("invalid request: {msg}"))
        }
        FirnflowError::Unsupported(msg) => {
            (StatusCode::NOT_IMPLEMENTED, format!("not supported: {msg}"))
        }
        FirnflowError::StorageRegionRedirect { configured_region } => {
            // The redirect is detected in firnflow-core, where the configured
            // region is known; we surface only that typed field, never the raw
            // backend error text (which still goes through the scrubbed
            // catch-all below). Naming the misconfigured region is the whole
            // point of the typed variant — it is the one piece of diagnostic
            // information an operator needs to fix this.
            tracing::error!(?configured_region, "object storage region redirect");
            let region = configured_region
                .as_deref()
                .map(|r| format!(" The configured region is `{r}`."))
                .unwrap_or_default();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "object storage redirected the request, which usually means the \
                     configured region does not match the bucket's region.{region} Set \
                     FIRNFLOW_S3_REGION (or AWS_REGION) to the bucket's region."
                ),
            )
        }
        err @ (FirnflowError::Backend(_)
        | FirnflowError::Cache(_)
        | FirnflowError::Io(_)
        | FirnflowError::Metrics(_)) => {
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
mod tests {
    use super::*;
    use axum::body::to_bytes;

    async fn body_string(resp: Response) -> String {
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn region_redirect_renders_safe_hint_naming_the_region() {
        let resp = ApiError::Core(FirnflowError::StorageRegionRedirect {
            configured_region: Some("us-east-1".into()),
        })
        .into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = body_string(resp).await;
        assert!(
            body.contains("us-east-1"),
            "should name the configured region: {body}"
        );
        assert!(
            body.contains("FIRNFLOW_S3_REGION"),
            "should point at the knob: {body}"
        );
    }

    #[tokio::test]
    async fn backend_error_text_never_leaks_even_if_redirect_shaped() {
        // A raw Backend error — even one whose text looks like a redirect — must
        // render as the scrubbed generic 500, never echoing the backend string.
        // (Region detection happens in firnflow-core, which is where the typed
        // variant is produced; anything still arriving as Backend is opaque.)
        let resp = ApiError::Core(FirnflowError::Backend(
            "redirect without LOCATION secret-bucket-internal".into(),
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = body_string(resp).await;
        assert_eq!(body, r#"{"error":"internal server error"}"#);
        assert!(!body.contains("secret-bucket-internal"));
    }
}
