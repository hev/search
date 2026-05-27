//! Integration test for the configurable request body limit.
//!
//! Two cases against the real axum router:
//! - A payload below the configured limit goes through the JSON
//!   extractor and reaches the handler (which here is `warmup` with
//!   an empty query list, so no backend access is needed).
//! - A payload above the configured limit is rejected at the body
//!   extractor with 413, before any handler runs.
//!
//! The `padding` field is an unknown key on `WarmupRequest`. Serde
//! ignores unknown fields by default, so the below-limit case still
//! deserialises cleanly into `{ queries: [] }` and returns 202
//! without touching the namespace manager or any object store.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use firnflow_api::router;
use serde_json::json;
use tower::ServiceExt;

mod common;
use common::{test_state_offline, unique_namespace};

const TEST_LIMIT_BYTES: usize = 65_536;

async fn post_warmup_with_padding(app: axum::Router, ns: &str, padding_bytes: usize) -> StatusCode {
    let padding = "x".repeat(padding_bytes);
    let body = json!({ "queries": [], "padding": padding });
    let request = Request::builder()
        .method("POST")
        .uri(format!("/ns/{ns}/warmup"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    app.oneshot(request).await.unwrap().status()
}

#[tokio::test]
async fn payload_below_limit_is_accepted() {
    let (mut state, _tmp) = test_state_offline().await;
    state.max_body_bytes = TEST_LIMIT_BYTES;
    let app = router(state);
    let ns = unique_namespace("body-ok");

    // 50 KB of padding plus the surrounding JSON sits comfortably
    // under the 64 KB limit.
    let status = post_warmup_with_padding(app, &ns, 50_000).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "below-limit payload should reach the warmup handler"
    );
}

#[tokio::test]
async fn payload_above_limit_returns_413() {
    let (mut state, _tmp) = test_state_offline().await;
    state.max_body_bytes = TEST_LIMIT_BYTES;
    let app = router(state);
    let ns = unique_namespace("body-too-big");

    // 100 KB of padding crosses the 64 KB limit before the handler
    // is reached.
    let status = post_warmup_with_padding(app, &ns, 100_000).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "above-limit payload should be rejected by the body extractor"
    );
}

#[tokio::test]
async fn body_limit_is_router_wide() {
    // The limit must apply to every protected route, not just the
    // one that triggered the original bug report. Hit upsert with
    // an oversized payload and assert the same 413 fires before the
    // namespace manager (which would otherwise try to reach the
    // unreachable offline backend) gets a look at it.
    let (mut state, _tmp) = test_state_offline().await;
    state.max_body_bytes = TEST_LIMIT_BYTES;
    let app = router(state);
    let ns = unique_namespace("body-upsert");

    let padding = "x".repeat(100_000);
    let body = json!({ "rows": [], "padding": padding });
    let request = Request::builder()
        .method("POST")
        .uri(format!("/ns/{ns}/upsert"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    // Drain the body so the test does not leak the response stream.
    let _ = to_bytes(response.into_body(), usize::MAX).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "body limit must cover all protected routes, not just warmup"
    );
}
