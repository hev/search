//! `GET /ns` namespace-enumeration integration tests over the HTTP
//! router.
//!
//! Gated `#[ignore]`: needs MinIO up. The MinIO bucket is shared
//! across concurrently-running tests, so assertions are containment
//! checks against namespaces this test created, never equality on the
//! full listing.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use hevsearch_api::router;
use serde_json::{json, Value};
use tower::ServiceExt;

mod common;
use common::{test_state, unique_namespace};

async fn build_app() -> (axum::Router, tempfile::TempDir) {
    let (state, tmp) = test_state().await;
    (router(state), tmp)
}

async fn send(app: axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

async fn upsert_one(app: axum::Router, ns: &str) {
    let body = json!({
        "rows": [{"id": 1, "vector": [1.0, 0.0, 0.0, 0.0], "text": "hello"}]
    });
    let request = Request::builder()
        .method("POST")
        .uri(format!("/ns/{ns}/upsert"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let (status, _) = send(app, request).await;
    assert_eq!(status, StatusCode::OK, "upsert must succeed");
}

async fn list_namespaces(app: axum::Router) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri("/ns")
        .body(Body::empty())
        .unwrap();
    send(app, request).await
}

#[tokio::test]
#[ignore]
async fn listing_includes_written_namespaces_and_sorts() {
    let (app, _tmp) = build_app().await;
    let ns_b = unique_namespace("ns-enum-b");
    let ns_a = unique_namespace("ns-enum-a");
    upsert_one(app.clone(), &ns_b).await;
    upsert_one(app.clone(), &ns_a).await;

    let (status, body) = list_namespaces(app).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = body["namespaces"]
        .as_array()
        .expect("namespaces array")
        .iter()
        .map(|v| v.as_str().expect("namespace name is a string"))
        .collect();
    assert!(
        names.contains(&ns_a.as_str()),
        "missing {ns_a} in {names:?}"
    );
    assert!(
        names.contains(&ns_b.as_str()),
        "missing {ns_b} in {names:?}"
    );

    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "listing must be sorted ascending");
}

#[tokio::test]
#[ignore]
async fn deleted_namespace_disappears_from_listing() {
    let (app, _tmp) = build_app().await;
    let ns = unique_namespace("ns-enum-del");
    upsert_one(app.clone(), &ns).await;

    let (status, body) = list_namespaces(app.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == ns.as_str()),
        "namespace must be listed after upsert"
    );

    let request = Request::builder()
        .method("DELETE")
        .uri(format!("/ns/{ns}"))
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(app.clone(), request).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = list_namespaces(app).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !body["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == ns.as_str()),
        "namespace must not be listed after delete"
    );
}
