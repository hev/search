//! API coverage for arbitrary string row ids (RFC 0005).
//!
//! Gated `#[ignore]`: needs MinIO up.

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

async fn post_json(app: axum::Router, uri: String, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
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

async fn get(app: axum::Router, uri: String) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
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

#[tokio::test]
#[ignore]
async fn string_ids_roundtrip_query_list_facet_info_and_wrong_type() {
    let (app, _tmp) = build_app().await;
    let ns = unique_namespace("api-string-ids");

    let upsert_body = json!({
        "rows": [
            {"id": "asin-B08N5WRWNW", "vector": [1.0, 0.0, 0.0, 0.0], "text": "coffee grinder", "attributes": {"family": "demo", "kind": "product"}},
            {"id": "ticket-4117", "vector": [0.0, 1.0, 0.0, 0.0], "text": "billing ticket", "attributes": {"family": "demo", "kind": "support"}},
            {"id": "openfda-set's-42", "vector": [0.0, 0.0, 1.0, 0.0], "text": "drug label", "attributes": {"family": "reference", "kind": "label"}}
        ]
    });
    let (status, body) = post_json(app.clone(), format!("/ns/{ns}/upsert"), upsert_body).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, info) = get(app.clone(), format!("/ns/{ns}")).await;
    assert_eq!(status, StatusCode::OK, "{info}");
    assert_eq!(info["id_type"], "string");
    assert_eq!(info["row_count"], 3);
    assert_eq!(info["has_scalar_index"], true);

    let (status, query) = post_json(
        app.clone(),
        format!("/ns/{ns}/query"),
        json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "k": 3,
            "filter": "family = 'demo'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{query}");
    let query_ids: Vec<&str> = query["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(query_ids.contains(&"asin-B08N5WRWNW"), "{query_ids:?}");
    assert!(query_ids.contains(&"ticket-4117"), "{query_ids:?}");

    let (status, first_page) = get(app.clone(), format!("/ns/{ns}/list?limit=2")).await;
    assert_eq!(status, StatusCode::OK, "{first_page}");
    let first_rows = first_page["rows"].as_array().unwrap();
    assert_eq!(first_rows.len(), 2);
    assert!(first_rows.iter().all(|row| row["id"].is_string()));
    let cursor = first_page["next_cursor"].as_str().expect("next cursor");
    assert!(cursor.starts_with("v1:s:"), "string cursor: {cursor}");

    let (status, second_page) = get(
        app.clone(),
        format!("/ns/{ns}/list?limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second_page}");
    assert_eq!(second_page["rows"].as_array().unwrap().len(), 1);

    let (status, facet) = post_json(
        app.clone(),
        format!("/ns/{ns}/facet"),
        json!({"fields": ["family"], "filter": "kind IN ('product', 'support')"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{facet}");
    assert_eq!(facet["facets"][0]["buckets"][0]["value"], "demo");
    assert_eq!(facet["facets"][0]["buckets"][0]["count"], 2);

    let (status, body) = post_json(
        app,
        format!("/ns/{ns}/upsert"),
        json!({"rows": [{"id": 99, "vector": [0.0, 0.0, 0.0, 1.0]}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("namespace id_type is string"),
        "{body}"
    );
}
