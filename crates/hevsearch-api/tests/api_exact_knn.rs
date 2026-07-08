//! RFC 0014 integration tests: explicit exact KNN query mode.
//!
//! Gated `#[ignore]`: needs MinIO up.
//!
//! ```text
//! docker compose up -d minio minio-init
//! ./scripts/cargo test -p hevsearch-api --test api_exact_knn -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use hevsearch_api::router;
use hevsearch_core::CoreMetrics;
use serde_json::{json, Value};
use tower::ServiceExt;

mod common;
use common::{test_state, unique_namespace};

async fn build_app_with_metrics() -> (axum::Router, tempfile::TempDir, Arc<CoreMetrics>) {
    let (state, tmp) = test_state().await;
    let metrics = Arc::clone(&state.metrics);
    (router(state), tmp, metrics)
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

async fn post_empty(app: axum::Router, uri: String) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    app.oneshot(request).await.unwrap().status()
}

fn metric_value(body: &str, metric: &str, label_needle: &str) -> Option<f64> {
    for line in body.lines() {
        if line.starts_with('#') || !line.starts_with(metric) {
            continue;
        }
        if !label_needle.is_empty() && !line.contains(label_needle) {
            continue;
        }
        let value = line.rsplit_once(char::is_whitespace)?.1;
        return value.parse().ok();
    }
    None
}

async fn wait_for_index(
    metrics: &Arc<CoreMetrics>,
    ns: &str,
    kind: &str,
    expected: u64,
    deadline: Duration,
) -> u64 {
    let start = std::time::Instant::now();
    let label = format!(r#"kind="{kind}",namespace="{ns}""#);
    loop {
        let body = metrics.encode().unwrap();
        let count = metric_value(
            &body,
            "hevsearch_index_build_duration_seconds_count",
            &label,
        )
        .map(|v| v as u64)
        .unwrap_or(0);
        if count >= expected || start.elapsed() >= deadline {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn make_vector(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| {
            let x = (seed as f32 + 1.0) * 0.131 + (j as f32 + 3.0) * 0.017;
            (x.sin() * 0.7) + ((seed * 31 + j * 17) as f32 * 0.011).cos() * 0.3
        })
        .collect()
}

fn ids(body: &Value) -> Vec<u64> {
    body["results"]
        .as_array()
        .expect("results array")
        .iter()
        .map(|r| r["id"].as_u64().expect("u64 id"))
        .collect()
}

fn recall(indexed: &[u64], exact: &[u64]) -> f64 {
    let hits = indexed.iter().filter(|id| exact.contains(id)).count();
    hits as f64 / exact.len() as f64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn exact_query_is_ground_truth_against_pq_indexed_namespace() {
    let (app, _tmp, metrics) = build_app_with_metrics().await;
    let ns = unique_namespace("exact-knn-recall");
    let dim = 32;
    let rows: Vec<Value> = (0..512)
        .map(|i| json!({"id": i, "vector": make_vector(i, dim), "attributes": {"bucket": i % 4}}))
        .collect();
    let (status, body) = post_json(
        app.clone(),
        format!("/ns/{ns}/upsert"),
        json!({ "rows": rows }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "upsert must succeed: {body}");

    let (status, body) = post_json(
        app.clone(),
        format!("/ns/{ns}/index"),
        json!({
            "kind": "ivf_pq",
            "num_partitions": 16,
            "num_sub_vectors": 4,
            "num_bits": 4
        }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "index response: {body}");
    let count = wait_for_index(&metrics, &ns, "ivf_pq", 1, Duration::from_secs(90)).await;
    assert_eq!(count, 1, "IVF_PQ index build must complete");

    println!("query_id,indexed_recall@20,exact_top,indexed_top");
    let mut saw_indexed_miss = false;
    for qid in [3usize, 19, 41, 73, 127, 211, 307, 401] {
        let query = make_vector(qid + 10_000, dim);
        let exact_body = json!({
            "vector": query,
            "k": 20,
            "exact": true,
            "include_vector": false
        });
        let indexed_body = json!({
            "vector": query,
            "k": 20,
            "nprobes": 1,
            "include_vector": false
        });
        let (status, exact_json) =
            post_json(app.clone(), format!("/ns/{ns}/query"), exact_body).await;
        assert_eq!(status, StatusCode::OK, "exact query status");
        let (status, indexed_json) =
            post_json(app.clone(), format!("/ns/{ns}/query"), indexed_body).await;
        assert_eq!(status, StatusCode::OK, "indexed query status");
        let exact_ids = ids(&exact_json);
        let indexed_ids = ids(&indexed_json);
        let r = recall(&indexed_ids, &exact_ids);
        println!("{qid},{r:.3},{:?},{:?}", &exact_ids[..3], &indexed_ids[..3]);
        if r < 1.0 {
            saw_indexed_miss = true;
        }
    }
    assert!(
        saw_indexed_miss,
        "expected at least one PQ-indexed nprobes=1 query to miss the exact top-20"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn exact_query_uses_same_selective_filter_path() {
    let (app, _tmp, _metrics) = build_app_with_metrics().await;
    let ns = unique_namespace("exact-knn-filter");
    let rows = json!({
        "rows": [
            {"id": 1, "vector": [1.0, 0.0, 0.0, 0.0], "attributes": {"tier": "drop"}},
            {"id": 2, "vector": [0.9, 0.1, 0.0, 0.0], "attributes": {"tier": "keep"}},
            {"id": 3, "vector": [0.8, 0.2, 0.0, 0.0], "attributes": {"tier": "keep"}},
            {"id": 4, "vector": [0.0, 1.0, 0.0, 0.0], "attributes": {"tier": "keep"}},
            {"id": 5, "vector": [0.0, 0.0, 1.0, 0.0], "attributes": {"tier": "drop"}}
        ]
    });
    let (status, body) = post_json(app.clone(), format!("/ns/{ns}/upsert"), rows).await;
    assert_eq!(status, StatusCode::OK, "upsert must succeed: {body}");

    let (status, body) = post_json(
        app,
        format!("/ns/{ns}/query"),
        json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "k": 3,
            "exact": true,
            "filter": "tier = 'keep'",
            "include_vector": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "filtered exact status: {body}");
    assert_eq!(ids(&body), vec![2, 3, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn exact_rejects_conflicting_query_shapes() {
    let (app, _tmp, _metrics) = build_app_with_metrics().await;
    let ns = unique_namespace("exact-knn-validation");
    let (status, body) = post_json(
        app.clone(),
        format!("/ns/{ns}/upsert"),
        json!({"rows": [{"id": 1, "vector": [1.0, 0.0], "text": "alpha"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "upsert must succeed: {body}");

    let (status, body) = post_json(
        app.clone(),
        format!("/ns/{ns}/query"),
        json!({"vector": [1.0, 0.0], "k": 1, "exact": true, "nprobes": 4}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("exact") && err.contains("nprobes"),
        "unexpected exact+nprobes error: {body}"
    );

    let (status, body) = post_json(
        app,
        format!("/ns/{ns}/query"),
        json!({"text": "alpha", "k": 1, "exact": true}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("FTS-only") || err.contains("vector index"),
        "unexpected FTS-only exact error: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn hybrid_query_with_exact_vector_leg_composes_with_fts() {
    let (app, _tmp, metrics) = build_app_with_metrics().await;
    let ns = unique_namespace("exact-knn-hybrid");
    let rows = json!({
        "rows": [
            {"id": 1, "vector": [1.0, 0.0, 0.0, 0.0], "text": "kubernetes routing guide"},
            {"id": 2, "vector": [0.0, 1.0, 0.0, 0.0], "text": "kubernetes storage guide"},
            {"id": 3, "vector": [0.0, 0.0, 1.0, 0.0], "text": "garden compost notes"}
        ]
    });
    let (status, body) = post_json(app.clone(), format!("/ns/{ns}/upsert"), rows).await;
    assert_eq!(status, StatusCode::OK, "upsert must succeed: {body}");
    let status = post_empty(app.clone(), format!("/ns/{ns}/fts-index")).await;
    assert_eq!(status, StatusCode::ACCEPTED, "fts-index must start");
    let count = wait_for_index(&metrics, &ns, "fts", 1, Duration::from_secs(30)).await;
    assert_eq!(count, 1, "FTS index build must complete");

    let (status, body) = post_json(
        app,
        format!("/ns/{ns}/query"),
        json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "text": "kubernets",
            "fuzzy": {"max_edit_distance": "auto"},
            "k": 3,
            "exact": true,
            "include_vector": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "hybrid exact status: {body}");
    let out = ids(&body);
    assert_eq!(
        out[0], 1,
        "nearest vector plus fuzzy FTS hit should rank first"
    );
    assert!(
        out.contains(&2),
        "FTS leg should still contribute id=2: {out:?}"
    );
}
