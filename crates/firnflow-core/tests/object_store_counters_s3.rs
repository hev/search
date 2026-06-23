//! Integration test: always-on object-store read counters work with the byte
//! cache **disabled**.
//!
//! Proves the cache-off observability path: the instrumented Lance session
//! installs a counting object store below the (absent) byte cache, so a real
//! query through lancedb increments `firnflow_object_store_requests_total` and
//! `firnflow_object_store_get_bytes_total` even when the object cache is off.
//! This is the gap that previously made cache-off S3 read cost invisible — the
//! object-cache counters only move when the cache is enabled.
//!
//! Gated `#[ignore]`: needs MinIO up.
//!
//! ```text
//! docker compose up -d minio minio-init
//! ./scripts/cargo test -p firnflow-core --test object_store_counters_s3 \
//!     -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use firnflow_core::metrics::test_metrics;
use firnflow_core::object_cache::build_instrumented_session;
use firnflow_core::{NamespaceId, NamespaceManager, StorageRoot, UpsertRow};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn unique_namespace(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn minio_options() -> HashMap<String, String> {
    HashMap::from([
        (
            "aws_access_key_id".into(),
            env_or("FIRNFLOW_S3_ACCESS_KEY", "minioadmin"),
        ),
        (
            "aws_secret_access_key".into(),
            env_or("FIRNFLOW_S3_SECRET_KEY", "minioadmin"),
        ),
        (
            "aws_endpoint".into(),
            env_or("FIRNFLOW_S3_ENDPOINT", "http://127.0.0.1:9000"),
        ),
        ("aws_region".into(), "us-east-1".into()),
        ("allow_http".into(), "true".into()),
        ("aws_virtual_hosted_style_request".into(), "false".into()),
    ])
}

fn seed_rows(dim: usize) -> Vec<UpsertRow> {
    let mut a = vec![0.0_f32; dim];
    a[0] = 1.0;
    let mut b = vec![0.0_f32; dim];
    b[1 % dim] = 1.0;
    vec![UpsertRow::from((1u64, a)), UpsertRow::from((2u64, b))]
}

#[tokio::test]
#[ignore]
async fn object_store_counters_increment_with_cache_disabled() {
    let bucket = env_or("FIRNFLOW_S3_BUCKET", "firnflow-test");
    let metrics = test_metrics();

    // Byte cache DISABLED (None), but the instrumented session is still installed,
    // so backend reads are counted regardless of cache state.
    let session = build_instrumented_session(metrics.object_store_counters(), None);
    let manager = NamespaceManager::new(
        StorageRoot::s3_bucket(&bucket).unwrap(),
        minio_options(),
        Arc::clone(&metrics),
    )
    .with_session(session);
    let ns = NamespaceId::new(unique_namespace("objstore-counters")).unwrap();

    let (get0, _head0, bytes0) = metrics.object_store_counters().snapshot();

    manager.upsert(&ns, seed_rows(8)).await.expect("upsert");
    let query = vec![1.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let _ = manager
        .query(&ns, query, None, 2, None, None, true)
        .await
        .expect("query");

    let (get1, _head1, bytes1) = metrics.object_store_counters().snapshot();
    assert!(
        get1 > get0,
        "GET requests must increase through the cache-off path: {get0} -> {get1}"
    );
    assert!(
        bytes1 > bytes0,
        "GET bytes must increase through the cache-off path: {bytes0} -> {bytes1}"
    );

    // Tidy up the test namespace.
    let _ = manager.delete(&ns).await;
}
