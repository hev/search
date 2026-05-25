//! Warm-vs-cold reachability test for `FoyerCacheBackend` against
//! MinIO. Confirms the adapter is on Lance's hot path and that a
//! second identical query benefits from the warm cache.
//!
//! Shape:
//!
//! 1. Build a session carrying a `FoyerCacheBackend` and thread it
//!    through `NamespaceManager::with_session(...)`.
//! 2. Seed a 260-row corpus and build an IVF_PQ index.
//! 3. Run query 1 (cold). Snapshot counters before and after.
//! 4. Run query 2 (identical query, warm). Snapshot again.
//! 5. Assert query 2 issued more `get` calls than `insert` calls
//!    (cache hits, not cache misses), and that the `insert` delta
//!    on query 2 is strictly less than on query 1.
//!
//! The precursor to the prototype benchmark in
//! `ISSUE_51_PROTOTYPE.md`; not the benchmark itself.
//!
//! Gated `#[ignore]`: needs MinIO up.
//!
//! ```text
//! docker compose up -d minio minio-init
//! ./scripts/cargo test -p firnflow-core --test cachebackend_foyer_warm_cold \
//!     -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use firnflow_core::cache::NamespaceCache;
use firnflow_core::cachebackend::{
    CacheBackendCounters, FoyerCacheBackend, FoyerCacheBackendConfig,
};
use firnflow_core::metrics::test_metrics;
use firnflow_core::{NamespaceId, NamespaceManager, NamespaceService, StorageRoot, UpsertRow};
use lance_core::cache::CacheBackend;
use lancedb::{ObjectStoreRegistry, Session};

const DIM: usize = 8;

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

fn unit_vector(axis: usize) -> Vec<f32> {
    let mut v = vec![0.0_f32; DIM];
    v[axis] = 1.0;
    v
}

#[tokio::test]
#[ignore]
async fn foyer_cachebackend_warms_index_on_repeat_query() {
    // --- foyer-backed CacheBackend + session ---
    let counters = Arc::new(CacheBackendCounters::default());
    let nvme = tempfile::tempdir().expect("nvme tempdir");
    let backend = Arc::new(
        FoyerCacheBackend::new(
            FoyerCacheBackendConfig {
                memory_bytes: 16 * 1024 * 1024,
                nvme_path: nvme.path(),
                nvme_bytes: 64 * 1024 * 1024,
            },
            Arc::clone(&counters),
        )
        .await
        .expect("foyer cachebackend build"),
    );
    let backend_dyn: Arc<dyn CacheBackend> = backend.clone();

    let registry = Arc::new(ObjectStoreRegistry::default());
    let session = Arc::new(Session::with_index_cache_backend(
        backend_dyn,
        8 * 1024 * 1024,
        registry,
    ));

    // --- manager + service with the session attached ---
    let bucket = env_or("FIRNFLOW_S3_BUCKET", "firnflow-test");
    let result_cache_dir = tempfile::tempdir().expect("result cache tempdir");
    let metrics = test_metrics();
    let manager = Arc::new(
        NamespaceManager::new(
            StorageRoot::s3_bucket(&bucket).unwrap(),
            minio_options(),
            Arc::clone(&metrics),
        )
        .with_session(Arc::clone(&session)),
    );
    let cache = Arc::new(
        NamespaceCache::new(
            16 * 1024 * 1024,
            result_cache_dir.path(),
            64 * 1024 * 1024,
            Arc::clone(&metrics),
        )
        .await
        .expect("result cache build"),
    );
    let service = NamespaceService::new(
        Arc::clone(&manager),
        Arc::clone(&cache),
        Arc::clone(&metrics),
    );
    let ns = NamespaceId::new(unique_namespace("foyer-warm-cold")).unwrap();

    // --- seed corpus + index ---
    //
    // 260 rows is just above Lance's 256 PQ training floor. Each
    // row points along a single axis cycling through `0..DIM`.
    let rows: Vec<UpsertRow> = (0..260u64)
        .map(|i| UpsertRow::from((i, unit_vector((i as usize) % DIM))))
        .collect();
    service.upsert(&ns, rows).await.expect("seed upsert");
    manager
        .create_index(&ns, Some(4), Some(2))
        .await
        .expect("index build");

    // Bypass the service for queries and call the manager
    // directly, which always hits Lance (the service-level result
    // cache would otherwise satisfy query 2 without touching
    // Lance at all, falsifying the warm-vs-cold signal).
    let single_query = || (unit_vector(0), None, 5usize, None, None);

    let before_q1 = counters.snapshot();
    let (v, vs, k, nprobes, text) = single_query();
    let res_q1 = manager
        .query(&ns, v, vs, k, nprobes, text)
        .await
        .expect("query 1");
    let after_q1 = counters.snapshot();
    assert!(!res_q1.results.is_empty(), "query 1 returned no hits");

    let (v, vs, k, nprobes, text) = single_query();
    let res_q2 = manager
        .query(&ns, v, vs, k, nprobes, text)
        .await
        .expect("query 2");
    let after_q2 = counters.snapshot();
    assert!(!res_q2.results.is_empty(), "query 2 returned no hits");

    let q1_inserts = after_q1.insert.saturating_sub(before_q1.insert);
    let q1_gets = after_q1.get.saturating_sub(before_q1.get);
    let q2_inserts = after_q2.insert.saturating_sub(after_q1.insert);
    let q2_gets = after_q2.get.saturating_sub(after_q1.get);

    eprintln!("query 1: get +{q1_gets} insert +{q1_inserts}");
    eprintln!("query 2: get +{q2_gets} insert +{q2_inserts}");
    eprintln!("counters at end: {after_q2:?}");

    // --- assertions ---
    assert!(
        q1_inserts > 0,
        "query 1 must populate the cache (cold path); got insert +{q1_inserts}",
    );
    assert!(
        q2_gets > 0,
        "query 2 must check the cache; got get +{q2_gets}",
    );
    assert!(
        q2_inserts < q1_inserts,
        "query 2 must insert fewer entries than query 1 (cache warmed): \
         q1 insert +{q1_inserts}, q2 insert +{q2_inserts}",
    );
}
