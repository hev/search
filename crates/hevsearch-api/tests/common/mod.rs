//! Shared helpers for integration tests.
//!
//! Adding a new field to `AppState` should change exactly one line
//! here, not ten — this module is the single point that constructs
//! `AppState` for tests without per-test churn.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use hevsearch_api::config::AppConfig;
use hevsearch_api::operations::OperationRegistry;
use hevsearch_api::AppState;
use hevsearch_core::cache::NamespaceCache;
use hevsearch_core::metrics::test_metrics;
use hevsearch_core::{NamespaceManager, NamespaceService, StorageRoot};

/// Read an env var with a default. Used to override MinIO defaults
/// in CI where the bucket and endpoint may differ from local dev.
pub fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Generate a unique namespace name. Uses nanoseconds since epoch
/// so concurrent tests do not share state on MinIO.
pub fn unique_namespace(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// MinIO `object_store` options for the local docker-compose stack.
pub fn minio_options() -> HashMap<String, String> {
    HashMap::from([
        (
            "aws_access_key_id".into(),
            env_or("HEVSEARCH_S3_ACCESS_KEY", "minioadmin"),
        ),
        (
            "aws_secret_access_key".into(),
            env_or("HEVSEARCH_S3_SECRET_KEY", "minioadmin"),
        ),
        (
            "aws_endpoint".into(),
            env_or("HEVSEARCH_S3_ENDPOINT", "http://127.0.0.1:9000"),
        ),
        ("aws_region".into(), "us-east-1".into()),
        ("allow_http".into(), "true".into()),
        ("aws_virtual_hosted_style_request".into(), "false".into()),
    ])
}

/// Build a default-open `AppState` against the local MinIO stack.
/// Returns the temp dir alongside so the caller can keep it alive
/// for the duration of the test (foyer NVMe tier lives here).
pub async fn test_state() -> (AppState, tempfile::TempDir) {
    let bucket = env_or("HEVSEARCH_S3_BUCKET", "hevsearch-test");
    let storage_root = StorageRoot::s3_bucket(&bucket).expect("test bucket name");
    let tmp = tempfile::tempdir().unwrap();
    let metrics = test_metrics();

    let manager = Arc::new(NamespaceManager::new(
        storage_root,
        minio_options(),
        Arc::clone(&metrics),
    ));
    let cache = Arc::new(
        NamespaceCache::new(
            16 * 1024 * 1024,
            tmp.path(),
            64 * 1024 * 1024,
            Arc::clone(&metrics),
        )
        .await
        .unwrap(),
    );
    let service = Arc::new(NamespaceService::new(
        Arc::clone(&manager),
        cache,
        Arc::clone(&metrics),
    ));
    let state = AppState {
        service,
        manager,
        metrics,
        operations: Arc::new(OperationRegistry::new()),
        max_body_bytes: 32 * 1024 * 1024,
        import_max_bytes: 0,
        import_tmp_dir: tmp.path().to_path_buf(),
    };
    (state, tmp)
}

/// Build an `AppState` with **no** S3 backend touch — purely
/// in-process. Skips MinIO entirely so tests that do not touch
/// storage can run without docker compose up.
pub async fn test_state_offline() -> (AppState, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let metrics = test_metrics();
    // Bucket name is irrelevant for tests that do not touch storage.
    // Use an obviously-fake hostname so any accidental touch fails
    // loudly rather than racing with a real MinIO.
    let manager = Arc::new(NamespaceManager::new(
        StorageRoot::s3_bucket("hevsearch-offline").unwrap(),
        HashMap::from([
            (
                "aws_endpoint".into(),
                "http://127.0.0.1:1".into(), // refuses connections
            ),
            ("aws_region".into(), "us-east-1".into()),
            ("allow_http".into(), "true".into()),
            ("aws_virtual_hosted_style_request".into(), "false".into()),
            ("aws_access_key_id".into(), "x".into()),
            ("aws_secret_access_key".into(), "x".into()),
        ]),
        Arc::clone(&metrics),
    ));
    let cache = Arc::new(
        NamespaceCache::new(
            4 * 1024 * 1024,
            tmp.path(),
            16 * 1024 * 1024,
            Arc::clone(&metrics),
        )
        .await
        .unwrap(),
    );
    let service = Arc::new(NamespaceService::new(
        Arc::clone(&manager),
        cache,
        Arc::clone(&metrics),
    ));
    let state = AppState {
        service,
        manager,
        metrics,
        operations: Arc::new(OperationRegistry::new()),
        max_body_bytes: 32 * 1024 * 1024,
        import_max_bytes: 0,
        import_tmp_dir: tmp.path().to_path_buf(),
    };
    (state, tmp)
}

/// Convenience: a minimal `AppConfig` for tests that need config
/// values without round-tripping through process env.
pub fn dummy_config() -> AppConfig {
    AppConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        storage_root: StorageRoot::s3_bucket("hevsearch-offline").unwrap(),
        cache_memory_bytes: 4 * 1024 * 1024,
        cache_nvme_path: std::env::temp_dir(),
        cache_nvme_bytes: 16 * 1024 * 1024,
        max_body_bytes: 32 * 1024 * 1024,
        storage_options: HashMap::new(),
        object_cache_enabled: false,
        object_cache_dir: std::env::temp_dir(),
        object_cache_bytes: 0,
        object_cache_max_entry_bytes: 0,
        import_max_bytes: 0,
        import_tmp_dir: std::env::temp_dir(),
    }
}
