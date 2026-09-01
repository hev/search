//! Storage-backed proof of the versioned ANN hydration contract.
//!
//! Gated because it needs the repository's MinIO stack.

use std::collections::HashMap;
use std::sync::Arc;

use hevsearch_core::object_cache::{build_cached_session, ObjectCacheConfig};
use hevsearch_core::{CoreMetrics, NamespaceId, NamespaceManager, StorageRoot, UpsertRow};

fn minio_options() -> HashMap<String, String> {
    HashMap::from([
        (
            "aws_access_key_id".into(),
            std::env::var("HEVSEARCH_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into()),
        ),
        (
            "aws_secret_access_key".into(),
            std::env::var("HEVSEARCH_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into()),
        ),
        (
            "aws_endpoint".into(),
            std::env::var("HEVSEARCH_S3_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:9000".into()),
        ),
        ("aws_region".into(), "us-east-1".into()),
        ("allow_http".into(), "true".into()),
        ("aws_virtual_hosted_style_request".into(), "false".into()),
    ])
}

fn vector(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|column| (((seed * 7919 + column * 31) as f32) * 0.001).sin())
        .collect()
}

#[tokio::test]
#[ignore]
async fn complete_hydration_serves_novel_query_without_index_misses() {
    let bucket = std::env::var("HEVSEARCH_S3_BUCKET").unwrap_or_else(|_| "hevsearch-test".into());
    let namespace = NamespaceId::new(format!(
        "hydrate-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
    .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let metrics = Arc::new(CoreMetrics::new().unwrap());
    let config = ObjectCacheConfig::new(cache.path().to_path_buf(), 128 * 1024 * 1024);
    let session = build_cached_session(&config, metrics.object_cache());
    let manager = NamespaceManager::new(
        StorageRoot::s3_bucket(&bucket).unwrap(),
        minio_options(),
        Arc::clone(&metrics),
    )
    .with_object_cache_session(session);

    let result: Result<(), String> = async {
        let rows: Vec<UpsertRow> = (0..1024)
            .map(|row| (row as u64, vector(row, 32)).into())
            .collect();
        manager
            .upsert(&namespace, rows)
            .await
            .map_err(|error| format!("upsert: {error}"))?;
        manager
            .create_index(&namespace, Some(4), Some(4), Some(8))
            .await
            .map_err(|error| format!("index: {error}"))?;

        let report = manager
            .hydrate_index(&namespace, 64 * 1024 * 1024, 4)
            .await
            .map_err(|error| format!("hydrate: {error}"))?;
        if report.required_bytes == 0
            || report.range_gets == 0
            || report.cache_hits + report.cache_misses != report.range_gets
            || report.local_cache_occupancy_bytes < report.required_bytes
        {
            return Err(format!("incomplete hydration report: {report:?}"));
        }

        let before = metrics.object_cache().index_inner_gets.get();
        let results = manager
            .query(
                &namespace,
                vector(777, 32),
                None,
                10,
                None,
                None,
                None,
                false,
            )
            .await
            .map_err(|error| format!("novel query: {error}"))?;
        let after = metrics.object_cache().index_inner_gets.get();
        if after != before {
            return Err(format!(
                "novel indexed query made {} index-object backend GETs",
                after - before
            ));
        }
        if results
            .results
            .first()
            .map(|row| row.id.to_string())
            .as_deref()
            != Some("777")
        {
            return Err(format!(
                "unexpected self-match: {:?}",
                results.results.first()
            ));
        }

        // A write commits a new live version for which no hydration manifest
        // exists. Startup hydration must close readiness, not reuse the old one.
        manager
            .upsert(&namespace, vec![(10_000_u64, vector(10_000, 32)).into()])
            .await
            .map_err(|error| format!("version bump: {error}"))?;
        if manager
            .hydrate_index(&namespace, 64 * 1024 * 1024, 4)
            .await
            .is_ok()
        {
            return Err("changed table version reused an old hydration manifest".into());
        }
        Ok(())
    }
    .await;

    let _ = manager.delete(&namespace).await;
    result.expect("storage-backed hydration gates");
}
