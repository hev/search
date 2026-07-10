//! Axum application state.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use hevsearch_core::cache::NamespaceCache;
use hevsearch_core::{CoreMetrics, NamespaceManager, NamespaceService};

use crate::config::AppConfig;
use crate::operations::OperationRegistry;

/// Shared state every handler receives via `axum::extract::State`.
///
/// Derives `Clone` because axum clones the state per-request to
/// hand it to extractors. Everything inside is already `Arc`-wrapped
/// so the clone is cheap.
#[derive(Clone)]
pub struct AppState {
    /// Service facade combining the cache and the namespace manager.
    /// Every cached read/write path goes through this.
    pub service: Arc<NamespaceService>,
    /// Direct manager handle for the one endpoint that intentionally
    /// bypasses the foyer cache (the `/list` path). Shares its
    /// underlying `NamespaceManager` with `service`; holding a
    /// separate `Arc` makes the architectural split explicit.
    pub manager: Arc<NamespaceManager>,
    /// Shared metrics registry; the `/metrics` handler encodes
    /// this into the Prometheus text format.
    pub metrics: Arc<CoreMetrics>,
    /// Registry of in-flight and recently-finished background
    /// operations (index builds, compaction, warmup). Backs the
    /// `GET /operations/{id}` status endpoint.
    pub operations: Arc<OperationRegistry>,
    /// Maximum request body size in bytes. The router reads this
    /// once at build time and installs a single global
    /// `DefaultBodyLimit` layer; it is not consulted per request.
    pub max_body_bytes: usize,
    /// Cap on a single `/import` Arrow body (spooled to disk). `0`
    /// disables the cap; otherwise the handler returns `413` once the
    /// streamed body exceeds it. The `/import` route bypasses
    /// `max_body_bytes`, so this is its replacement guard.
    pub import_max_bytes: u64,
    /// Directory the `/import` handler spools request bodies to.
    pub import_tmp_dir: PathBuf,
}

/// Assemble `AppState` from an `AppConfig`.
pub async fn build_state(cfg: &AppConfig) -> anyhow::Result<AppState> {
    tracing::warn!(
        "hevsearch API is running without authentication; expects a trusted gateway / NetworkPolicy"
    );

    let metrics =
        Arc::new(CoreMetrics::new().map_err(|e| anyhow::anyhow!("build metrics registry: {e}"))?);

    let mut manager = NamespaceManager::new(
        cfg.storage_root.clone(),
        cfg.storage_options.clone(),
        Arc::clone(&metrics),
    );
    if cfg.object_cache_enabled {
        std::fs::create_dir_all(&cfg.object_cache_dir).with_context(|| {
            format!(
                "creating object cache directory {}",
                cfg.object_cache_dir.display()
            )
        })?;
        let mut oc_cfg = hevsearch_core::object_cache::ObjectCacheConfig::new(
            cfg.object_cache_dir.clone(),
            cfg.object_cache_bytes,
        );
        oc_cfg.max_entry_bytes = cfg.object_cache_max_entry_bytes;
        // Hand the registered object-cache counters in so hits/misses/evictions surface at /metrics.
        let session =
            hevsearch_core::object_cache::build_cached_session(&oc_cfg, metrics.object_cache());
        manager = manager.with_object_cache_session(session);
        tracing::info!(
            dir = %cfg.object_cache_dir.display(),
            capacity_bytes = cfg.object_cache_bytes,
            "object cache enabled (issue #51): Lance object-store reads served from local NVMe"
        );
    }
    let manager = Arc::new(manager);

    std::fs::create_dir_all(&cfg.cache_nvme_path).with_context(|| {
        format!(
            "creating cache nvme directory {}",
            cfg.cache_nvme_path.display()
        )
    })?;

    let cache = Arc::new(
        NamespaceCache::new(
            cfg.cache_memory_bytes,
            &cfg.cache_nvme_path,
            cfg.cache_nvme_bytes,
            Arc::clone(&metrics),
        )
        .await
        .map_err(|e| anyhow::anyhow!("build namespace cache: {e}"))?,
    );

    let service = Arc::new(NamespaceService::new(
        Arc::clone(&manager),
        cache,
        Arc::clone(&metrics),
    ));

    let operations = Arc::new(OperationRegistry::new());

    Ok(AppState {
        service,
        manager,
        metrics,
        operations,
        max_body_bytes: cfg.max_body_bytes,
        import_max_bytes: cfg.import_max_bytes,
        import_tmp_dir: cfg.import_tmp_dir.clone(),
    })
}
