//! foyer-backed `lance_core::cache::CacheBackend`.
//!
//! Two-tier storage:
//!
//! 1. **Hot tier.** A foyer `HybridCache<EncodedKey, Vec<u8>>`,
//!    RAM-resident with optional NVMe spill. Receives every entry
//!    whose [`CacheCodec`] is `Some(_)`: the entry is serialised
//!    to bytes via the caller-supplied codec on insert and
//!    deserialised on get.
//! 2. **Cold tier.** A `DashMap<InternalCacheKey, CacheEntry>`,
//!    memory-only. Receives every entry whose codec is `None`,
//!    since `Arc<dyn Any>` cannot be serialised through serde.
//!    Cleared on process exit; never spills to disk.
//!
//! A `DashMap<Arc<str>, DashSet<InternalCacheKey>>` maintains the
//! reverse index that lets `invalidate_prefix` find entries to
//! remove without scanning foyer's keyspace (foyer has no
//! prefix-scan API on either tier).
//!
//! Concurrent loaders of the same key are **not** deduplicated.
//! The `CacheBackend` trait says "should deduplicate"; this
//! implementation runs the loader on every `get_or_insert` miss,
//! and the second concurrent inserter overwrites the first. The
//! trade-off was chosen so the implementation does not need to
//! reconcile foyer's `'static` loader bound with Lance's `'a`
//! lifetime on the supplied future. Re-fetch is correct; only the
//! work is duplicated. Revisit if the prototype benchmark surfaces
//! it as a hot path.

use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};
use futures::Future;
use lance_core::cache::{CacheBackend, CacheCodec, CacheEntry, InternalCacheKey};
use lance_core::Result as LanceResult;

use super::{CacheBackendCounters, EncodedKey};
use crate::FirnflowError;

/// Tier sizing for [`FoyerCacheBackend`].
///
/// `memory_bytes` is RAM-tier capacity and `nvme_bytes` is the
/// on-disk capacity for the foyer block file rooted at `nvme_path`.
/// The prototype benchmark uses 1 GiB / 10 GiB respectively;
/// production sizing is whatever the deployment can afford.
pub struct FoyerCacheBackendConfig<'a> {
    /// Memory-tier capacity, in bytes.
    pub memory_bytes: usize,
    /// Directory the foyer block file lives in. Must exist.
    pub nvme_path: &'a Path,
    /// NVMe-tier capacity, in bytes.
    pub nvme_bytes: usize,
}

/// foyer-backed `CacheBackend` with a memory-only side store for
/// entries whose value type has no [`CacheCodec`].
///
/// See the module-level docs for the full storage model. Counter
/// instrumentation is inherited from the no-op backend so tests
/// can swap one for the other without rewriting assertions.
pub struct FoyerCacheBackend {
    /// Serialisable entries. Foyer manages memory ↔ disk spill.
    hot: HybridCache<EncodedKey, Vec<u8>>,
    /// Non-serialisable entries (codec = None). Memory only.
    cold: DashMap<InternalCacheKey, CacheEntry>,
    /// Reverse index: prefix string → set of keys stored under
    /// that exact prefix. `invalidate_prefix(p)` walks the outer
    /// map for prefixes that `starts_with(p)`.
    by_prefix: DashMap<Arc<str>, DashSet<InternalCacheKey>>,
    /// Call counters; same shape as the no-op probe so the same
    /// snapshot/assertion pattern works against either backend.
    counters: Arc<CacheBackendCounters>,
}

impl std::fmt::Debug for FoyerCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerCacheBackend")
            .field("cold_entries", &self.cold.len())
            .field("by_prefix_buckets", &self.by_prefix.len())
            .finish_non_exhaustive()
    }
}

impl FoyerCacheBackend {
    /// Construct a new foyer-backed cache.
    ///
    /// Returns `FirnflowError::Cache` on foyer device or builder
    /// failure (typically misconfigured `nvme_path` or insufficient
    /// disk space).
    pub async fn new(
        config: FoyerCacheBackendConfig<'_>,
        counters: Arc<CacheBackendCounters>,
    ) -> Result<Self, FirnflowError> {
        let device = FsDeviceBuilder::new(config.nvme_path)
            .with_capacity(config.nvme_bytes)
            .build()
            .map_err(|e| FirnflowError::Cache(format!("cachebackend device build: {e}")))?;

        let hot: HybridCache<EncodedKey, Vec<u8>> = HybridCacheBuilder::new()
            .memory(config.memory_bytes)
            .storage()
            .with_engine_config(BlockEngineConfig::new(device))
            .build()
            .await
            .map_err(|e| FirnflowError::Cache(format!("cachebackend hybrid build: {e}")))?;

        Ok(Self {
            hot,
            cold: DashMap::new(),
            by_prefix: DashMap::new(),
            counters,
        })
    }

    /// Shared `CacheBackendCounters` handle. Tests clone this and
    /// snapshot it before / after a query to observe traffic.
    pub fn counters(&self) -> Arc<CacheBackendCounters> {
        Arc::clone(&self.counters)
    }

    /// Register `key` under its prefix bucket so
    /// `invalidate_prefix` can find it without scanning foyer's
    /// keyspace. One `Arc<str>` allocation per insert when the
    /// prefix bucket is new; existing buckets are O(1).
    fn register(&self, key: InternalCacheKey) {
        let prefix: Arc<str> = Arc::from(key.prefix());
        self.by_prefix.entry(prefix).or_default().insert(key);
    }
}

#[async_trait]
impl CacheBackend for FoyerCacheBackend {
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        self.counters.get.fetch_add(1, Ordering::Relaxed);

        match codec {
            Some(codec) => {
                let encoded = EncodedKey::from(key);
                match self.hot.get(&encoded).await {
                    Ok(Some(entry)) => {
                        let bytes = Bytes::copy_from_slice(entry.value());
                        match codec.deserialize(&bytes) {
                            Ok(deserialised) => Some(deserialised),
                            Err(e) => {
                                tracing::warn!(
                                    target: "firnflow_core::cachebackend",
                                    prefix = key.prefix(),
                                    key = key.key(),
                                    type_name = key.type_name(),
                                    error = %e,
                                    "cachebackend hot deserialize failed; treating as miss",
                                );
                                None
                            }
                        }
                    }
                    Ok(None) => None,
                    Err(e) => {
                        tracing::warn!(
                            target: "firnflow_core::cachebackend",
                            prefix = key.prefix(),
                            key = key.key(),
                            type_name = key.type_name(),
                            error = %e,
                            "cachebackend hot get failed; treating as miss",
                        );
                        None
                    }
                }
            }
            None => self.cold.get(key).map(|e| Arc::clone(e.value())),
        }
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        _size_bytes: usize,
        codec: Option<CacheCodec>,
    ) {
        self.counters.insert.fetch_add(1, Ordering::Relaxed);

        match codec {
            Some(codec) => {
                let mut buf = Vec::new();
                if let Err(e) = codec.serialize(&entry, &mut buf) {
                    tracing::warn!(
                        target: "firnflow_core::cachebackend",
                        prefix = key.prefix(),
                        key = key.key(),
                        type_name = key.type_name(),
                        error = %e,
                        "cachebackend hot serialize failed; entry not cached",
                    );
                    return;
                }
                let encoded = EncodedKey::from(key);
                self.hot.insert(encoded, buf);
            }
            None => {
                self.cold.insert(key.clone(), entry);
            }
        }
        self.register(key.clone());
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = LanceResult<(CacheEntry, usize)>> + Send + 'a>>,
        codec: Option<CacheCodec>,
    ) -> LanceResult<(CacheEntry, bool)> {
        self.counters.get_or_insert.fetch_add(1, Ordering::Relaxed);

        // Fast path: hit. Re-uses the trait's own `get` so the
        // codec / cold-tier branching stays in one place.
        if let Some(hit) = self.get(key, codec).await {
            return Ok((hit, true));
        }

        // Slow path: run the loader, populate, return uncached.
        let (entry, size_bytes) = loader.await?;
        self.insert(key, Arc::clone(&entry), size_bytes, codec)
            .await;
        Ok((entry, false))
    }

    async fn invalidate_prefix(&self, prefix: &str) {
        self.counters
            .invalidate_prefix
            .fetch_add(1, Ordering::Relaxed);

        // Collect first, mutate second: DashMap iteration is
        // unsafe to interleave with structural mutation.
        let bucket_keys: Vec<Arc<str>> = self
            .by_prefix
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| Arc::clone(entry.key()))
            .collect();

        for bucket_key in bucket_keys {
            if let Some((_, bucket)) = self.by_prefix.remove(&bucket_key) {
                for entry_key in bucket.iter() {
                    let key: &InternalCacheKey = entry_key.key();
                    self.hot.remove(&EncodedKey::from(key));
                    self.cold.remove(key);
                }
            }
        }
    }

    async fn clear(&self) {
        self.counters.clear.fetch_add(1, Ordering::Relaxed);
        // foyer's clear can fail on the disk tier; log and
        // continue. The memory tier is dropped regardless.
        if let Err(e) = self.hot.clear().await {
            tracing::warn!(
                target: "firnflow_core::cachebackend",
                error = %e,
                "cachebackend hot clear failed; memory tier still dropped",
            );
        }
        self.cold.clear();
        self.by_prefix.clear();
    }

    async fn num_entries(&self) -> usize {
        // foyer's `Cache::usage()` returns bytes, not entries, and
        // its `HybridCache` does not surface a per-tier entry
        // count cheaply. The cold tier is exact; the hot tier is
        // not measured here. Acceptable since this method is only
        // used for telemetry / debug, not correctness.
        self.cold.len()
    }

    async fn size_bytes(&self) -> usize {
        // Memory-tier bytes from foyer; cold tier has no weight
        // tracking. Disk tier deliberately not added: it is
        // capacity-bounded by the device, not by any per-entry
        // accounting visible from here.
        self.hot.memory().usage()
    }

    fn approx_num_entries(&self) -> usize {
        self.cold.len()
    }

    fn approx_size_bytes(&self) -> usize {
        self.hot.memory().usage()
    }
}
