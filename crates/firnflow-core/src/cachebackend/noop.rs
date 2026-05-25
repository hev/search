//! No-op `lance_core::cache::CacheBackend` for reachability probes.
//!
//! Used to confirm that a custom backend installed through
//! [`lancedb::ConnectBuilder::session(...)`] actually receives
//! calls from Lance's index-cache code path on a real query. The
//! implementation deliberately:
//!
//! - Answers every `get` with `None` (cold miss).
//! - Runs the loader on every `get_or_insert`, returns the
//!   computed entry, and reports `was_cached = false` (Lance keeps
//!   making forward progress on cold opens).
//! - Discards every `insert` payload.
//! - Reports zero entries / zero bytes at all times.
//!
//! Every method bumps an atomic counter on the
//! `Arc<CacheBackendCounters>` the backend was constructed with,
//! so callers can snapshot the counts before and after a query
//! and confirm the cache layer is on the hot path.
//!
//! This is not a production cache layer. The foyer-backed
//! implementation in the sibling module is the candidate
//! production cache.

use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Future;
use lance_core::cache::{CacheBackend, CacheCodec, CacheEntry, InternalCacheKey};
use lance_core::Result as LanceResult;

use super::CacheBackendCounters;

/// A `CacheBackend` that stores nothing and counts every call.
///
/// See the module docs for the contract. Lance's cache hot path
/// stays correct (every lookup is a cold miss), counter changes
/// are the only observable side effect.
#[derive(Debug)]
pub struct NoOpCacheBackend {
    counters: Arc<CacheBackendCounters>,
}

impl NoOpCacheBackend {
    /// Construct a new no-op backend that records every call
    /// into `counters`. Clone the `Arc` and hold the clone
    /// externally to read counts after a query.
    pub fn new(counters: Arc<CacheBackendCounters>) -> Self {
        Self { counters }
    }
}

#[async_trait]
impl CacheBackend for NoOpCacheBackend {
    async fn get(&self, key: &InternalCacheKey, _codec: Option<CacheCodec>) -> Option<CacheEntry> {
        self.counters.get.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "firnflow_core::cachebackend",
            prefix = key.prefix(),
            key = key.key(),
            type_name = key.type_name(),
            "cachebackend get (miss)",
        );
        None
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        _entry: CacheEntry,
        size_bytes: usize,
        _codec: Option<CacheCodec>,
    ) {
        self.counters.insert.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "firnflow_core::cachebackend",
            prefix = key.prefix(),
            key = key.key(),
            type_name = key.type_name(),
            size_bytes,
            "cachebackend insert (discarded)",
        );
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = LanceResult<(CacheEntry, usize)>> + Send + 'a>>,
        _codec: Option<CacheCodec>,
    ) -> LanceResult<(CacheEntry, bool)> {
        self.counters.get_or_insert.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "firnflow_core::cachebackend",
            prefix = key.prefix(),
            key = key.key(),
            type_name = key.type_name(),
            "cachebackend get_or_insert (loader runs)",
        );
        let (entry, _size) = loader.await?;
        Ok((entry, false))
    }

    async fn invalidate_prefix(&self, prefix: &str) {
        self.counters
            .invalidate_prefix
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "firnflow_core::cachebackend",
            prefix,
            "cachebackend invalidate_prefix",
        );
    }

    async fn clear(&self) {
        self.counters.clear.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "firnflow_core::cachebackend",
            "cachebackend clear",
        );
    }

    async fn num_entries(&self) -> usize {
        0
    }

    async fn size_bytes(&self) -> usize {
        0
    }
}
