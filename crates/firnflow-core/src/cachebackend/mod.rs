//! `lance_core::cache::CacheBackend` implementations for the index
//! cache hook lancedb 0.29+ exposes via
//! [`lancedb::ConnectBuilder::session(...)`].
//!
//! Two backends live here:
//!
//! - [`NoOpCacheBackend`] returns `None` from every `get`, runs the
//!   loader on every `get_or_insert`, discards `insert` payloads,
//!   and counts every call. Used by the reachability probe test to
//!   confirm the session install actually reaches Lance's hot path.
//! - [`FoyerCacheBackend`] is the candidate production cache layer:
//!   a two-tier store backed by a foyer `HybridCache` for entries
//!   whose value type has a serialisation codec, and a memory-only
//!   `DashMap` for entries without one. Built behind the
//!   reachability-probe wiring; whether it ships to a release is
//!   the question the prototype benchmark answers.
//!
//! Common counter type [`CacheBackendCounters`] is shared between
//! both implementations so a test can swap one backend for the
//! other and read traffic the same way.

mod foyer;
mod key;
mod noop;

pub use foyer::{FoyerCacheBackend, FoyerCacheBackendConfig};
pub use key::EncodedKey;
pub use noop::NoOpCacheBackend;

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters incremented by every call into a backend.
///
/// Wrap in an `Arc` and share the same handle between the backend
/// and the observer (typically a test). Used by both the no-op
/// reachability probe and the foyer adapter so the same assertion
/// shape works against either backend.
#[derive(Debug, Default)]
pub struct CacheBackendCounters {
    /// Total `get` calls.
    pub get: AtomicU64,
    /// Total `insert` calls.
    pub insert: AtomicU64,
    /// Total `get_or_insert` calls.
    pub get_or_insert: AtomicU64,
    /// Total `invalidate_prefix` calls.
    pub invalidate_prefix: AtomicU64,
    /// Total `clear` calls.
    pub clear: AtomicU64,
}

impl CacheBackendCounters {
    /// Snapshot every counter as a plain `u64`. Loads are
    /// independent with [`Ordering::Relaxed`]; the snapshot is not
    /// atomic across fields. Adequate for the "did any traffic
    /// arrive at all?" question the reachability probe asks.
    pub fn snapshot(&self) -> CacheBackendCountersSnapshot {
        CacheBackendCountersSnapshot {
            get: self.get.load(Ordering::Relaxed),
            insert: self.insert.load(Ordering::Relaxed),
            get_or_insert: self.get_or_insert.load(Ordering::Relaxed),
            invalidate_prefix: self.invalidate_prefix.load(Ordering::Relaxed),
            clear: self.clear.load(Ordering::Relaxed),
        }
    }
}

/// Plain-`u64` snapshot of [`CacheBackendCounters`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheBackendCountersSnapshot {
    /// Total `get` calls observed at snapshot time.
    pub get: u64,
    /// Total `insert` calls observed at snapshot time.
    pub insert: u64,
    /// Total `get_or_insert` calls observed at snapshot time.
    pub get_or_insert: u64,
    /// Total `invalidate_prefix` calls observed at snapshot time.
    pub invalidate_prefix: u64,
    /// Total `clear` calls observed at snapshot time.
    pub clear: u64,
}
