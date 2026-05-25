//! Off-line tests for `FoyerCacheBackend`. No object storage or
//! lancedb involvement; the adapter is driven directly through the
//! `lance_core::cache::CacheBackend` trait.
//!
//! Three tests:
//!
//! 1. **Round-trip with a codec.** Insert a serialisable entry,
//!    get it back, check the value matches and the hot tier was
//!    used.
//! 2. **Memory-only fallback.** Insert with `codec = None`,
//!    confirm the cold tier holds it and the hot tier stays empty.
//! 3. **Prefix invalidation.** Two entries under prefix A, two
//!    under prefix B; invalidate A; A entries are gone, B entries
//!    intact, by-prefix index consistent.

use std::any::Any;
use std::sync::Arc;

use bytes::Bytes;
use firnflow_core::cachebackend::{
    CacheBackendCounters, FoyerCacheBackend, FoyerCacheBackendConfig,
};
use lance_core::cache::{CacheBackend, CacheCodec, CacheCodecImpl, CacheEntry, InternalCacheKey};
use lance_core::Result as LanceResult;

/// A tiny serialisable cache entry type for the round-trip test.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Probe {
    payload: Vec<u8>,
}

impl CacheCodecImpl for Probe {
    fn serialize(&self, writer: &mut dyn std::io::Write) -> LanceResult<()> {
        // `From<std::io::Error> for lance_core::Error` makes `?`
        // sufficient; no manual error wrapping needed.
        writer.write_all(&self.payload)?;
        Ok(())
    }

    fn deserialize(data: &Bytes) -> LanceResult<Self> {
        Ok(Self {
            payload: data.to_vec(),
        })
    }
}

fn key(prefix: &str, type_name: &'static str, k: &str) -> InternalCacheKey {
    InternalCacheKey::new(Arc::from(prefix), Arc::from(k), type_name)
}

async fn build(tmp: &tempfile::TempDir) -> Arc<FoyerCacheBackend> {
    let counters = Arc::new(CacheBackendCounters::default());
    let backend = FoyerCacheBackend::new(
        FoyerCacheBackendConfig {
            memory_bytes: 1024 * 1024,
            nvme_path: tmp.path(),
            nvme_bytes: 8 * 1024 * 1024,
        },
        counters,
    )
    .await
    .expect("foyer backend build");
    Arc::new(backend)
}

#[tokio::test]
async fn round_trip_with_codec() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = build(&tmp).await;
    let codec = CacheCodec::from_impl::<Probe>();
    let k = key("ds-a/", "Probe", "42");

    let value: CacheEntry = Arc::new(Probe {
        payload: b"hello world".to_vec(),
    });
    backend
        .insert(&k, Arc::clone(&value), 11, Some(codec))
        .await;

    let got = backend
        .get(&k, Some(codec))
        .await
        .expect("hot tier hit after insert");
    let got_probe: &Probe = (got.as_ref() as &dyn Any)
        .downcast_ref::<Probe>()
        .expect("downcast to Probe");
    assert_eq!(got_probe.payload, b"hello world");

    // Sanity: the cold tier should not have grown because the
    // entry went through the hot path.
    assert_eq!(backend.num_entries().await, 0, "cold tier must stay empty");
}

#[tokio::test]
async fn memory_only_fallback_when_codec_is_none() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = build(&tmp).await;
    let k = key("ds-b/", "OpaqueType", "99");

    // A boxed type that has no CacheCodecImpl. Stored as
    // `Arc<dyn Any + Send + Sync>` and never crosses a codec.
    let value: CacheEntry = Arc::new(vec![1u8, 2, 3, 4, 5]);
    backend.insert(&k, Arc::clone(&value), 5, None).await;

    let got = backend.get(&k, None).await.expect("cold tier hit");
    let got_vec: &Vec<u8> = (got.as_ref() as &dyn Any)
        .downcast_ref::<Vec<u8>>()
        .expect("downcast to Vec<u8>");
    assert_eq!(got_vec, &vec![1, 2, 3, 4, 5]);

    // The hot tier should still be at zero bytes since no codec
    // path was taken.
    assert_eq!(
        backend.size_bytes().await,
        0,
        "hot tier must stay empty under codec=None inserts",
    );
    // Cold tier carries exactly one entry.
    assert_eq!(
        backend.num_entries().await,
        1,
        "cold tier carries one entry"
    );
}

#[tokio::test]
async fn prefix_invalidation_removes_only_matching_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = build(&tmp).await;
    let codec = CacheCodec::from_impl::<Probe>();

    // Mix codec=None (cold tier, DashMap) and codec=Some (hot
    // tier, foyer HybridCache) under both prefixes so the
    // assertion covers both storage paths.
    let a_cold = key("ds-a/", "OpaqueType", "1");
    let a_hot = key("ds-a/", "Probe", "2");
    let b_cold = key("ds-b/", "OpaqueType", "1");
    let b_hot = key("ds-b/", "Probe", "2");

    let cold_entry = |n: u8| -> CacheEntry { Arc::new(vec![n]) };
    let hot_entry = |n: u8| -> CacheEntry {
        Arc::new(Probe {
            payload: vec![n; 4],
        })
    };
    backend.insert(&a_cold, cold_entry(1), 1, None).await;
    backend.insert(&a_hot, hot_entry(2), 4, Some(codec)).await;
    backend.insert(&b_cold, cold_entry(3), 1, None).await;
    backend.insert(&b_hot, hot_entry(4), 4, Some(codec)).await;

    // Sanity: hot lookups hit before invalidation.
    assert!(
        backend.get(&a_hot, Some(codec)).await.is_some(),
        "a_hot must be present before invalidation",
    );
    assert!(
        backend.get(&b_hot, Some(codec)).await.is_some(),
        "b_hot must be present before invalidation",
    );

    backend.invalidate_prefix("ds-a/").await;

    // ds-a/ entries gone across both tiers; ds-b/ entries
    // untouched across both tiers.
    assert!(
        backend.get(&a_cold, None).await.is_none(),
        "a_cold must be gone (cold tier)",
    );
    assert!(
        backend.get(&a_hot, Some(codec)).await.is_none(),
        "a_hot must be gone (hot tier)",
    );
    assert!(
        backend.get(&b_cold, None).await.is_some(),
        "b_cold must survive (cold tier)",
    );
    assert!(
        backend.get(&b_hot, Some(codec)).await.is_some(),
        "b_hot must survive (hot tier)",
    );

    // Now wipe everything with a wider prefix.
    backend.invalidate_prefix("ds-").await;
    assert!(
        backend.get(&b_cold, None).await.is_none(),
        "b_cold must be gone after wider prefix",
    );
    assert!(
        backend.get(&b_hot, Some(codec)).await.is_none(),
        "b_hot must be gone after wider prefix",
    );
}
