//! Regression test for stale cache reads after a process restart.
//!
//! The bug: the cache generation used to come from an in-memory counter
//! that reset to 0 on restart, while the foyer NVMe tier persists and
//! recovers its entries on reopen. After a restart the generation
//! sequence replayed from 0 and re-synthesised keys that matched
//! recovered entries from before a write, serving them as stale hits.
//!
//! The fix derives the generation from the Lance table version — a
//! persistent, monotonic value that advances on every commit. The read
//! path writes that version into the cache via
//! [`NamespaceCache::set_generation`] before every lookup, so a
//! recovered NVMe entry is reachable only when the table has not
//! changed since the entry was stored.
//!
//! This test exercises the cache layer directly and stands in for the
//! manager by calling `set_generation` with the version the manager
//! would report: the same version across a simulated restart means "no
//! writes happened" (the recovered entry is still valid and must hit);
//! a higher version means "a write advanced the table" (the recovered
//! entry is superseded and must miss).

use firnflow_core::cache::{NamespaceCache, QueryHash};
use firnflow_core::metrics::test_metrics;
use firnflow_core::{FirnflowError, NamespaceId};

const MEM_BYTES: usize = 16 * 1024 * 1024;
const DISK_BYTES: usize = 64 * 1024 * 1024;

async fn build(nvme: &std::path::Path) -> NamespaceCache {
    NamespaceCache::new(MEM_BYTES, nvme, DISK_BYTES, test_metrics())
        .await
        .expect("build cache")
}

#[tokio::test]
async fn restart_does_not_serve_entries_from_a_superseded_version() {
    let tmp = tempfile::tempdir().unwrap();
    let nvme = tmp.path();

    let ns = NamespaceId::new("acme").unwrap();
    let query = QueryHash::of(b"select top 10 where x > 5");

    // --- First process lifetime: cache a result at table version 1. ---
    let cache = build(nvme).await;
    cache.set_generation(&ns, 1);
    let r1 = cache
        .get_or_populate(&ns, query, || async {
            Ok::<_, FirnflowError>(b"v1".to_vec())
        })
        .await
        .unwrap();
    assert_eq!(r1, b"v1");

    // Flush the NVMe writer and shut down, as a clean restart would.
    cache.close().await.expect("close cache");
    drop(cache);

    // --- Restart over the same NVMe directory. The in-memory counter is
    //     gone; the recovered "v1" entry is still on disk. ---
    let restarted = build(nvme).await;

    // A write advanced the table to version 2 before/around the restart,
    // so the manager now reports version 2. The recovered v1 entry was
    // stored at version 1 and must not be served.
    restarted.set_generation(&ns, 2);
    let (hit, _) = restarted.try_get(&ns, query).await;
    assert!(
        hit.is_none(),
        "after a write advanced the table version, the recovered v1 entry \
         must not be served — that would be a stale read across the write"
    );

    // Control: keying at the original version 1 still finds the recovered
    // entry. This proves the miss above came from the version check, not
    // from a cold tier — the NVMe cache is genuinely reused across the
    // restart when the table has not moved on.
    restarted.set_generation(&ns, 1);
    let (hit, _) = restarted.try_get(&ns, query).await;
    assert_eq!(
        hit.as_deref(),
        Some(b"v1".as_slice()),
        "an unchanged table (same version) must still serve its recovered entry"
    );
}
