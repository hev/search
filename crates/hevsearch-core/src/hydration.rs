//! Versioned contract for prefetching an ANN index into the local object cache
//! (GitHub issue #50).
//!
//! Hydration is deliberately narrower than dataset warming: manifests may name
//! immutable Lance index objects only. Data fragments, table manifests,
//! transaction logs, and version pointers remain demand-read from object
//! storage so a large raw-`f32` dataset is never mistaken for a resident
//! working set.

use serde::{Deserialize, Serialize};

use crate::HevSearchError;

/// Current on-object-storage hydration-manifest schema.
pub const HYDRATION_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// An immutable byte range required to serve arbitrary indexed queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HydrationRange {
    /// Object path relative to the Lance table root.
    pub path: String,
    /// Inclusive byte offset.
    pub start: u64,
    /// Exclusive byte offset.
    pub end: u64,
}

impl HydrationRange {
    /// Number of bytes represented by this range.
    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Whether the range is empty.
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

/// Immutable, engine-produced description of the ANN bytes to hydrate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HydrationManifest {
    /// Manifest format version, currently [`HYDRATION_MANIFEST_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Namespace this manifest was built for.
    pub namespace: String,
    /// Exact Lance table version whose index metadata produced this manifest.
    pub table_version: u64,
    /// Sorted, non-overlapping immutable index ranges.
    pub ranges: Vec<HydrationRange>,
}

/// Measurements returned after a complete admitted hydration pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HydrationReport {
    /// Lance table version hydrated.
    pub table_version: u64,
    /// Serialized manifest size read from object storage.
    pub manifest_bytes: u64,
    /// Mutable/versioned manifest reads performed (exactly one on success).
    pub manifest_gets: u64,
    /// Sum of all declared ranges.
    pub required_bytes: u64,
    /// Number of declared ranges fetched.
    pub range_gets: u64,
    /// Configured maximum in-flight range fetches.
    pub concurrency: usize,
    /// Wall-clock duration of the range-prefetch phase.
    pub elapsed_ms: u64,
    /// Object-cache counters attributable to the hydration pass.
    pub cache_inner_gets: u64,
    /// Declared objects already resident and served from the local cache.
    pub cache_hits: u64,
    /// Declared objects fetched from object storage and admitted locally.
    pub cache_misses: u64,
    /// Payload bytes fetched from object storage during hydration.
    pub object_store_bytes: u64,
    /// Local cache entries evicted during hydration.
    pub cache_evictions: u64,
    /// Resident immutable payload bytes currently held by the local object cache.
    pub local_cache_occupancy_bytes: u64,
    /// Process peak resident set size at hydration completion (Linux `VmHWM`).
    pub peak_rss_bytes: u64,
}

impl HydrationManifest {
    /// Validate identity, immutability, range shape, and the hydration byte cap.
    ///
    /// Returns the admitted byte count. The error for a byte-cap violation
    /// includes both required and allowed bytes so readiness can fail visibly.
    pub fn validate_for(
        &self,
        namespace: &str,
        live_table_version: u64,
        max_bytes: u64,
    ) -> Result<u64, HevSearchError> {
        if self.schema_version != HYDRATION_MANIFEST_SCHEMA_VERSION {
            return Err(HevSearchError::InvalidRequest(format!(
                "unsupported hydration manifest schema version {}; expected {}",
                self.schema_version, HYDRATION_MANIFEST_SCHEMA_VERSION
            )));
        }
        if self.namespace != namespace {
            return Err(HevSearchError::InvalidRequest(format!(
                "hydration manifest namespace {:?} does not match requested namespace {:?}",
                self.namespace, namespace
            )));
        }
        if self.table_version != live_table_version {
            return Err(HevSearchError::InvalidRequest(format!(
                "stale hydration manifest: manifest table version {}, live table version {}",
                self.table_version, live_table_version
            )));
        }
        if self.ranges.is_empty() {
            return Err(HevSearchError::InvalidRequest(
                "hydration manifest has no index ranges".into(),
            ));
        }

        let mut required_bytes = 0_u64;
        let mut previous: Option<&HydrationRange> = None;
        for range in &self.ranges {
            // Lance index UUIDs are immutable/write-once. Everything outside
            // `_indices/` includes data or mutable metadata and must bypass the
            // resident cache contract.
            if !range.path.starts_with("_indices/")
                || range.path.contains("/../")
                || range.path.ends_with("/..")
            {
                return Err(HevSearchError::InvalidRequest(format!(
                    "hydration range path is not an immutable Lance index object: {:?}",
                    range.path
                )));
            }
            if range.start >= range.end {
                return Err(HevSearchError::InvalidRequest(format!(
                    "hydration range for {:?} is empty or reversed: {}..{}",
                    range.path, range.start, range.end
                )));
            }
            if let Some(prev) = previous {
                if (range.path.as_str(), range.start) < (prev.path.as_str(), prev.start) {
                    return Err(HevSearchError::InvalidRequest(
                        "hydration ranges must be sorted by path and start offset".into(),
                    ));
                }
                if range.path == prev.path && range.start < prev.end {
                    return Err(HevSearchError::InvalidRequest(format!(
                        "hydration ranges overlap for {:?}: previous ends at {}, next starts at {}",
                        range.path, prev.end, range.start
                    )));
                }
            }
            required_bytes = required_bytes.checked_add(range.len()).ok_or_else(|| {
                HevSearchError::InvalidRequest(
                    "hydration manifest required byte count overflows u64".into(),
                )
            })?;
            previous = Some(range);
        }

        if required_bytes > max_bytes {
            return Err(HevSearchError::InvalidRequest(format!(
                "hydration budget exceeded: required_bytes={required_bytes}, allowed_bytes={max_bytes}"
            )));
        }
        Ok(required_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> HydrationManifest {
        HydrationManifest {
            schema_version: 1,
            namespace: "story-phase2".into(),
            table_version: 42,
            ranges: vec![
                HydrationRange {
                    path: "_indices/abc/auxiliary.idx".into(),
                    start: 0,
                    end: 12,
                },
                HydrationRange {
                    path: "_indices/abc/index.idx".into(),
                    start: 10,
                    end: 30,
                },
            ],
        }
    }

    #[test]
    fn admits_exact_budget() {
        assert_eq!(manifest().validate_for("story-phase2", 42, 32).unwrap(), 32);
    }

    #[test]
    fn rejects_stale_version() {
        let error = manifest().validate_for("story-phase2", 43, 32).unwrap_err();
        assert!(error.to_string().contains("manifest table version 42"));
        assert!(error.to_string().contains("live table version 43"));
    }

    #[test]
    fn rejects_budget_with_required_and_allowed_counts() {
        let error = manifest().validate_for("story-phase2", 42, 31).unwrap_err();
        assert!(error.to_string().contains("required_bytes=32"));
        assert!(error.to_string().contains("allowed_bytes=31"));
    }

    #[test]
    fn rejects_data_and_mutable_metadata_paths() {
        for path in [
            "data/abc.lance",
            "_versions/42.manifest",
            "_transactions/1.txn",
        ] {
            let mut candidate = manifest();
            candidate.ranges[0].path = path.into();
            assert!(candidate.validate_for("story-phase2", 42, 32).is_err());
        }
    }

    #[test]
    fn rejects_overlapping_ranges() {
        let mut candidate = manifest();
        candidate.ranges = vec![
            HydrationRange {
                path: "_indices/abc/index.idx".into(),
                start: 0,
                end: 20,
            },
            HydrationRange {
                path: "_indices/abc/index.idx".into(),
                start: 10,
                end: 30,
            },
        ];
        assert!(candidate.validate_for("story-phase2", 42, 40).is_err());
    }
}
