//! Namespace service — combines the Lance backend, the foyer hybrid
//! cache, and the bincode result payload format into a single
//! cache-aside read path with invalidate-on-write.
//!
//! * [`NamespaceManager`] — the Lance backend
//! * [`NamespaceCache`] — foyer hybrid cache + generation counter
//! * bincode-2 serde path — the cached result payload format
//!
//! The axum handlers own an `Arc<NamespaceService>` and call
//! straight into `upsert` / `query`.
//!
//! Every call records query/write duration histograms and an
//! `s3_requests_total` counter. The cache hit/miss counters live on
//! the cache itself — see [`NamespaceCache::try_get`].

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatchReader;
use bincode::config;
use serde::Serialize;

use crate::cache::{NamespaceCache, QueryHash};
use crate::manager::{CompactResult, NamespaceManager, UpsertRow};
use crate::metrics::CoreMetrics;
use crate::query::{validate_facet_request, validate_query_request, FacetRequest, QueryRequest};
use crate::DistanceMetric;
use crate::{FacetResultSet, HevSearchError, NamespaceId, QueryResultSet};

/// Where a query result came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCacheSource {
    /// The query ran against the underlying Lance backend.
    Backend,
    /// The exact result cache served the query.
    ExactCache,
}

impl QueryCacheSource {
    /// Stable header value used by the API's debug cache-source signal.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backend => "backend",
            Self::ExactCache => "exact_cache",
        }
    }
}

/// Query results plus the cache/backend source used to produce them.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryOutcome {
    /// Ranked search results.
    pub result: QueryResultSet,
    /// Cache/backend source for observability and benchmarks.
    pub cache_source: QueryCacheSource,
}

/// Service facade over [`NamespaceManager`] + [`NamespaceCache`].
pub struct NamespaceService {
    manager: Arc<NamespaceManager>,
    cache: Arc<NamespaceCache>,
    metrics: Arc<CoreMetrics>,
}

impl NamespaceService {
    /// Construct a new service wrapping a manager, a cache, and a
    /// metrics handle that will be shared across every handler in
    /// the API. `cache` must already have been constructed with the
    /// same `metrics` so hit/miss counts land on the same registry.
    pub fn new(
        manager: Arc<NamespaceManager>,
        cache: Arc<NamespaceCache>,
        metrics: Arc<CoreMetrics>,
    ) -> Self {
        Self {
            manager,
            cache,
            metrics,
        }
    }

    /// Write path: upsert rows via the manager (merge-insert by `id`).
    /// The write advances the Lance table version, which is the cache
    /// generation, so the next read derives a new generation and every
    /// result cached against the pre-write version becomes unreachable
    /// — no explicit cache bump is needed. Because the version only
    /// moves on a successful commit, a failed write leaves the cache
    /// self-consistent (it keeps serving the pre-failure results). The
    ///
    /// Records `s3_requests_total{operation="upsert"}` eagerly (one
    /// per call) and `write_duration_seconds` on return.
    pub async fn upsert(
        &self,
        ns: &NamespaceId,
        rows: Vec<UpsertRow>,
    ) -> Result<(), HevSearchError> {
        self.upsert_with_distance_metric(ns, rows, None).await
    }

    /// Write path with an optional namespace-creation distance metric.
    pub async fn upsert_with_distance_metric(
        &self,
        ns: &NamespaceId,
        rows: Vec<UpsertRow>,
        distance_metric: Option<DistanceMetric>,
    ) -> Result<(), HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "upsert");
        self.manager
            .upsert_with_distance_metric(ns, rows, distance_metric)
            .await?;
        self.metrics.record_write(ns, start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Bulk-append an Arrow IPC stream insert-only, in a single commit
    /// (the binary `/import` path). Cache handling matches
    /// [`upsert`](Self::upsert): the append advances the table version,
    /// so results cached against the pre-import version become
    /// unreachable with no explicit bump. Returns the number of rows appended.
    ///
    /// Records `s3_requests_total{operation="import"}` eagerly and
    /// `write_duration_seconds` on return.
    pub async fn import(
        &self,
        ns: &NamespaceId,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> Result<usize, HevSearchError> {
        self.import_with_distance_metric(ns, reader, None).await
    }

    /// Bulk import with an optional namespace-creation distance metric.
    pub async fn import_with_distance_metric(
        &self,
        ns: &NamespaceId,
        reader: Box<dyn RecordBatchReader + Send>,
        distance_metric: Option<DistanceMetric>,
    ) -> Result<usize, HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "import");
        let imported = self
            .manager
            .import_arrow_with_distance_metric(ns, reader, distance_metric)
            .await?;
        self.metrics.record_write(ns, start.elapsed().as_secs_f64());
        Ok(imported)
    }

    /// Delete every object under the namespace prefix.
    ///
    /// Removing the Lance table drops the namespace back to "no table"
    /// (generation 0 on the next read), so results cached against the
    /// deleted table become unreachable. Recreating the namespace under
    /// the same name is safe even though its Lance version restarts at
    /// 1: the cache generation folds in the manifest commit timestamp
    /// (see [`NamespaceManager::generation`](crate::NamespaceManager::generation)),
    /// so the recreated incarnation keys differently from the deleted
    /// one and cannot re-serve its cached bytes. A failed delete leaves
    /// the cache serving the pre-delete entries, self-consistent with
    /// the data still sitting in object storage.
    ///
    /// Returns the number of objects the manager removed.
    pub async fn delete(&self, ns: &NamespaceId) -> Result<usize, HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "delete");
        let count = self.manager.delete(ns).await?;
        self.metrics.record_write(ns, start.elapsed().as_secs_f64());
        Ok(count)
    }

    /// Delete rows from an existing namespace by id.
    pub async fn delete_ids(
        &self,
        ns: &NamespaceId,
        ids: &[crate::RowId],
    ) -> Result<u64, HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "delete_ids");
        let count = self.manager.delete_ids(ns, ids).await?;
        self.metrics.record_write(ns, start.elapsed().as_secs_f64());
        Ok(count)
    }

    /// Delete rows from an existing namespace by DataFusion SQL
    /// predicate.
    pub async fn delete_rows(
        &self,
        ns: &NamespaceId,
        predicate: &str,
    ) -> Result<u64, HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "delete_rows");
        let count = self.manager.delete_rows(ns, predicate).await?;
        self.metrics.record_write(ns, start.elapsed().as_secs_f64());
        Ok(count)
    }

    /// Read path: check the exact cache and fall through to the manager
    /// on a miss, populating it with the serialised result on the way back.
    ///
    /// The exact-cache key is a deterministic hash of the bincode
    /// encoding of the request's cacheable fields. The capture-once
    /// generation discipline lives inside
    /// [`NamespaceCache::try_get`] / [`NamespaceCache::populate_with_generation`];
    /// we don't reimplement it here.
    ///
    /// Records `query_duration_seconds{query_type="…"}` around the
    /// whole path and `s3_requests_total{operation="query"}` only
    /// when the backend actually runs (exact hits stay off the S3
    /// counter, which is the entire point of the metric).
    pub async fn query(
        &self,
        ns: &NamespaceId,
        req: &QueryRequest,
    ) -> Result<QueryResultSet, HevSearchError> {
        Ok(self.query_with_cache_source(ns, req).await?.result)
    }

    /// Same read path as [`Self::query`], but returns the cache/backend
    /// source used for this request. Intended for API debug headers and
    /// benchmark harnesses; normal callers should keep using
    /// [`Self::query`].
    pub async fn query_with_cache_source(
        &self,
        ns: &NamespaceId,
        req: &QueryRequest,
    ) -> Result<QueryOutcome, HevSearchError> {
        let start = Instant::now();
        validate_query_request(req)?;

        let query_hash = hash_query_for_cache(req)?;

        // Derive the cache generation from the persistent Lance table
        // version before consulting either layer. The in-process
        // generation counter resets to 0 on restart; the table version
        // does not, so seeding the counter from it makes a recovered
        // NVMe entry reachable only when the namespace has not changed
        // since the entry was stored. The exact cache keys off this
        // value. Cheap on a warm handle — an in-memory manifest read.
        let generation = self.manager.generation(ns).await?;
        self.cache.set_generation(ns, generation);

        // 1. Exact cache — always consulted, opt-in or not. A payload
        //    that fails to decode is treated as a miss, not an error:
        //    the NVMe tier survives restarts, so after an upgrade that
        //    changes the result wire format a recovered entry can hit
        //    at the same key with bytes this build cannot read. The
        //    fall-through re-runs the query and repopulates the entry
        //    with the current format — self-healing, at the cost of
        //    one backend round-trip.
        let (exact_hit, captured_generation) = self.cache.try_get(ns, query_hash).await;
        if let Some(bytes) = exact_hit {
            match decode_payload(&bytes) {
                Ok(decoded) => {
                    self.metrics.record_query(
                        ns,
                        classify_query_type(req),
                        start.elapsed().as_secs_f64(),
                    );
                    return Ok(QueryOutcome {
                        result: decoded,
                        cache_source: QueryCacheSource::ExactCache,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        namespace = %ns,
                        error = %e,
                        "cached result payload failed to decode; \
                         treating as a miss and re-running the query"
                    );
                }
            }
        }

        // 2. Backend — one s3_request per cache-miss query.
        self.metrics.record_s3_request(ns, "query");
        let result = self
            .manager
            .query_with_fuzzy(
                ns,
                req.vector.clone(),
                req.vectors.clone(),
                req.k,
                req.nprobes,
                req.exact,
                req.text.clone(),
                req.fuzzy.clone(),
                req.filter.clone(),
                req.include_vector,
            )
            .await?;
        let bytes = encode_payload(&result)?;
        self.cache
            .populate_with_generation(ns, captured_generation, query_hash, bytes);

        self.metrics
            .record_query(ns, classify_query_type(req), start.elapsed().as_secs_f64());
        Ok(QueryOutcome {
            result,
            cache_source: QueryCacheSource::Backend,
        })
    }

    /// Compute facet counts through the exact cache.
    pub async fn facet(
        &self,
        ns: &NamespaceId,
        req: &FacetRequest,
    ) -> Result<FacetResultSet, HevSearchError> {
        let top = validate_facet_request(req)?;
        let mut fields = req.fields.clone();
        fields.sort();
        let hash = hash_facet_for_cache(req.filter.as_ref(), &fields, top)?;
        let generation = self.manager.generation(ns).await?;
        self.cache.set_generation(ns, generation);
        let (hit, captured_generation) = self.cache.try_get(ns, hash).await;
        if let Some(bytes) = hit {
            match decode_facet_payload(&bytes) {
                Ok(decoded) => return Ok(decoded),
                Err(e) => {
                    tracing::warn!(
                        namespace = %ns,
                        error = %e,
                        "cached facet payload failed to decode; treating as a miss"
                    );
                }
            }
        }

        self.metrics.record_s3_request(ns, "facet");
        let result = self
            .manager
            .facet(ns, req.filter.clone(), &fields, top)
            .await?;
        let bytes = encode_facet_payload(&result)?;
        self.cache
            .populate_with_generation(ns, captured_generation, hash, bytes);
        Ok(result)
    }

    /// Build an IVF_PQ index on the namespace's vector column.
    ///
    /// Records `hevsearch_index_build_duration_seconds{namespace, kind}`
    /// on completion — the "Index Tax" metric.
    ///
    /// Building an index is a Lance commit, so it advances the table
    /// version and the next read derives a new generation: results
    /// cached before the build become unreachable. This is a behaviour
    /// change from the old generation-counter design, which left the
    /// cache untouched on index builds. It is the safer default —
    /// post-build queries run against the new index instead of
    /// replaying a pre-index cached result — at the cost of dropping
    /// the warm cache after an infrequent, operator-triggered build.
    pub async fn create_index(
        &self,
        ns: &NamespaceId,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        num_bits: Option<u32>,
    ) -> Result<(), HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "index");
        self.manager
            .create_index(ns, num_partitions, num_sub_vectors, num_bits)
            .await?;
        self.metrics
            .record_index_build(ns, "ivf_pq", start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Build a BM25 full-text search index on the namespace's analyzed
    /// text surface (`text_tok`, RFC 0001), backfilling the column on
    /// pre-RFC-0001 namespaces first.
    pub async fn create_fts_index(&self, ns: &NamespaceId) -> Result<(), HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "fts_index");
        self.manager.create_fts_index(ns).await?;
        self.metrics
            .record_index_build(ns, "fts", start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Build a BTree scalar index on `column` (v1: `_ingested_at`
    /// only). Records `hevsearch_index_build_duration_seconds{kind="scalar"}`
    /// on completion.
    ///
    /// Like the other index builders, this is a Lance commit, so it
    /// advances the table version and the next read derives a new
    /// generation — the warm cache is dropped even though the rows
    /// themselves are unchanged. See [`create_index`](Self::create_index).
    pub async fn create_scalar_index(
        &self,
        ns: &NamespaceId,
        column: &str,
    ) -> Result<(), HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "scalar_index");
        self.manager.create_scalar_index(ns, column).await?;
        self.metrics
            .record_index_build(ns, "scalar", start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Compact the namespace's data files.
    ///
    /// Compaction is a Lance commit, so it advances the table version
    /// and the next read derives a new generation — results cached
    /// against the pre-compaction version fall out of reach without an
    /// explicit bump.
    ///
    /// Records `hevsearch_compaction_duration_seconds{namespace}` on
    /// completion.
    pub async fn compact(&self, ns: &NamespaceId) -> Result<CompactResult, HevSearchError> {
        let start = Instant::now();
        self.metrics.record_s3_request(ns, "compact");
        let result = self.manager.compact(ns).await?;
        self.metrics
            .record_compaction(ns, start.elapsed().as_secs_f64());
        Ok(result)
    }
}

/// Hash the cacheable fields of `req` for the exact-cache key.
///
/// `include_vector` is included because the response payload differs
/// when vectors are omitted. Bincode-2 over a
/// tuple-view of the underlying fields gives a deterministic,
/// allocation-light encoding suitable for hashing.
///
/// `pub` but hidden: integration tests use it to seed cache entries
/// at the same key the read path derives. Not a public API.
#[doc(hidden)]
pub fn hash_query_for_cache(req: &QueryRequest) -> Result<QueryHash, HevSearchError> {
    #[derive(Serialize)]
    struct Canonical<'a> {
        vector: &'a Vec<f32>,
        vectors: &'a Option<Vec<Vec<f32>>>,
        k: usize,
        nprobes: Option<usize>,
        exact: bool,
        text: &'a Option<String>,
        fuzzy: &'a Option<crate::query::FuzzyRequest>,
        filter: &'a Option<String>,
        // A full and a vector-light result set are different payloads
        // and must not collide on the same entry.
        include_vector: bool,
    }
    let canonical = Canonical {
        vector: &req.vector,
        vectors: &req.vectors,
        k: req.k,
        nprobes: req.nprobes,
        exact: req.exact,
        text: &req.text,
        fuzzy: &req.fuzzy,
        filter: &req.filter,
        include_vector: req.include_vector,
    };
    let bytes = bincode::serde::encode_to_vec(&canonical, config::standard())
        .map_err(|e| HevSearchError::Backend(format!("encode query: {e}")))?;
    Ok(QueryHash::of(&bytes))
}

#[doc(hidden)]
/// Hash cacheable facet fields.
pub fn hash_facet_for_cache(
    filter: Option<&String>,
    sorted_fields: &[String],
    top: usize,
) -> Result<QueryHash, HevSearchError> {
    #[derive(Serialize)]
    struct Canonical<'a> {
        kind: &'static str,
        filter: Option<&'a String>,
        fields: &'a [String],
        top: usize,
    }
    let canonical = Canonical {
        kind: "facet",
        filter,
        fields: sorted_fields,
        top,
    };
    let bytes = bincode::serde::encode_to_vec(&canonical, config::standard())
        .map_err(|e| HevSearchError::Backend(format!("encode facet key: {e}")))?;
    Ok(QueryHash::of(&bytes))
}

// Query payloads are cached as JSON, not bincode: `RowId` is
// `#[serde(untagged)]` (string ids, RFC 0005), and untagged enums need a
// self-describing format — bincode encodes them but can never decode them
// back, which silently turned every exact-cache hit into a decode-miss
// (one extra backend round-trip per repeated query). Old bincode NVMe
// entries decode-fail once and self-heal through the same fall-through.
fn encode_payload(result: &QueryResultSet) -> Result<Vec<u8>, HevSearchError> {
    serde_json::to_vec(result).map_err(|e| HevSearchError::Backend(format!("encode result: {e}")))
}

fn decode_payload(bytes: &[u8]) -> Result<QueryResultSet, HevSearchError> {
    serde_json::from_slice(bytes)
        .map_err(|e| HevSearchError::Backend(format!("decode result: {e}")))
}

// JSON for the same reason as `encode_payload`: facet bucket values are
// `serde_json::Value`, which needs a self-describing format to decode.
fn encode_facet_payload(result: &FacetResultSet) -> Result<Vec<u8>, HevSearchError> {
    serde_json::to_vec(result)
        .map_err(|e| HevSearchError::Backend(format!("encode facet result: {e}")))
}

fn decode_facet_payload(bytes: &[u8]) -> Result<FacetResultSet, HevSearchError> {
    serde_json::from_slice(bytes)
        .map_err(|e| HevSearchError::Backend(format!("decode facet result: {e}")))
}

/// Compute the `query_type` label exactly the same way the previous
/// implementation did. Lifted out so exact-cache and backend paths
/// can both attribute the same label.
fn classify_query_type(req: &QueryRequest) -> &'static str {
    let has_single = !req.vector.is_empty();
    let has_multi = req.vectors.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    let has_text = req.text.is_some();
    match (has_single, has_multi, has_text) {
        (true, _, true) | (_, true, true) => "hybrid",
        (true, _, false) => "vector",
        (_, true, false) => "multivector",
        (false, false, true) => "fts",
        (false, false, false) => "vector",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_with_filter(filter: Option<&str>) -> QueryRequest {
        QueryRequest {
            vector: vec![1.0, 0.0, 0.0],
            vectors: None,
            k: 10,
            nprobes: None,
            exact: false,
            text: None,
            fuzzy: None,
            filter: filter.map(str::to_string),
            include_vector: true,
        }
    }

    #[test]
    fn filter_changes_cache_key() {
        let unfiltered = hash_query_for_cache(&req_with_filter(None)).unwrap();
        let lt = hash_query_for_cache(&req_with_filter(Some("id < 5"))).unwrap();
        let gt = hash_query_for_cache(&req_with_filter(Some("id > 5"))).unwrap();

        assert_ne!(unfiltered, lt);
        assert_ne!(unfiltered, gt);
        assert_ne!(lt, gt);
    }
}
