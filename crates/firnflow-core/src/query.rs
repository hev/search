//! Query and index request types.
//!
//! Request payloads are kept in their own module so that `result.rs`
//! stays focused on the response side. Both modules share the same
//! serde derives and are what the axum handlers parse straight from
//! request bodies.

use serde::{Deserialize, Serialize};

/// Default number of IVF partitions to probe per query when an
/// index exists. Matches lancedb's own default (20).
pub const DEFAULT_NPROBES: usize = 20;

/// Parameters of a search query.
///
/// Supports three query modes depending on which fields are set:
/// - **Vector-only**: `vector` or `vectors` set, `text` absent →
///   nearest-neighbour search.
/// - **FTS-only**: `text` set, both vector fields absent → BM25
///   full-text search.
/// - **Hybrid**: a vector field and `text` both set → combined
///   vector + FTS via Reciprocal Rank Fusion.
///
/// The vector payload field is determined by the namespace's kind:
/// - **Single-vector namespaces** accept `vector: [f32, ...]`.
/// - **Multivector namespaces** accept
///   `vectors: [[f32, ...], [f32, ...]]` — a bag of small vectors,
///   each of the namespace's inner dimension.
///
/// At most one of `vector` / `vectors` may be set; setting both
/// returns 400. The payload shape must match the namespace's kind
/// (a single-vector namespace receiving a `vectors` payload, or
/// vice-versa, returns 400 with the expected shape spelled out).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// The single-vector query payload. Length must match the
    /// namespace's dimension. Empty / absent means "no single vector".
    /// Mutually exclusive with [`vectors`](Self::vectors).
    #[serde(default)]
    pub vector: Vec<f32>,
    /// The multivector query payload. Each inner vector must have
    /// the namespace's inner sub-vector dimension; the outer list
    /// length is the per-query sub-vector count. `None` means "no
    /// multivector". Mutually exclusive with [`vector`](Self::vector).
    #[serde(default)]
    pub vectors: Option<Vec<Vec<f32>>>,
    /// Maximum number of results to return.
    pub k: usize,
    /// Number of IVF partitions to probe. Only meaningful when an
    /// index exists; ignored for linear scans. Defaults to 20 if
    /// omitted.
    #[serde(default)]
    pub nprobes: Option<usize>,
    /// Full-text search query string. When set alongside a vector
    /// field, triggers hybrid search (vector + FTS combined via RRF).
    /// When set without any vector field, triggers FTS-only search.
    #[serde(default)]
    pub text: Option<String>,
}

/// Parameters for an explicit index build request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexRequest {
    /// Index type. Currently only `"ivf_pq"` is supported.
    #[serde(default = "default_index_kind")]
    pub kind: String,
    /// Number of IVF partitions. Defaults to `sqrt(row_count)` if
    /// omitted.
    pub num_partitions: Option<u32>,
    /// Number of PQ sub-vectors. Defaults to `dim / 16` if omitted.
    pub num_sub_vectors: Option<u32>,
}

fn default_index_kind() -> String {
    "ivf_pq".into()
}
