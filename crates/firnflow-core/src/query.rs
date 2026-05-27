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
    /// PQ codebook bit width. `Some(4)` halves the per-vector index
    /// storage cost relative to the 8-bit default at the cost of
    /// some recall. Accepted values are 4 or 8; omitting keeps
    /// Lance's 8-bit default.
    ///
    /// `Some(4)` additionally requires `num_sub_vectors` to be even
    /// (Lance 6 rejects 4-bit PQ over an odd sub-vector count).
    #[serde(default)]
    pub num_bits: Option<u32>,
}

fn default_index_kind() -> String {
    "ivf_pq".into()
}

/// Validate the IVF_PQ tuning options carried by an [`IndexRequest`].
///
/// Pure: no I/O. Two intended call sites: the API handler calls
/// this synchronously before spawning the index build so a bad
/// payload returns 400 immediately, and
/// [`crate::NamespaceManager::create_index`] calls it before the
/// `IvfPqIndexBuilder` step so direct callers (benches, integration
/// tests) bypassing the API are still protected.
///
/// Checks:
/// - `num_bits` ∈ {`None`, `Some(4)`, `Some(8)`}. Lance only
///   supports 4 and 8.
/// - `num_bits == Some(4)` combined with an explicit odd
///   `num_sub_vectors` is rejected. Lance 6 requires the sub-vector
///   count to be even for 4-bit PQ.
pub fn validate_ivf_pq_options(
    num_bits: Option<u32>,
    num_sub_vectors: Option<u32>,
) -> Result<(), crate::FirnflowError> {
    if let Some(bits) = num_bits {
        if bits != 4 && bits != 8 {
            return Err(crate::FirnflowError::InvalidRequest(format!(
                "num_bits={bits} is not supported; accepted values are 4 or 8"
            )));
        }
        if bits == 4 {
            if let Some(n) = num_sub_vectors {
                if n % 2 != 0 {
                    return Err(crate::FirnflowError::InvalidRequest(format!(
                        "num_bits=4 requires num_sub_vectors to be even (got {n})"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FirnflowError;

    #[test]
    fn validate_accepts_none_and_supported_widths() {
        assert!(validate_ivf_pq_options(None, None).is_ok());
        assert!(validate_ivf_pq_options(None, Some(63)).is_ok());
        assert!(validate_ivf_pq_options(Some(4), None).is_ok());
        assert!(validate_ivf_pq_options(Some(4), Some(64)).is_ok());
        assert!(validate_ivf_pq_options(Some(8), Some(63)).is_ok());
    }

    #[test]
    fn validate_rejects_unsupported_bit_width() {
        let err = validate_ivf_pq_options(Some(7), None).unwrap_err();
        match err {
            FirnflowError::InvalidRequest(msg) => {
                assert!(msg.contains("num_bits=7"), "{msg}");
                assert!(msg.contains("4 or 8"), "{msg}");
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_4bit_with_odd_sub_vectors() {
        let err = validate_ivf_pq_options(Some(4), Some(63)).unwrap_err();
        match err {
            FirnflowError::InvalidRequest(msg) => {
                assert!(msg.contains("num_bits=4"), "{msg}");
                assert!(msg.contains("even"), "{msg}");
                assert!(msg.contains("63"), "{msg}");
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn validate_allows_4bit_with_unspecified_sub_vectors() {
        // num_sub_vectors omitted means "use Lance's default" — the
        // default is dim/16 which is even for any vector dim that is
        // a multiple of 32 (the realistic case). Don't reject here.
        assert!(validate_ivf_pq_options(Some(4), None).is_ok());
    }
}
