//! Vector payload kinds.
//!
//! A namespace is either *single-vector* (one dense vector per row,
//! stored as `FixedSizeList<Float32, dim>`) or *multivector* (a
//! variable-length bag of fixed-dimension sub-vectors per row, stored
//! as `List<FixedSizeList<Float32, dim>>`). The latter shape is what
//! lancedb uses for ColBERT-style late-interaction retrieval — Lance
//! dispatches MaxSim scoring automatically when it sees the nested
//! column type.
//!
//! The kind is determined by the *shape* of the first upsert payload
//! and is immutable thereafter. Subsequent payloads whose shape does
//! not match are rejected at the API boundary.

use serde::{Deserialize, Serialize};

/// The vector representation used by a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VectorKind {
    /// One dense vector per row. Column type:
    /// `FixedSizeList<Float32, dim>`. Distance metric is whatever the
    /// index builder was configured with (today L2).
    Single,
    /// A bag of small vectors per row, scored by late-interaction
    /// MaxSim. Column type: `List<FixedSizeList<Float32, dim>>`.
    /// Distance metric is forced to cosine by Lance — the constraint
    /// is enforced at the manager boundary so non-cosine requests
    /// fail with a clear error before reaching Lance.
    Multivector,
}

impl VectorKind {
    /// Stable label suitable for log fields and Prometheus label
    /// values. The single-vector value is `"single"`; the multivector
    /// value is `"multivector"`.
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Multivector => "multivector",
        }
    }
}
