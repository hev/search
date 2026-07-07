//! hevsearch-core — tiered storage primitives for hevsearch.
//!
//! This crate hosts the foyer-backed cache layer, the namespace
//! manager, and the LanceDB wrapper. It is consumed by
//! `hevsearch-api` and `hevsearch-bench`.

#![warn(missing_docs)]

pub mod analyzer;
pub mod cache;
pub mod error;
pub mod manager;
pub mod metrics;
pub mod namespace;
pub mod object_cache;
pub mod query;
pub mod result;
pub mod service;
pub mod storage_root;
pub mod vector;

pub use error::HevSearchError;
pub use manager::{
    decode_list_cursor, encode_list_cursor, validate_arrow_import_schema,
    validate_scalar_index_column, CompactResult, NamespaceManager, UpsertRow, LIST_MAX_LIMIT,
};
pub use metrics::CoreMetrics;
pub use namespace::NamespaceId;
pub use query::{
    effective_semantic_threshold, validate_ivf_pq_options, validate_semantic_cache_request,
    FacetRequest, FuzzyMaxEditDistance, FuzzyRequest, IndexRequest, QueryRequest,
    SemanticCacheRequest, DEFAULT_SEMANTIC_MIN_SIMILARITY,
};
pub use result::{
    DistanceMetric, FacetBucket, FacetField, FacetResultSet, ListOrder, ListPage, ListRow,
    NamespaceInfo, QueryResult, QueryResultSet, RowId, RowIdType,
};
pub use service::{NamespaceService, QueryCacheSource, QueryOutcome};
pub use storage_root::{resolve_s3_region, Scheme, StorageRoot};
pub use vector::VectorKind;
