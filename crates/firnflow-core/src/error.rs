//! Error types for firnflow-core.

use thiserror::Error;

/// Top-level error type for firnflow-core operations.
#[derive(Debug, Error)]
pub enum FirnflowError {
    /// A cache backend operation failed (foyer, local NVMe device, …).
    #[error("cache backend error: {0}")]
    Cache(String),

    /// A storage backend operation failed (lancedb, object store, …).
    #[error("storage backend error: {0}")]
    Backend(String),

    /// Object storage redirected the request, which almost always
    /// means the configured region does not match the bucket's
    /// region. Carried as its own typed variant (rather than a
    /// `Backend(String)`) so the API layer can surface an actionable
    /// hint that names the configured region **without** echoing raw
    /// backend error text. `configured_region` is the S3 region
    /// firnflow resolved at startup, when known.
    #[error("object storage redirected the request; configured region {configured_region:?} likely does not match the bucket's region")]
    StorageRegionRedirect {
        /// The S3 region firnflow was configured with, if known.
        configured_region: Option<String>,
    },

    /// An I/O error (disk, network, filesystem, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The requested namespace name is invalid.
    #[error("invalid namespace name: {0:?}")]
    InvalidNamespace(String),

    /// A request payload failed validation (wrong vector dimension,
    /// malformed query, …).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// The requested operation or configuration is not supported by
    /// this build. Today the main caller is a namespace-level
    /// operation whose Lance schema pre-dates a feature (the
    /// `/list` endpoint surfaces this as HTTP 501); the variant
    /// stays available for any future scheme- or feature-gated
    /// rejection that needs the same shape.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A metrics registry or encoding operation failed.
    #[error("metrics error: {0}")]
    Metrics(String),
}
