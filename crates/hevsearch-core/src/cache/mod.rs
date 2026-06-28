//! Cache layer for hevsearch.
//!
//! A single foyer `HybridCache` (RAM + NVMe) is shared across all
//! namespaces, with a per-namespace generation counter to make
//! invalidation on writes O(1). The generation-counter approach
//! avoids the memory-growth cost of tracking every live cache key
//! explicitly, has no race window between cache population and key
//! registration, and is O(1) on write regardless of how many cached
//! queries exist for a namespace.

mod invalidation;
mod key;
mod layer;
mod semantic;

pub use invalidation::GenerationCounter;
pub use key::{CacheKey, QueryHash};
pub use layer::NamespaceCache;
pub use semantic::{SemanticCache, SemanticLookup, SEMANTIC_CACHE_MAX_PER_NAMESPACE};
