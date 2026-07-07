//! Shared benchmark library for the hevsearch bench harness.
//!
//! The `recall` module (RFC 0011) owns dataset loading, exact-NN
//! ground truth, and recall/ndcg scoring so every bench bin — and
//! eventually the Layer-side store-vs-store twin — scores with the
//! same IR math instead of reimplementing it.

pub mod recall;
