//! Foyer-friendly key encoding for `lance_core::cache::InternalCacheKey`.
//!
//! Foyer's `StorageKey` bound demands `Hash + Eq + Send + Sync +
//! 'static + Debug + Serialize + DeserializeOwned`. `InternalCacheKey`
//! has the trait bounds but not the serde derives, and its
//! `type_name` field is `&'static str` which serde cannot round-trip
//! through `DeserializeOwned`. [`EncodedKey`] is the thin newtype
//! that bridges the gap.
//!
//! Encoding is `format!("{prefix}\x1f{type_name}\x1f{key}")` where
//! `\x1f` is the ASCII unit separator. The unit separator is the
//! conventional choice for non-printable field separation and
//! does not appear in any of the three component values Lance uses
//! in practice (URI prefixes, Rust type names, version numbers,
//! UUIDs). The `prefix` component leads so the encoded form
//! preserves the field order that
//! [`CacheBackend::invalidate_prefix`] operates on, leaving room
//! for a future foyer prefix-scan API to be useful without
//! re-encoding.

use std::hash::Hash;

use lance_core::cache::InternalCacheKey;
use serde::{Deserialize, Serialize};

/// Owned, serialisable rendering of an [`InternalCacheKey`].
///
/// Construct via [`EncodedKey::from`], pass to foyer's
/// `HybridCache<EncodedKey, Vec<u8>>` get / insert. The internal
/// `String` is the only field, so serialisation cost is one
/// allocation per encode and zero overhead through serde.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EncodedKey(String);

impl EncodedKey {
    /// The encoded string. Exposed mainly so tests can assert
    /// shape; production callers should not need to peek inside.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&InternalCacheKey> for EncodedKey {
    fn from(key: &InternalCacheKey) -> Self {
        // ASCII unit separator (\x1f) between components keeps the
        // three fields losslessly recoverable without escape
        // sequences. None of `prefix`, `type_name`, or `key`
        // contains \x1f in any Lance code path.
        Self(format!(
            "{prefix}\x1f{type_name}\x1f{key}",
            prefix = key.prefix(),
            type_name = key.type_name(),
            key = key.key(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn encodes_three_fields_with_unit_separator() {
        let k = InternalCacheKey::new(
            Arc::from("s3://bucket/dataset/"),
            Arc::from("42"),
            "Vec<IndexMetadata>",
        );
        let encoded = EncodedKey::from(&k);
        assert_eq!(
            encoded.as_str(),
            "s3://bucket/dataset/\x1fVec<IndexMetadata>\x1f42",
        );
    }

    #[test]
    fn distinct_keys_encode_distinctly() {
        let a = InternalCacheKey::new(Arc::from("a/"), Arc::from("1"), "T");
        let b = InternalCacheKey::new(Arc::from("a/"), Arc::from("1"), "U");
        let c = InternalCacheKey::new(Arc::from("a/"), Arc::from("2"), "T");
        let d = InternalCacheKey::new(Arc::from("b/"), Arc::from("1"), "T");
        let encoded: std::collections::HashSet<_> = [&a, &b, &c, &d]
            .iter()
            .map(|k| EncodedKey::from(*k))
            .collect();
        assert_eq!(
            encoded.len(),
            4,
            "every distinct key must encode distinctly"
        );
    }

    #[test]
    fn serde_round_trip() {
        let k = InternalCacheKey::new(Arc::from("p/"), Arc::from("k"), "T");
        let encoded = EncodedKey::from(&k);
        let bytes = bincode::serde::encode_to_vec(&encoded, bincode::config::standard()).unwrap();
        let (decoded, _): (EncodedKey, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, encoded);
    }
}
