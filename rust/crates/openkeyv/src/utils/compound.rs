//! Canonical compound identities for stores that do not natively support collections.

use crate::error::{Error, Result};

/// How collection enumeration treats scanned keys that do not decode as
/// openkeyv compound identities (foreign keys sharing the database).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ForeignKeyPolicy {
    /// Return an error when a scanned key fails to decode (previous behavior).
    #[default]
    Strict,
    /// Skip foreign keys and keep scanning; for shared-database deployments.
    Skip,
}

/// Return the canonical prefix for every key in `collection`.
///
/// The length is the collection's UTF-8 byte length, not its character count.
pub fn collection_prefix(collection: &str) -> String {
    format!("{}:{}", collection.len(), collection)
}

/// Encode an exact `(collection, key)` pair as one unambiguous identity.
pub fn compound_key(collection: &str, key: &str) -> String {
    let mut identity = collection_prefix(collection);
    identity.push_str(key);
    identity
}

/// Decode a canonical compound identity into its borrowed collection and key.
pub fn decompound_key(compound: &str) -> Result<(&str, &str)> {
    let (length, payload) = compound.split_once(':').ok_or_else(|| {
        Error::InvalidKey("compound identity is missing its length delimiter".to_string())
    })?;

    if length.is_empty() || !length.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::InvalidKey(
            "compound identity has an invalid collection length".to_string(),
        ));
    }
    if length.len() > 1 && length.starts_with('0') {
        return Err(Error::InvalidKey(
            "compound identity has a non-canonical collection length".to_string(),
        ));
    }

    let collection_len = length.parse::<usize>().map_err(|_| {
        Error::InvalidKey("compound identity collection length is too large".to_string())
    })?;
    let collection = payload.get(..collection_len).ok_or_else(|| {
        Error::InvalidKey(
            "compound identity collection length is outside a UTF-8 boundary".to_string(),
        )
    })?;

    Ok((collection, &payload[collection_len..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn canonical_identity_roundtrips_exact_strings() {
        for (collection, key) in [
            ("", ""),
            ("default", "key"),
            ("a:b", "c/d"),
            ("集合", "键🔑"),
            ("*?[\\]", ":/::"),
        ] {
            let identity = compound_key(collection, key);
            assert_eq!(decompound_key(&identity).unwrap(), (collection, key));
        }
    }

    #[test]
    fn canonical_identity_uses_utf8_byte_length() {
        let identity = compound_key("集合", "key");

        assert_eq!(identity, "6:集合key");
        assert_eq!(collection_prefix("集合"), "6:集合");
    }

    #[test]
    fn canonical_identity_distinguishes_collision_pairs() {
        let left = compound_key("a:b", "c");
        let right = compound_key("a", "b:c");

        assert_ne!(left, right);
        assert_eq!(decompound_key(&left).unwrap(), ("a:b", "c"));
        assert_eq!(decompound_key(&right).unwrap(), ("a", "b:c"));
    }

    #[test]
    fn canonical_identity_preserves_case_and_unicode_normalization() {
        assert_ne!(compound_key("Users", "Key"), compound_key("users", "Key"));
        assert_ne!(compound_key("é", "key"), compound_key("e\u{301}", "key"));
    }

    #[test]
    fn collection_prefix_matches_only_the_exact_collection_frame() {
        let prefix = collection_prefix("a:b");

        assert!(compound_key("a:b", "key").starts_with(&prefix));
        assert!(!compound_key("a", "b:key").starts_with(&prefix));
    }

    #[test]
    fn malformed_identity_is_rejected() {
        for identity in [
            "",
            ":key",
            "x:key",
            "01:akey",
            "2:a",
            "1:ékey",
            "999999999999999999999999999999999999999999999999999999999999999999999999999999:key",
        ] {
            assert!(matches!(
                decompound_key(identity),
                Err(Error::InvalidKey(_))
            ));
        }
    }
}
