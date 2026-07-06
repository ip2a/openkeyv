//! Compound key utilities for stores that don't natively support collections.

/// Join a collection and key into a single compound key.
pub fn compound_key(collection: &str, key: &str, separator: &str) -> String {
    format!("{}{}{}", collection, separator, key)
}

/// Split a compound key back into (collection, key).
pub fn decompound_key<'a>(compound: &'a str, separator: &'a str) -> Option<(&'a str, &'a str)> {
    let pos = compound.find(separator)?;
    Some((&compound[..pos], &compound[pos + separator.len()..]))
}

/// Prefix a key with a collection name.
pub fn prefix_key(collection: &str, key: &str, separator: &str) -> String {
    compound_key(collection, key, separator)
}

/// Remove a collection prefix from a key.
pub fn unprefix_key<'a>(prefixed: &'a str, collection: &str, separator: &str) -> Option<&'a str> {
    let expected_prefix = format!("{}{}", collection, separator);
    prefixed.strip_prefix(&expected_prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compound_and_decompound() {
        let c = compound_key("my_collection", "my_key", "::");
        assert_eq!(c, "my_collection::my_key");

        let (col, key) = decompound_key(&c, "::").unwrap();
        assert_eq!(col, "my_collection");
        assert_eq!(key, "my_key");
    }

    #[test]
    fn test_prefix_and_unprefix() {
        let prefixed = prefix_key("col", "key", ":");
        assert_eq!(prefixed, "col:key");
        assert_eq!(unprefix_key(&prefixed, "col", ":"), Some("key"));
        assert_eq!(unprefix_key(&prefixed, "other", ":"), None);
    }
}
