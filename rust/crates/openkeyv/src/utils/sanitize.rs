use crate::error::{Error, Result};
use std::borrow::Cow;

/// Strategy for sanitizing and validating keys/collections.
pub trait SanitizationStrategy: Send + Sync {
    /// Validate that a value is acceptable. Returns an error if not.
    fn validate(&self, value: &str) -> Result<()>;

    /// Sanitize a value for storage. Returns the sanitized string.
    fn sanitize<'a>(&self, value: &'a str) -> Cow<'a, str>;
}

/// Pass-through strategy: does not modify values.
pub struct PassthroughStrategy;

impl SanitizationStrategy for PassthroughStrategy {
    fn validate(&self, _value: &str) -> Result<()> {
        Ok(())
    }

    fn sanitize<'a>(&self, value: &'a str) -> Cow<'a, str> {
        Cow::Borrowed(value)
    }
}

/// Rejects values that contain invalid characters.
pub struct CharacterBlacklistStrategy {
    invalid_chars: Vec<char>,
}

impl CharacterBlacklistStrategy {
    pub fn new(invalid_chars: Vec<char>) -> Self {
        Self { invalid_chars }
    }
}

impl SanitizationStrategy for CharacterBlacklistStrategy {
    fn validate(&self, value: &str) -> Result<()> {
        if let Some(ch) = value.chars().find(|c| self.invalid_chars.contains(c)) {
            return Err(Error::InvalidKey(format!(
                "character '{}' is not allowed",
                ch
            )));
        }
        Ok(())
    }

    fn sanitize<'a>(&self, value: &'a str) -> Cow<'a, str> {
        let sanitized: String = value
            .chars()
            .filter(|c| !self.invalid_chars.contains(c))
            .collect();
        if sanitized.len() == value.len() {
            Cow::Borrowed(value)
        } else {
            Cow::Owned(sanitized)
        }
    }
}

/// Hashes the excess length beyond a maximum, producing a compound key.
pub struct HashExcessLengthStrategy {
    max_length: usize,
}

impl HashExcessLengthStrategy {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }
}

impl SanitizationStrategy for HashExcessLengthStrategy {
    fn validate(&self, value: &str) -> Result<()> {
        if value.is_empty() {
            return Err(Error::InvalidKey("empty value".to_string()));
        }
        Ok(())
    }

    fn sanitize<'a>(&self, value: &'a str) -> Cow<'a, str> {
        if value.len() <= self.max_length {
            return Cow::Borrowed(value);
        }
        let prefix = &value[..self.max_length];
        let suffix = &value[self.max_length..];
        let hash = blake3::hash(suffix.as_bytes());
        Cow::Owned(format!("{}_{}", prefix, hash.to_hex()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passthrough() {
        let s = PassthroughStrategy;
        assert!(s.validate("hello/world").is_ok());
        assert_eq!(s.sanitize("hello"), "hello");
    }

    #[test]
    fn test_blacklist() {
        let s = CharacterBlacklistStrategy::new(vec!['/', '\\']);
        assert!(s.validate("hello/world").is_err());
        assert_eq!(s.sanitize("hello/world"), "helloworld");
    }
}
