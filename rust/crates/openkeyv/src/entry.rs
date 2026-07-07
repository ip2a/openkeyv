use crate::value::Value;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A managed cache entry containing value data and TTL metadata.
///
/// All values stored in backends are wrapped in this structure to enable
/// consistent TTL tracking and expiration handling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedEntry {
    pub value: Value,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

impl ManagedEntry {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            created_at: Some(Utc::now()),
            expires_at: None,
        }
    }

    pub fn with_ttl(value: Value, ttl_secs: f64) -> Self {
        let created_at = Utc::now();
        let expires_at =
            Some(created_at + chrono::TimeDelta::milliseconds((ttl_secs * 1000.0) as i64));
        Self {
            value,
            created_at: Some(created_at),
            expires_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            None => false,
            Some(expires) => expires <= Utc::now(),
        }
    }

    /// Returns remaining TTL in seconds, or None if no expiration.
    pub fn ttl(&self) -> Option<f64> {
        match self.expires_at {
            None => None,
            Some(expires) => {
                let now = Utc::now();
                if expires <= now {
                    Some(0.0)
                } else {
                    Some((expires - now).num_milliseconds() as f64 / 1000.0)
                }
            }
        }
    }

    pub fn estimate_size(&self) -> usize {
        self.value.len() + std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_no_ttl() {
        let entry = ManagedEntry::new(Value::null());
        assert!(!entry.is_expired());
        assert!(entry.ttl().is_none());
    }

    #[test]
    fn test_entry_with_ttl() {
        let entry = ManagedEntry::with_ttl(Value::null(), 3600.0);
        assert!(!entry.is_expired());
        assert!(entry.ttl().unwrap() > 3500.0);
    }

    #[test]
    fn test_entry_expired() {
        let mut entry = ManagedEntry::new(Value::null());
        entry.expires_at = Some(Utc::now() - chrono::TimeDelta::seconds(1));
        assert!(entry.is_expired());
        assert_eq!(entry.ttl(), Some(0.0));
    }
}
