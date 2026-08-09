use crate::change::{ChangeFeedRequest, ChangeStream};
use crate::error::Result;
use crate::value::Value;
use async_trait::async_trait;

/// Opaque revision token returned by stores with atomic conditional-write support.
///
/// Revisions may be compared for equality, but their bytes do not carry ordering,
/// timestamp, or application-version semantics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Revision([u8; Self::BYTE_LEN]);

impl Revision {
    pub const BYTE_LEN: usize = 16;

    pub const fn from_bytes(bytes: [u8; Self::BYTE_LEN]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; Self::BYTE_LEN] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; Self::BYTE_LEN] {
        self.0
    }

    /// Generate a fresh opaque revision token from operating-system randomness.
    ///
    /// Per the CAS/revision ADR (section 5), every successful write candidate on a
    /// CAS-capable store receives a fresh 16-byte token drawn from the OS random
    /// source. This must be called before any backend mutation so that a
    /// randomness failure cannot leave a partial write behind. The bytes carry no
    /// ordering, timestamp, or business-version meaning.
    pub fn fresh() -> Result<Revision> {
        let mut bytes = [0u8; Self::BYTE_LEN];
        getrandom::fill(&mut bytes)
            .map_err(|err| crate::error::Error::RevisionGeneration(err.to_string()))?;
        Ok(Revision(bytes))
    }
}

/// Value and revision observed from the same atomic store entry.
#[derive(Clone, Debug, PartialEq)]
pub struct RevisionedValue {
    pub value: Value,
    pub revision: Revision,
    pub ttl: Option<f64>,
}

/// Result of an atomic conditional write.
#[derive(Clone, Debug, PartialEq)]
pub enum CompareAndSwapResult {
    Applied { revision: Revision },
    Conflict { current: Option<RevisionedValue> },
}

/// Result of an atomic conditional delete.
#[derive(Clone, Debug, PartialEq)]
pub enum CompareAndDeleteResult {
    Deleted,
    Conflict { current: Option<RevisionedValue> },
}

/// Core async key-value protocol.
///
/// All store implementations and wrappers must implement this trait.
/// Values are Rust-native typed bytes. Language-specific object conversion belongs
/// at the boundary layer, not in this protocol.
#[async_trait]
pub trait AsyncKeyValue: Send + Sync {
    /// Retrieve a value by key from the specified collection.
    /// Returns `None` if the key is not found or has expired.
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>>;

    /// Retrieve the value and remaining TTL for a key.
    /// Returns `None` if the key is not found or expired. The nested TTL is
    /// `None` when the value does not expire.
    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>>;

    /// Store a key-value pair with optional TTL (in seconds).
    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()>;

    /// Delete a key. Returns `true` if the key existed and was deleted.
    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool>;

    /// Retrieve multiple values by key.
    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>>;

    /// Retrieve multiple values and their TTLs. A missing outer value means the
    /// key is absent; a missing nested TTL means the value does not expire.
    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>>;

    /// Store multiple key-value pairs with the same optional TTL.
    ///
    /// `keys` and `values` must have the same length. Implementations may
    /// perform partial writes when the backend reports an error, but a
    /// successful result means every key/value pair was accepted.
    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()>;

    /// Delete multiple keys. Returns the number of keys deleted.
    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize>;
}

/// The minimum store contract shared by every OpenKeyv store.
#[async_trait]
pub trait BaseStore: AsyncKeyValue + Send + Sync {
    fn store_name(&self) -> &'static str;
}

impl<T> BaseStore for T
where
    T: AsyncKeyValue + Send + Sync,
{
    fn store_name(&self) -> &'static str {
        std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("store")
    }
}

/// Optional protocol for stores with genuinely atomic conditional writes.
///
/// `expected = None` means create-if-absent. Implementations must not emulate
/// this capability with a non-atomic read followed by a write.
#[async_trait]
pub trait AsyncCompareAndSwap: Send + Sync {
    /// Atomically retrieve a live value and its opaque revision.
    async fn get_with_revision(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<RevisionedValue>>;

    /// Store a value only when the expected revision matches.
    ///
    /// A missing expected revision creates only when the key is absent or expired.
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&Revision>,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<CompareAndSwapResult>;

    /// Delete a value only when the expected revision matches.
    async fn compare_and_delete(
        &self,
        key: &str,
        expected: &Revision,
        collection: Option<&str>,
    ) -> Result<CompareAndDeleteResult>;
}

/// Protocol for stores that support culling (removing expired entries).
#[async_trait]
pub trait AsyncCull: Send + Sync {
    async fn cull(&self) -> Result<()>;
}

/// Protocol for enumerating keys within a collection.
#[async_trait]
pub trait AsyncEnumerateKeys: Send + Sync {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>>;
}

/// Protocol for enumerating collections.
#[async_trait]
pub trait AsyncEnumerateCollections: Send + Sync {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>>;
}

/// Protocol for destroying an entire store.
#[async_trait]
pub trait AsyncDestroyStore: Send + Sync {
    async fn destroy(&self) -> Result<bool>;
}

/// Protocol for destroying a single collection.
#[async_trait]
pub trait AsyncDestroyCollection: Send + Sync {
    async fn destroy_collection(&self, collection: &str) -> Result<bool>;
}

/// Protocol for stores that expose an ordered, resumable mutation feed.
#[async_trait]
pub trait AsyncChangeFeed: Send + Sync {
    async fn subscribe(&self, request: ChangeFeedRequest) -> Result<Box<dyn ChangeStream + Send>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn revision_has_fixed_opaque_bytes() {
        let bytes = [0xA5; Revision::BYTE_LEN];
        let revision = Revision::from_bytes(bytes);

        assert_eq!(revision.as_bytes(), &bytes);
        assert_eq!(revision.into_bytes(), bytes);
        assert_eq!(HashSet::from([revision]).len(), 1);
    }

    #[test]
    fn conditional_results_preserve_revisions_and_current_values() {
        let revision = Revision::from_bytes([1; Revision::BYTE_LEN]);
        let current = RevisionedValue {
            value: Value::utf8("current"),
            revision,
            ttl: Some(5.0),
        };

        match (CompareAndSwapResult::Applied { revision }, revision) {
            (CompareAndSwapResult::Applied { revision: actual }, expected) => {
                assert_eq!(actual, expected)
            }
            (result, _) => panic!("unexpected CAS result: {result:?}"),
        }

        let conflict = CompareAndSwapResult::Conflict {
            current: Some(current.clone()),
        };
        match conflict {
            CompareAndSwapResult::Conflict {
                current: Some(actual),
            } => {
                assert_eq!(actual.value, current.value);
                assert_eq!(actual.revision, current.revision);
                assert_eq!(actual.ttl, current.ttl);
            }
            result => panic!("unexpected CAS result: {result:?}"),
        }

        match (CompareAndDeleteResult::Conflict {
            current: Some(current.clone()),
        }) {
            CompareAndDeleteResult::Conflict {
                current: Some(actual),
            } => {
                assert_eq!(actual.value, current.value);
                assert_eq!(actual.revision, current.revision);
                assert_eq!(actual.ttl, current.ttl);
            }
            result => panic!("unexpected conditional-delete result: {result:?}"),
        }

        assert!(matches!(
            CompareAndDeleteResult::Deleted,
            CompareAndDeleteResult::Deleted
        ));
    }

    #[test]
    fn compare_and_swap_trait_is_object_safe() {
        fn accept(_: Option<&dyn AsyncCompareAndSwap>) {}

        accept(None);
    }
}
