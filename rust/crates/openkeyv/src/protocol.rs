use crate::error::Result;
use crate::value::Value;
use async_trait::async_trait;

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
    /// Returns `(None, None)` if the key is not found or expired.
    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>>;

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

    /// Retrieve multiple values and their TTLs.
    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>>;

    /// Store multiple key-value pairs with the same optional TTL.
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
