use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;
use tracing::{debug, instrument};

/// A wrapper that logs all operations at debug level.
pub struct LoggingWrapper<T: AsyncKeyValue> {
    inner: T,
    name: String,
}

impl<T: AsyncKeyValue> LoggingWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            name: "kv".to_string(),
        }
    }

    pub fn with_name(inner: T, name: impl Into<String>) -> Self {
        Self {
            inner,
            name: name.into(),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for LoggingWrapper<T> {
    #[instrument(skip(self), fields(store = %self.name))]
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        debug!(key, collection, "get");
        let result = self.inner.get(key, collection).await;
        debug!(
            key,
            collection,
            found = result.is_ok() && result.as_ref().unwrap().is_some(),
            "get done"
        );
        result
    }

    #[instrument(skip(self, value), fields(store = %self.name))]
    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        debug!(key, collection, ttl, "put");
        self.inner.put(key, value, collection, ttl).await
    }

    #[instrument(skip(self), fields(store = %self.name))]
    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        debug!(key, collection, "delete");
        self.inner.delete(key, collection).await
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        self.inner.ttl(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        debug!(count = keys.len(), collection, "get_many");
        self.inner.get_many(keys, collection).await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        self.inner.ttl_many(keys, collection).await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        debug!(count = keys.len(), collection, ttl, "put_many");
        self.inner.put_many(keys, values, collection, ttl).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        debug!(count = keys.len(), collection, "delete_many");
        self.inner.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for LoggingWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        debug!(collection, ?limit, "keys");
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for LoggingWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        debug!(?limit, "collections");
        self.inner.collections(limit).await
    }
}
