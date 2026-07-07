use crate::error::{Error, Result};
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;
use std::time::Duration;
use tokio::time::timeout;

/// A wrapper that enforces a maximum duration on every operation.
pub struct TimeoutWrapper<T: AsyncKeyValue> {
    inner: T,
    duration: Duration,
}

impl<T: AsyncKeyValue> TimeoutWrapper<T> {
    pub fn new(inner: T, duration: Duration) -> Self {
        Self { inner, duration }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for TimeoutWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        match timeout(self.duration, self.inner.get(key, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        match timeout(self.duration, self.inner.ttl(key, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        match timeout(self.duration, self.inner.put(key, value, collection, ttl)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        match timeout(self.duration, self.inner.delete(key, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        match timeout(self.duration, self.inner.get_many(keys, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        match timeout(self.duration, self.inner.ttl_many(keys, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        match timeout(
            self.duration,
            self.inner.put_many(keys, values, collection, ttl),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        match timeout(self.duration, self.inner.delete_many(keys, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for TimeoutWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        match timeout(self.duration, self.inner.keys(collection, limit)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for TimeoutWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        match timeout(self.duration, self.inner.collections(limit)).await {
            Ok(result) => result,
            Err(_) => Err(Error::InvalidOperation("operation timed out".to_string())),
        }
    }
}
