use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::utils::retry::retry_operation;
use crate::value::Value;
use async_trait::async_trait;

/// A wrapper that retries failed operations with exponential backoff.
pub struct RetryWrapper<T: AsyncKeyValue> {
    inner: T,
    max_attempts: usize,
    base_delay_ms: u64,
    max_delay_ms: u64,
}

impl<T: AsyncKeyValue> RetryWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            max_attempts: 3,
            base_delay_ms: 100,
            max_delay_ms: 5000,
        }
    }

    pub fn with_config(
        inner: T,
        max_attempts: usize,
        base_delay_ms: u64,
        max_delay_ms: u64,
    ) -> Self {
        Self {
            inner,
            max_attempts,
            base_delay_ms,
            max_delay_ms,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for RetryWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        retry_operation(
            || self.inner.get(key, collection),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        retry_operation(
            || self.inner.ttl(key, collection),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        retry_operation(
            || self.inner.put(key, value.clone(), collection, ttl),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        retry_operation(
            || self.inner.delete(key, collection),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        retry_operation(
            || self.inner.get_many(keys, collection),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        retry_operation(
            || self.inner.ttl_many(keys, collection),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        retry_operation(
            || self.inner.put_many(keys, values, collection, ttl),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        retry_operation(
            || self.inner.delete_many(keys, collection),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for RetryWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        retry_operation(
            || self.inner.keys(collection, limit),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for RetryWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        retry_operation(
            || self.inner.collections(limit),
            self.max_attempts,
            self.base_delay_ms,
            self.max_delay_ms,
            |_e| true,
        )
        .await
    }
}
