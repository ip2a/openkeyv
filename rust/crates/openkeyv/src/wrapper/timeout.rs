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
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        match timeout(self.duration, self.inner.ttl(key, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Timeout),
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
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        match timeout(self.duration, self.inner.delete(key, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        match timeout(self.duration, self.inner.get_many(keys, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        match timeout(self.duration, self.inner.ttl_many(keys, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Timeout),
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
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        match timeout(self.duration, self.inner.delete_many(keys, collection)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Timeout),
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
            Err(_) => Err(Error::Timeout),
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
            Err(_) => Err(Error::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::AsyncKeyValue;

    struct SlowStore;

    #[async_trait]
    impl AsyncKeyValue for SlowStore {
        async fn get(&self, _key: &str, _collection: Option<&str>) -> Result<Option<Value>> {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(None)
        }

        async fn ttl(
            &self,
            _key: &str,
            _collection: Option<&str>,
        ) -> Result<Option<(Value, Option<f64>)>> {
            Ok(None)
        }

        async fn put(
            &self,
            _key: &str,
            _value: Value,
            _collection: Option<&str>,
            _ttl: Option<f64>,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, _key: &str, _collection: Option<&str>) -> Result<bool> {
            Ok(false)
        }

        async fn get_many(
            &self,
            _keys: &[String],
            _collection: Option<&str>,
        ) -> Result<Vec<Option<Value>>> {
            Ok(Vec::new())
        }

        async fn ttl_many(
            &self,
            _keys: &[String],
            _collection: Option<&str>,
        ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
            Ok(Vec::new())
        }

        async fn put_many(
            &self,
            _keys: &[String],
            _values: &[Value],
            _collection: Option<&str>,
            _ttl: Option<f64>,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_many(&self, _keys: &[String], _collection: Option<&str>) -> Result<usize> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn timeout_wrapper_returns_typed_timeout_error() {
        let wrapper = TimeoutWrapper::new(SlowStore, Duration::from_millis(1));

        let error = wrapper.get("key", None).await.unwrap_err();

        assert_eq!(error, Error::Timeout);
    }
}
