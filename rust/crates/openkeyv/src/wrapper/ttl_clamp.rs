use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;

/// A wrapper that clamps TTL values to a configured range.
pub struct TtlClampWrapper<T: AsyncKeyValue> {
    inner: T,
    min_ttl: Option<f64>,
    max_ttl: Option<f64>,
    missing_ttl: Option<f64>,
}

impl<T: AsyncKeyValue> TtlClampWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            min_ttl: None,
            max_ttl: None,
            missing_ttl: None,
        }
    }

    pub fn with_min(inner: T, min_ttl: f64) -> Self {
        Self {
            inner,
            min_ttl: Some(min_ttl),
            max_ttl: None,
            missing_ttl: None,
        }
    }

    pub fn with_max(inner: T, max_ttl: f64) -> Self {
        Self {
            inner,
            min_ttl: None,
            max_ttl: Some(max_ttl),
            missing_ttl: None,
        }
    }

    pub fn with_range(inner: T, min_ttl: f64, max_ttl: f64) -> Self {
        Self {
            inner,
            min_ttl: Some(min_ttl),
            max_ttl: Some(max_ttl),
            missing_ttl: None,
        }
    }

    pub fn with_missing_ttl(inner: T, missing_ttl: f64) -> Self {
        Self {
            inner,
            min_ttl: None,
            max_ttl: None,
            missing_ttl: Some(missing_ttl),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn clamp_ttl(&self, ttl: Option<f64>) -> Option<f64> {
        let ttl = ttl.or(self.missing_ttl);
        match ttl {
            Some(seconds) => {
                let mut clamped = seconds;
                if let Some(min) = self.min_ttl {
                    clamped = clamped.max(min);
                }
                if let Some(max) = self.max_ttl {
                    clamped = clamped.min(max);
                }
                Some(clamped)
            }
            None => None,
        }
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for TtlClampWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        self.inner.get(key, collection).await
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        self.inner.ttl(key, collection).await
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let ttl = self.clamp_ttl(ttl);
        self.inner.put(key, value, collection, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        self.inner.get_many(keys, collection).await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        self.inner.ttl_many(keys, collection).await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let ttl = self.clamp_ttl(ttl);
        self.inner.put_many(keys, values, collection, ttl).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.inner.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for TtlClampWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for TtlClampWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.collections(limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    #[tokio::test]
    async fn test_ttl_clamp_max() {
        let mem = MemoryStore::new();
        let wrapper = TtlClampWrapper::with_max(mem, 10.0);

        let value = Value::null();
        wrapper
            .put("k", value.clone(), None, Some(100.0))
            .await
            .unwrap();

        let (_, ttl) = wrapper.ttl("k", None).await.unwrap().unwrap();
        assert!(ttl.unwrap() <= 10.0);
    }

    #[tokio::test]
    async fn test_ttl_clamp_min() {
        let mem = MemoryStore::new();
        let wrapper = TtlClampWrapper::with_min(mem, 10.0);

        let value = Value::null();
        wrapper
            .put("k", value.clone(), None, Some(1.0))
            .await
            .unwrap();

        let (_, ttl) = wrapper.ttl("k", None).await.unwrap().unwrap();
        assert!(ttl.unwrap() > 9.0);
    }

    #[tokio::test]
    async fn test_ttl_missing() {
        let mem = MemoryStore::new();
        let wrapper = TtlClampWrapper::with_missing_ttl(mem, 5.0);

        let value = Value::null();
        wrapper.put("k", value.clone(), None, None).await.unwrap();

        let (_, ttl) = wrapper.ttl("k", None).await.unwrap().unwrap();
        assert!(ttl.unwrap() > 4.0);
    }
}
