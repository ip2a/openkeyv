use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// A wrapper that rejects values outside a size range.
pub struct LimitSizeWrapper<T: AsyncKeyValue> {
    inner: T,
    min_size: Option<usize>,
    max_size: Option<usize>,
}

impl<T: AsyncKeyValue> LimitSizeWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            min_size: None,
            max_size: None,
        }
    }

    pub fn with_min(inner: T, min_size: usize) -> Self {
        Self {
            inner,
            min_size: Some(min_size),
            max_size: None,
        }
    }

    pub fn with_max(inner: T, max_size: usize) -> Self {
        Self {
            inner,
            min_size: None,
            max_size: Some(max_size),
        }
    }

    pub fn with_range(inner: T, min_size: usize, max_size: usize) -> Self {
        Self {
            inner,
            min_size: Some(min_size),
            max_size: Some(max_size),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn check_size(&self, value: &HashMap<String, Value>) -> Result<()> {
        let entry = ManagedEntry::new(value.clone());
        let size = entry.estimate_size();

        if let Some(min) = self.min_size {
            if size < min {
                return Err(Error::EntryTooSmall);
            }
        }
        if let Some(max) = self.max_size {
            if size > max {
                return Err(Error::EntryTooLarge);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for LimitSizeWrapper<T> {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        self.inner.get(key, collection).await
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        self.inner.ttl(key, collection).await
    }

    async fn put(
        &self,
        key: &str,
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.check_size(&value)?;
        self.inner.put(key, value, collection, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        self.inner.get_many(keys, collection).await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        self.inner.ttl_many(keys, collection).await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[HashMap<String, Value>],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        for value in values {
            self.check_size(value)?;
        }
        self.inner.put_many(keys, values, collection, ttl).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.inner.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for LimitSizeWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for LimitSizeWrapper<T>
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
    async fn test_limit_size_too_large() {
        let mem = MemoryStore::new();
        let wrapper = LimitSizeWrapper::with_max(mem, 10);

        let mut value = HashMap::new();
        value.insert(
            "key".to_string(),
            Value::String("this is a very long string that exceeds ten bytes".to_string()),
        );

        let err = wrapper.put("k", value, None, None).await.unwrap_err();
        assert_eq!(err, Error::EntryTooLarge);
    }

    #[tokio::test]
    async fn test_limit_size_within_range() {
        let mem = MemoryStore::new();
        let wrapper = LimitSizeWrapper::with_range(mem, 1, 1000);

        let mut value = HashMap::new();
        value.insert("key".to_string(), Value::String("short".to_string()));

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }
}
