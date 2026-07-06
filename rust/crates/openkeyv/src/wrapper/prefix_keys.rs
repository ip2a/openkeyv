use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// A wrapper that prefixes all keys before delegating to the underlying store.
pub struct PrefixKeysWrapper<T: AsyncKeyValue> {
    inner: T,
    prefix: String,
}

impl<T: AsyncKeyValue> PrefixKeysWrapper<T> {
    pub fn new(inner: T, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn prefixed(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for PrefixKeysWrapper<T> {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        self.inner.get(&self.prefixed(key), collection).await
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        self.inner.ttl(&self.prefixed(key), collection).await
    }

    async fn put(
        &self,
        key: &str,
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.inner
            .put(&self.prefixed(key), value, collection, ttl)
            .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner.delete(&self.prefixed(key), collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let prefixed: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
        self.inner.get_many(&prefixed, collection).await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let prefixed: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
        self.inner.ttl_many(&prefixed, collection).await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[HashMap<String, Value>],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let prefixed: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
        self.inner
            .put_many(&prefixed, values, collection, ttl)
            .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let prefixed: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
        self.inner.delete_many(&prefixed, collection).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for PrefixKeysWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let keys = self.inner.keys(collection, limit).await?;
        Ok(keys
            .into_iter()
            .filter_map(|key| key.strip_prefix(&self.prefix).map(str::to_string))
            .collect())
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for PrefixKeysWrapper<T>
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
    async fn test_prefix_keys() {
        let mem = MemoryStore::new();
        let wrapper = PrefixKeysWrapper::new(mem, "app:");

        let mut value = HashMap::new();
        value.insert("a".to_string(), Value::String("b".to_string()));
        wrapper.put("k", value.clone(), None, None).await.unwrap();

        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));

        // Verify it's stored with prefix
        let raw = wrapper.into_inner();
        assert!(raw.get("app:k", None).await.unwrap().is_some());
    }
}
