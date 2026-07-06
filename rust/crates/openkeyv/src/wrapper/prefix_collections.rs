use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// A wrapper that prefixes all collection names before delegating to the underlying store.
pub struct PrefixCollectionsWrapper<T: AsyncKeyValue> {
    inner: T,
    prefix: String,
}

impl<T: AsyncKeyValue> PrefixCollectionsWrapper<T> {
    pub fn new(inner: T, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn prefixed_collection(&self, collection: Option<&str>) -> String {
        match collection {
            Some(c) => format!("{}{}", self.prefix, c),
            None => format!("{}default_collection", self.prefix),
        }
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for PrefixCollectionsWrapper<T> {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        self.inner
            .get(key, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        self.inner
            .ttl(key, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn put(
        &self,
        key: &str,
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.inner
            .put(key, value, Some(&self.prefixed_collection(collection)), ttl)
            .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner
            .delete(key, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        self.inner
            .get_many(keys, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        self.inner
            .ttl_many(keys, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[HashMap<String, Value>],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.inner
            .put_many(
                keys,
                values,
                Some(&self.prefixed_collection(collection)),
                ttl,
            )
            .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.inner
            .delete_many(keys, Some(&self.prefixed_collection(collection)))
            .await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for PrefixCollectionsWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner
            .keys(Some(&self.prefixed_collection(collection)), limit)
            .await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for PrefixCollectionsWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let collections = self.inner.collections(limit).await?;
        Ok(collections
            .into_iter()
            .filter_map(|collection| collection.strip_prefix(&self.prefix).map(str::to_string))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    #[tokio::test]
    async fn test_prefix_collections() {
        let mem = MemoryStore::new();
        let wrapper = PrefixCollectionsWrapper::new(mem, "tenant_");

        let mut value = HashMap::new();
        value.insert("a".to_string(), Value::String("b".to_string()));
        wrapper
            .put("k", value.clone(), Some("users"), None)
            .await
            .unwrap();

        let got = wrapper.get("k", Some("users")).await.unwrap();
        assert_eq!(got, Some(value));

        let raw = wrapper.into_inner();
        assert!(raw.get("k", Some("tenant_users")).await.unwrap().is_some());
    }
}
