use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// A wrapper that returns a default value when a key is missing.
pub struct DefaultValueWrapper<T: AsyncKeyValue> {
    inner: T,
    default_value: HashMap<String, Value>,
}

impl<T: AsyncKeyValue> DefaultValueWrapper<T> {
    pub fn new(inner: T, default_value: HashMap<String, Value>) -> Self {
        Self {
            inner,
            default_value,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for DefaultValueWrapper<T> {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        match self.inner.get(key, collection).await? {
            Some(value) => Ok(Some(value)),
            None => Ok(Some(self.default_value.clone())),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        match self.inner.ttl(key, collection).await? {
            Some((value, ttl)) => Ok(Some((value, ttl))),
            None => Ok(Some((self.default_value.clone(), 0.0))),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
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
        let results = self.inner.get_many(keys, collection).await?;
        Ok(results
            .into_iter()
            .map(|opt| opt.or_else(|| Some(self.default_value.clone())))
            .collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let results = self.inner.ttl_many(keys, collection).await?;
        Ok(results
            .into_iter()
            .map(|opt| opt.or_else(|| Some((self.default_value.clone(), 0.0))))
            .collect())
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[HashMap<String, Value>],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.inner.put_many(keys, values, collection, ttl).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.inner.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for DefaultValueWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for DefaultValueWrapper<T>
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
    async fn test_default_value() {
        let mem = MemoryStore::new();
        let mut default_value = HashMap::new();
        default_value.insert("default".to_string(), Value::Bool(true));

        let wrapper = DefaultValueWrapper::new(mem, default_value.clone());

        let got = wrapper.get("missing", None).await.unwrap();
        assert_eq!(got, Some(default_value));
    }

    #[tokio::test]
    async fn test_default_value_existing() {
        let mem = MemoryStore::new();
        let mut existing = HashMap::new();
        existing.insert("real".to_string(), Value::String("value".to_string()));
        mem.put("k", existing.clone(), None, None).await.unwrap();

        let mut default_value = HashMap::new();
        default_value.insert("default".to_string(), Value::Bool(true));

        let wrapper = DefaultValueWrapper::new(mem, default_value);

        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(existing));
    }
}
