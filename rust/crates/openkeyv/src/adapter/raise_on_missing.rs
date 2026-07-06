use crate::error::{Error, Result};
use crate::protocol::AsyncKeyValue;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Wrapper that raises `MissingKey` error when a key is not found.
pub struct RaiseOnMissingAdapter<T: AsyncKeyValue> {
    inner: T,
}

impl<T: AsyncKeyValue> RaiseOnMissingAdapter<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for RaiseOnMissingAdapter<T> {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        match self.inner.get(key, collection).await? {
            Some(value) => Ok(Some(value)),
            None => Err(Error::MissingKey(key.to_string())),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        match self.inner.ttl(key, collection).await? {
            Some((value, ttl)) => Ok(Some((value, ttl))),
            None => Err(Error::MissingKey(key.to_string())),
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
        for (key, result) in keys.iter().zip(results.iter()) {
            if result.is_none() {
                return Err(Error::MissingKey(key.clone()));
            }
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let results = self.inner.ttl_many(keys, collection).await?;
        for (key, result) in keys.iter().zip(results.iter()) {
            if result.is_none() {
                return Err(Error::MissingKey(key.clone()));
            }
        }
        Ok(results)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    #[tokio::test]
    async fn test_raise_on_missing_found() {
        let mem = MemoryStore::new();
        let mut value = HashMap::new();
        value.insert("a".to_string(), Value::String("b".to_string()));
        mem.put("k", value.clone(), None, None).await.unwrap();

        let adapter = RaiseOnMissingAdapter::new(mem);
        let got = adapter.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_raise_on_missing_not_found() {
        let mem = MemoryStore::new();
        let adapter = RaiseOnMissingAdapter::new(mem);
        let err = adapter.get("missing", None).await.unwrap_err();
        assert_eq!(err, Error::MissingKey("missing".to_string()));
    }
}
