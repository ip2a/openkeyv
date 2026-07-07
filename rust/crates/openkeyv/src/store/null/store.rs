use super::client::NullClient;
use super::config::NullConfig;
use super::error::Result;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;

/// A store that accepts all operations but stores nothing.
/// Useful for testing and as a no-op fallback.
pub struct NullStore {
    _client: NullClient,
    _config: NullConfig,
}

impl NullStore {
    pub fn new() -> Self {
        Self::with_config(NullConfig)
    }

    pub fn with_config(config: NullConfig) -> Self {
        Self {
            _client: NullClient::new(),
            _config: config,
        }
    }
}

impl Default for NullStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncKeyValue for NullStore {
    async fn get(&self, _key: &str, _collection: Option<&str>) -> Result<Option<Value>> {
        Ok(None)
    }

    async fn ttl(&self, _key: &str, _collection: Option<&str>) -> Result<Option<(Value, f64)>> {
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
        keys: &[String],
        _collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        Ok(vec![None; keys.len()])
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        _collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        Ok(vec![None; keys.len()])
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

#[async_trait]
impl AsyncCull for NullStore {
    async fn cull(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for NullStore {
    async fn keys(&self, _collection: Option<&str>, _limit: Option<usize>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl AsyncEnumerateCollections for NullStore {
    async fn collections(&self, _limit: Option<usize>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl AsyncDestroyCollection for NullStore {
    async fn destroy_collection(&self, _collection: &str) -> Result<bool> {
        Ok(false)
    }
}

#[async_trait]
impl AsyncDestroyStore for NullStore {
    async fn destroy(&self) -> Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_null_store() {
        let store = NullStore::new();
        let value = Value::null();

        store.put("k", value.clone(), None, None).await.unwrap();
        assert_eq!(store.get("k", None).await.unwrap(), None);
        assert!(!store.delete("k", None).await.unwrap());
        assert!(store.destroy().await.unwrap());
    }
}
