use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::RwLock;

const DEFAULT_COLLECTION: &str = "default_collection";

/// A simple single-threaded in-memory store backed by `HashMap`.
/// Intended for testing and development — no concurrency optimizations.
pub struct SimpleStore {
    data: RwLock<HashMap<String, HashMap<String, ManagedEntry>>>,
    default_collection: String,
}

impl SimpleStore {
    pub fn new() -> Self {
        Self::with_options(None)
    }

    pub fn with_options(default_collection: Option<String>) -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }
}

impl Default for SimpleStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncKeyValue for SimpleStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        let data = self.data.read().await;
        match data.get(cname).and_then(|col| col.get(key)) {
            Some(entry) if !entry.is_expired() => Ok(Some(entry.value.clone())),
            _ => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let cname = self.collection_name(collection);
        let data = self.data.read().await;
        match data.get(cname).and_then(|col| col.get(key)) {
            Some(entry) if !entry.is_expired() => {
                let ttl = entry.ttl().unwrap_or(0.0);
                Ok(Some((entry.value.clone(), ttl)))
            }
            _ => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        let mut data = self.data.write().await;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        data.entry(cname.to_string())
            .or_default()
            .insert(key.to_string(), entry);
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let mut data = self.data.write().await;
        Ok(data
            .get_mut(cname)
            .map(|col| col.remove(key).is_some())
            .unwrap_or(false))
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let cname = self.collection_name(collection);
        let data = self.data.read().await;
        let col = data.get(cname);
        Ok(keys
            .iter()
            .map(|k| {
                col.and_then(|c| c.get(k))
                    .filter(|e| !e.is_expired())
                    .map(|e| e.value.clone())
            })
            .collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let cname = self.collection_name(collection);
        let data = self.data.read().await;
        let col = data.get(cname);
        Ok(keys
            .iter()
            .map(|k| {
                col.and_then(|c| c.get(k))
                    .filter(|e| !e.is_expired())
                    .map(|e| {
                        let ttl = e.ttl().unwrap_or(0.0);
                        (e.value.clone(), ttl)
                    })
            })
            .collect())
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[HashMap<String, Value>],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        if keys.len() != values.len() {
            return Err(Error::BatchSizeMismatch {
                keys: keys.len(),
                values: values.len(),
            });
        }
        let cname = self.collection_name(collection);
        let mut data = self.data.write().await;
        let col = data.entry(cname.to_string()).or_default();
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            col.insert(key.clone(), entry);
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let mut data = self.data.write().await;
        let mut count = 0;
        if let Some(col) = data.get_mut(cname) {
            for key in keys {
                if col.remove(key).is_some() {
                    count += 1;
                }
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for SimpleStore {
    async fn cull(&self) -> Result<()> {
        let mut data = self.data.write().await;
        for col in data.values_mut() {
            col.retain(|_k, v| !v.is_expired());
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for SimpleStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let data = self.data.read().await;
        let keys: Vec<String> = data
            .get(cname)
            .map(|col| col.keys().cloned().collect())
            .unwrap_or_default();
        let limit = limit.unwrap_or(10_000).min(10_000);
        Ok(keys.into_iter().take(limit).collect())
    }
}

#[async_trait]
impl AsyncEnumerateCollections for SimpleStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let data = self.data.read().await;
        let limit = limit.unwrap_or(10_000).min(10_000);
        Ok(data.keys().take(limit).cloned().collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for SimpleStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let mut data = self.data.write().await;
        Ok(data.remove(collection).is_some())
    }
}

#[async_trait]
impl AsyncDestroyStore for SimpleStore {
    async fn destroy(&self) -> Result<bool> {
        let mut data = self.data.write().await;
        data.clear();
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple_store_roundtrip() {
        let store = SimpleStore::new();
        let mut value = HashMap::new();
        value.insert("x".to_string(), Value::Number(1.into()));

        store.put("k", value.clone(), None, None).await.unwrap();
        let got = store.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_simple_store_delete() {
        let store = SimpleStore::new();
        let value = HashMap::new();
        store.put("k", value, None, None).await.unwrap();
        assert!(store.delete("k", None).await.unwrap());
        assert!(!store.delete("k", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_simple_store_destroy() {
        let store = SimpleStore::new();
        let value = HashMap::new();
        store.put("k", value, None, None).await.unwrap();
        assert!(store.destroy().await.unwrap());
        assert_eq!(store.get("k", None).await.unwrap(), None);
    }
}
