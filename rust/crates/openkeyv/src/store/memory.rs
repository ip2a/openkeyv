use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::HashMap;
use tokio::sync::RwLock;

type SeedData = HashMap<String, HashMap<String, Value>>;

const DEFAULT_COLLECTION: &str = "default_collection";
const DEFAULT_PAGE_SIZE: usize = 10_000;
const PAGE_LIMIT: usize = 10_000;

/// A fixed-size in-memory key-value store using time-aware LRU cache per collection.
pub struct MemoryStore {
    collections: DashMap<String, DashMap<String, ManagedEntry>>,
    max_entries_per_collection: Option<usize>,
    setup_complete: RwLock<bool>,
    default_collection: String,
    seed: Option<SeedData>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::with_options(None, None, None)
    }

    pub fn with_options(
        max_entries_per_collection: Option<usize>,
        default_collection: Option<String>,
        seed: Option<SeedData>,
    ) -> Self {
        Self {
            collections: DashMap::new(),
            max_entries_per_collection,
            setup_complete: RwLock::new(false),
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
            seed,
        }
    }

    async fn setup(&self) -> Result<()> {
        let mut complete = self.setup_complete.write().await;
        if *complete {
            return Ok(());
        }

        // Seed store if data provided
        if let Some(seed) = &self.seed {
            for (collection, items) in seed {
                let col = self.collections.entry(collection.clone()).or_default();
                for (key, value) in items {
                    let entry = ManagedEntry::new(value.clone());
                    col.insert(key.clone(), entry);
                }
            }
        }

        *complete = true;
        Ok(())
    }

    async fn setup_collection(&self, collection: &str) -> Result<()> {
        self.setup().await?;
        self.collections.entry(collection.to_string()).or_default();
        Ok(())
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    fn get_collection(
        &self,
        name: &str,
    ) -> Result<dashmap::mapref::one::Ref<'_, String, DashMap<String, ManagedEntry>>> {
        self.collections
            .get(name)
            .ok_or_else(|| Error::InvalidOperation(format!("collection '{}' not found", name)))
    }

    fn maybe_cull_collection(&self, col: &DashMap<String, ManagedEntry>) {
        // Evict expired entries if we have a size limit approaching
        if let Some(max) = self.max_entries_per_collection {
            if col.len() > max {
                // Remove expired first
                col.retain(|_k, v| !v.is_expired());
                // If still over limit, remove oldest (simple strategy: arbitrary)
                while col.len() > max {
                    if let Some(k) = col.iter().next().map(|e| e.key().clone()) {
                        col.remove(&k);
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncKeyValue for MemoryStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        match col.get(key) {
            Some(entry) if !entry.is_expired() => Ok(Some(entry.value.clone())),
            _ => Ok(None),
        }
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        match col.get(key) {
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
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };

        if let Some(col) = self.collections.get_mut(cname) {
            self.maybe_cull_collection(&col);
            col.insert(key.to_string(), entry);
        }
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        Ok(col.remove(key).is_some())
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        let results: Vec<_> = keys
            .iter()
            .map(|k| {
                col.get(k)
                    .filter(|e| !e.is_expired())
                    .map(|e| e.value.clone())
            })
            .collect();
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        let results: Vec<_> = keys
            .iter()
            .map(|k| {
                col.get(k).filter(|e| !e.is_expired()).map(|e| {
                    let ttl = e.ttl().unwrap_or(0.0);
                    (e.value.clone(), ttl)
                })
            })
            .collect();
        Ok(results)
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
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
        self.setup_collection(cname).await?;

        if let Some(col) = self.collections.get_mut(cname) {
            self.maybe_cull_collection(&col);
            for (key, value) in keys.iter().zip(values.iter()) {
                let entry = match ttl {
                    Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                    None => ManagedEntry::new(value.clone()),
                };
                col.insert(key.clone(), entry);
            }
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        let mut count = 0;
        for key in keys {
            if col.remove(key).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for MemoryStore {
    async fn cull(&self) -> Result<()> {
        for entry in self.collections.iter() {
            let col = entry.value();
            col.retain(|_k, v| !v.is_expired());
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for MemoryStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        Ok(col.iter().take(limit).map(|e| e.key().clone()).collect())
    }
}

#[async_trait]
impl AsyncEnumerateCollections for MemoryStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        self.setup().await?;
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        Ok(self
            .collections
            .iter()
            .take(limit)
            .map(|e| e.key().clone())
            .collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for MemoryStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        self.setup().await?;
        Ok(self.collections.remove(collection).is_some())
    }
}

#[async_trait]
impl AsyncDestroyStore for MemoryStore {
    async fn destroy(&self) -> Result<bool> {
        self.collections.clear();
        let mut complete = self.setup_complete.write().await;
        *complete = false;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_store_basic() {
        let store = MemoryStore::new();
        let value = Value::utf8("test");

        store.put("key1", value.clone(), None, None).await.unwrap();
        let result = store.get("key1", None).await.unwrap();
        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_memory_store_ttl() {
        let store = MemoryStore::new();
        let value = Value::null();

        store
            .put("key1", value.clone(), None, Some(0.01))
            .await
            .unwrap();
        assert!(store.get("key1", None).await.unwrap().is_some());

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert!(store.get("key1", None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memory_store_delete() {
        let store = MemoryStore::new();
        let value = Value::null();

        store.put("key1", value, None, None).await.unwrap();
        assert!(store.delete("key1", None).await.unwrap());
        assert!(!store.delete("key1", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_store_collections() {
        let store = MemoryStore::new();
        let value = Value::null();

        store
            .put("k", value.clone(), Some("c1"), None)
            .await
            .unwrap();
        store.put("k", value, Some("c2"), None).await.unwrap();

        let cols = store.collections(None).await.unwrap();
        assert!(cols.contains(&"c1".to_string()));
        assert!(cols.contains(&"c2".to_string()));
    }

    #[tokio::test]
    async fn test_memory_store_bulk() {
        let store = MemoryStore::new();
        let v1 = Value::integer(1);
        let v2 = Value::integer(2);

        store
            .put_many(
                &["k1".to_string(), "k2".to_string()],
                &[v1.clone(), v2.clone()],
                None,
                None,
            )
            .await
            .unwrap();

        let results = store
            .get_many(&["k1".to_string(), "k2".to_string()], None)
            .await
            .unwrap();
        assert_eq!(results, vec![Some(v1), Some(v2)]);
    }

    #[tokio::test]
    async fn test_memory_store_destroy() {
        let store = MemoryStore::new();
        let value = Value::null();
        store.put("k", value, None, None).await.unwrap();

        assert!(store.destroy().await.unwrap());
        assert_eq!(store.get("k", None).await.unwrap(), None);
    }
}
