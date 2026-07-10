use super::client::DiskClient;
use super::config::DiskConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;

fn entry_to_ivec(entry: &ManagedEntry) -> Result<sled::IVec> {
    Ok(entry.encode().into())
}

fn ivec_to_entry(iv: sled::IVec) -> Result<ManagedEntry> {
    ManagedEntry::decode(Bytes::from_owner(iv))
}

/// Disk-backed store using [sled], an embedded key-value database.
///
/// Each collection maps to a separate sled `Tree`.
pub struct DiskStore {
    client: DiskClient,
    config: DiskConfig,
}

impl DiskStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(path).map_err(|e| Error::StoreSetup {
            message: format!("failed to open sled database: {}", e),
        })?;
        Ok(Self::with_config(db, DiskConfig::default()))
    }

    pub fn with_config(db: sled::Db, config: DiskConfig) -> Self {
        Self {
            client: DiskClient::new(db),
            config,
        }
    }

    fn db(&self) -> &sled::Db {
        self.client.db()
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn get_tree(&self, collection: &str) -> Result<sled::Tree> {
        self.db()
            .open_tree(collection)
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to open tree: {}", e),
            })
    }
}

impl Default for DiskStore {
    fn default() -> Self {
        let db = sled::Config::new()
            .temporary(true)
            .open()
            .expect("temporary sled database should open");
        Self::with_config(db, DiskConfig::default())
    }
}

#[async_trait]
impl AsyncKeyValue for DiskStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let key = key.as_bytes();
        let res = tree.get(key).map_err(|e| Error::StoreConnection {
            message: format!("failed to get: {}", e),
        })?;
        match res {
            Some(iv) => {
                let entry = ivec_to_entry(iv)?;
                if entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry.value))
                }
            }
            None => Ok(None),
        }
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let res = tree.get(key).map_err(|e| Error::StoreConnection {
            message: format!("failed to get: {}", e),
        })?;
        match res {
            Some(iv) => {
                let entry = ivec_to_entry(iv)?;
                if entry.is_expired() {
                    Ok(None)
                } else {
                    let ttl = entry.ttl().unwrap_or(0.0);
                    Ok(Some((entry.value, ttl)))
                }
            }
            None => Ok(None),
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
        let tree = self.get_tree(cname)?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let iv = entry_to_ivec(&entry)?;
        tree.insert(key, iv).map_err(|e| Error::StoreConnection {
            message: format!("failed to insert: {}", e),
        })?;
        tree.flush_async()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to flush: {}", e),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let res = tree.remove(key).map_err(|e| Error::StoreConnection {
            message: format!("failed to remove: {}", e),
        })?;
        Ok(res.is_some())
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let res = tree
                .get(key.as_bytes())
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to get: {}", e),
                })?;
            match res {
                Some(iv) => {
                    let entry = ivec_to_entry(iv)?;
                    if entry.is_expired() {
                        results.push(None);
                    } else {
                        results.push(Some(entry.value));
                    }
                }
                None => results.push(None),
            }
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let res = tree
                .get(key.as_bytes())
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to get: {}", e),
                })?;
            match res {
                Some(iv) => {
                    let entry = ivec_to_entry(iv)?;
                    if entry.is_expired() {
                        results.push(None);
                    } else {
                        let ttl = entry.ttl().unwrap_or(0.0);
                        results.push(Some((entry.value, ttl)));
                    }
                }
                None => results.push(None),
            }
        }
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
        let tree = self.get_tree(cname)?;
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let iv = entry_to_ivec(&entry)?;
            tree.insert(key.as_bytes(), iv)
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to insert: {}", e),
                })?;
        }
        tree.flush_async()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to flush: {}", e),
            })?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut count = 0;
        for key in keys {
            let res = tree.remove(key).map_err(|e| Error::StoreConnection {
                message: format!("failed to remove: {}", e),
            })?;
            if res.is_some() {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for DiskStore {
    async fn cull(&self) -> Result<()> {
        for name in self.db().tree_names() {
            let tree = self
                .db()
                .open_tree(&name)
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to open tree: {}", e),
                })?;
            for res in tree.iter() {
                let (k, v) = res.map_err(|e| Error::StoreConnection {
                    message: format!("failed to iterate: {}", e),
                })?;
                let entry = ivec_to_entry(v)?;
                if entry.is_expired() {
                    tree.remove(k).map_err(|e| Error::StoreConnection {
                        message: format!("failed to remove expired entry: {}", e),
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for DiskStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut keys = Vec::new();
        let limit = limit.unwrap_or(10_000).min(10_000);
        for res in tree.iter() {
            let (k, _) = res.map_err(|e| Error::StoreConnection {
                message: format!("failed to iterate: {}", e),
            })?;
            if let Ok(s) = std::str::from_utf8(&k) {
                keys.push(s.to_string());
            }
            if keys.len() >= limit {
                break;
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for DiskStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let mut collections = Vec::new();
        let limit = limit.unwrap_or(10_000).min(10_000);
        for name in self.db().tree_names() {
            if let Ok(s) = std::str::from_utf8(&name) {
                collections.push(s.to_string());
            }
            if collections.len() >= limit {
                break;
            }
        }
        // Also include the default tree if it has been accessed
        if !self.db().was_recovered() {
            // sled doesn't have a direct way to list "open" trees vs all trees
            // tree_names already returns all trees
        }
        Ok(collections)
    }
}

#[async_trait]
impl AsyncDestroyCollection for DiskStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let cname = self.collection_name(Some(collection));
        self.db()
            .drop_tree(cname)
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to drop tree: {}", e),
            })?;
        Ok(true)
    }
}

#[async_trait]
impl AsyncDestroyStore for DiskStore {
    async fn destroy(&self) -> Result<bool> {
        self.db().clear().map_err(|e| Error::StoreConnection {
            message: format!("failed to clear database: {}", e),
        })?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_disk_store_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let value = Value::utf8("Alice");

        store.put("user1", value.clone(), None, None).await.unwrap();
        let got = store.get("user1", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_disk_store_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let value = Value::null();

        store.put("k", value, None, None).await.unwrap();
        assert!(store.delete("k", None).await.unwrap());
        assert!(!store.delete("k", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_disk_store_collections() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
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
    async fn test_disk_store_destroy_collection() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let value = Value::null();

        store.put("k", value, Some("c1"), None).await.unwrap();
        assert!(store.destroy_collection("c1").await.unwrap());
    }

    #[tokio::test]
    async fn test_disk_store_rejects_json_entry_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let tree = store.get_tree(store.collection_name(None)).unwrap();
        tree.insert("k", br#"{"value":null}"#).unwrap();

        let err = store.get("k", None).await.unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }
}
