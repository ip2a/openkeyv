use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

const DEFAULT_COLLECTION: &str = "default_collection";
const COLLECTION_SEPARATOR: &str = ":";

fn compound_key(collection: &str, key: &str) -> String {
    format!("{}{}{}", collection, COLLECTION_SEPARATOR, key)
}

fn map_rocksdb_err(e: rocksdb::Error) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}

/// RocksDB-backed key-value store.
///
/// Each collection is represented by a key prefix.
/// Values are stored as JSON-serialized `ManagedEntry` bytes.
pub struct RocksDBStore {
    db: rocksdb::DB,
    default_collection: String,
}

impl RocksDBStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, path).map_err(map_rocksdb_err)?;
        Ok(Self {
            db,
            default_collection: DEFAULT_COLLECTION.to_string(),
        })
    }

    pub fn from_db(db: rocksdb::DB) -> Self {
        Self {
            db,
            default_collection: DEFAULT_COLLECTION.to_string(),
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    fn get_entry(&self, key: &str, collection: &str) -> Result<Option<ManagedEntry>> {
        let ck = compound_key(collection, key);
        match self.db.get(&ck).map_err(map_rocksdb_err)? {
            Some(bytes) => {
                let entry: ManagedEntry = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry))
                }
            }
            None => Ok(None),
        }
    }

    fn put_entry(&self, key: &str, collection: &str, entry: &ManagedEntry) -> Result<()> {
        let ck = compound_key(collection, key);
        let bytes = serde_json::to_vec(entry).map_err(|e| Error::Serialization(e.to_string()))?;
        self.db.put(&ck, bytes).map_err(map_rocksdb_err)?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for RocksDBStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        Ok(self.get_entry(key, cname)?.map(|e| e.value))
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let cname = self.collection_name(collection);
        match self.get_entry(key, cname)? {
            Some(entry) => {
                let ttl = entry.ttl().unwrap_or(0.0);
                Ok(Some((entry.value, ttl)))
            }
            None => Ok(None),
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
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        self.put_entry(key, cname, &entry)
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let existed = self.db.key_may_exist(&ck);
        self.db.delete(&ck).map_err(map_rocksdb_err)?;
        Ok(existed)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let cname = self.collection_name(collection);
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get_entry(key, cname)?.map(|e| e.value));
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let cname = self.collection_name(collection);
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_entry(key, cname)? {
                Some(entry) => {
                    let ttl = entry.ttl().unwrap_or(0.0);
                    results.push(Some((entry.value, ttl)));
                }
                None => results.push(None),
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
        if keys.len() != values.len() {
            return Err(Error::BatchSizeMismatch {
                keys: keys.len(),
                values: values.len(),
            });
        }
        let cname = self.collection_name(collection);
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let ck = compound_key(cname, key);
            let bytes =
                serde_json::to_vec(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
            batch.put(&ck, bytes);
        }
        self.db.write(batch).map_err(map_rocksdb_err)?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let mut batch = rocksdb::WriteBatch::default();
        let mut count = 0;
        for key in keys {
            let ck = compound_key(cname, key);
            if self.db.key_may_exist(&ck) {
                count += 1;
            }
            batch.delete(&ck);
        }
        self.db.write(batch).map_err(map_rocksdb_err)?;
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for RocksDBStore {
    async fn cull(&self) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, v) = item.map_err(map_rocksdb_err)?;
            if let Ok(entry) = serde_json::from_slice::<ManagedEntry>(&v) {
                if entry.is_expired() {
                    batch.delete(&k);
                }
            }
        }
        self.db.write(batch).map_err(map_rocksdb_err)?;
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for RocksDBStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let prefix = format!("{}{}", cname, COLLECTION_SEPARATOR);
        let limit = limit.unwrap_or(10_000).min(10_000);
        let mut keys = Vec::new();
        let iter = self.db.prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            if let Ok(s) = std::str::from_utf8(&k) {
                if let Some(stripped) = s.strip_prefix(&prefix) {
                    keys.push(stripped.to_string());
                }
            }
            if keys.len() >= limit {
                break;
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for RocksDBStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000);
        let mut collections = std::collections::HashSet::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            if let Ok(s) = std::str::from_utf8(&k) {
                if let Some(pos) = s.find(COLLECTION_SEPARATOR) {
                    collections.insert(s[..pos].to_string());
                }
            }
            if collections.len() >= limit {
                break;
            }
        }
        Ok(collections.into_iter().collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for RocksDBStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let prefix = format!("{}{}", collection, COLLECTION_SEPARATOR);
        let mut batch = rocksdb::WriteBatch::default();
        let mut had_any = false;
        let iter = self.db.prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            batch.delete(&k);
            had_any = true;
        }
        if had_any {
            self.db.write(batch).map_err(map_rocksdb_err)?;
        }
        Ok(had_any)
    }
}

#[async_trait]
impl AsyncDestroyStore for RocksDBStore {
    async fn destroy(&self) -> Result<bool> {
        let mut batch = rocksdb::WriteBatch::default();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            batch.delete(&k);
        }
        self.db.write(batch).map_err(map_rocksdb_err)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rocksdb_store_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let mut value = HashMap::new();
        value.insert("name".to_string(), Value::String("Alice".to_string()));

        store.put("user1", value.clone(), None, None).await.unwrap();
        let got = store.get("user1", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_rocksdb_store_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let value = HashMap::new();

        store.put("k", value, None, None).await.unwrap();
        assert!(store.delete("k", None).await.unwrap());
        assert!(!store.delete("k", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_rocksdb_store_collections() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let value = HashMap::new();

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
    async fn test_rocksdb_store_destroy_collection() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let value = HashMap::new();

        store.put("k", value, Some("c1"), None).await.unwrap();
        assert!(store.destroy_collection("c1").await.unwrap());
        assert!(!store.destroy_collection("c1").await.unwrap());
    }
}
