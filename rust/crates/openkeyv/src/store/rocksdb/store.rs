use super::client::RocksDBClient;
use super::config::RocksDBConfig;
use super::error::{Error, Result, map_rocksdb_err};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;

const COLLECTION_SEPARATOR: &str = ":";

fn compound_key(collection: &str, key: &str) -> String {
    format!("{}{}{}", collection, COLLECTION_SEPARATOR, key)
}

/// RocksDB-backed key-value store.
///
/// Each collection is represented by a key prefix.
/// Values are stored as `OKVE1`-encoded `ManagedEntry` bytes.
pub struct RocksDBStore {
    client: RocksDBClient,
    config: RocksDBConfig,
}

impl RocksDBStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, path).map_err(map_rocksdb_err)?;
        Ok(Self::with_config(db, RocksDBConfig::default()))
    }

    pub fn from_db(db: rocksdb::DB) -> Self {
        Self::with_config(db, RocksDBConfig::default())
    }

    pub fn with_config(db: rocksdb::DB, config: RocksDBConfig) -> Self {
        Self {
            client: RocksDBClient::new(db),
            config,
        }
    }

    fn db(&self) -> &rocksdb::DB {
        self.client.db()
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn get_entry(&self, key: &str, collection: &str) -> Result<Option<ManagedEntry>> {
        let ck = compound_key(collection, key);
        match self.db().get(&ck).map_err(map_rocksdb_err)? {
            Some(bytes) => {
                let entry = ManagedEntry::decode(Bytes::from(bytes))?;
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
        self.db()
            .put(&ck, entry.encode())
            .map_err(map_rocksdb_err)?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for RocksDBStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        Ok(self.get_entry(key, cname)?.map(|e| e.value))
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
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
        value: Value,
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
        let existed = self.db().key_may_exist(&ck);
        self.db().delete(&ck).map_err(map_rocksdb_err)?;
        Ok(existed)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        let compound_keys: Vec<String> = keys.iter().map(|key| compound_key(cname, key)).collect();
        self.db()
            .multi_get(&compound_keys)
            .into_iter()
            .map(|result| match result.map_err(map_rocksdb_err)? {
                Some(bytes) => {
                    let entry = ManagedEntry::decode(Bytes::from(bytes))?;
                    if entry.is_expired() {
                        Ok(None)
                    } else {
                        Ok(Some(entry.value))
                    }
                }
                None => Ok(None),
            })
            .collect()
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        let cname = self.collection_name(collection);
        let compound_keys: Vec<String> = keys.iter().map(|key| compound_key(cname, key)).collect();
        self.db()
            .multi_get(&compound_keys)
            .into_iter()
            .map(|result| match result.map_err(map_rocksdb_err)? {
                Some(bytes) => {
                    let entry = ManagedEntry::decode(Bytes::from(bytes))?;
                    if entry.is_expired() {
                        Ok(None)
                    } else {
                        let ttl = entry.ttl().unwrap_or(0.0);
                        Ok(Some((entry.value, ttl)))
                    }
                }
                None => Ok(None),
            })
            .collect()
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
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let ck = compound_key(cname, key);
            batch.put(&ck, entry.encode());
        }
        self.db().write(batch).map_err(map_rocksdb_err)?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let mut batch = rocksdb::WriteBatch::default();
        let mut count = 0;
        for key in keys {
            let ck = compound_key(cname, key);
            if self.db().key_may_exist(&ck) {
                count += 1;
            }
            batch.delete(&ck);
        }
        self.db().write(batch).map_err(map_rocksdb_err)?;
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for RocksDBStore {
    async fn cull(&self) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let iter = self.db().iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, v) = item.map_err(map_rocksdb_err)?;
            let entry = ManagedEntry::decode(Bytes::from(v))?;
            if entry.is_expired() {
                batch.delete(&k);
            }
        }
        self.db().write(batch).map_err(map_rocksdb_err)?;
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
        let iter = self.db().prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            let key = std::str::from_utf8(&k)
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let Some(stripped) = key.strip_prefix(&prefix) else {
                break;
            };
            keys.push(stripped.to_string());
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
        let iter = self.db().iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            let key = std::str::from_utf8(&k)
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let separator = key.find(COLLECTION_SEPARATOR).ok_or_else(|| {
                Error::Deserialization("invalid RocksDB compound key".to_string())
            })?;
            collections.insert(key[..separator].to_string());
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
        let iter = self.db().prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            batch.delete(&k);
            had_any = true;
        }
        if had_any {
            self.db().write(batch).map_err(map_rocksdb_err)?;
        }
        Ok(had_any)
    }
}

#[async_trait]
impl AsyncDestroyStore for RocksDBStore {
    async fn destroy(&self) -> Result<bool> {
        let mut batch = rocksdb::WriteBatch::default();
        let iter = self.db().iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, _) = item.map_err(map_rocksdb_err)?;
            batch.delete(&k);
        }
        self.db().write(batch).map_err(map_rocksdb_err)?;
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
        let value = Value::utf8("Alice");

        store.put("user1", value.clone(), None, None).await.unwrap();
        let got = store.get("user1", None).await.unwrap();
        assert_eq!(got, Some(value));

        let key = compound_key(store.collection_name(None), "user1");
        let bytes = store.db().get(key).unwrap().unwrap();
        assert!(bytes.starts_with(b"OKVE1"));
    }

    #[tokio::test]
    async fn test_rocksdb_store_batch_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let keys = vec!["one".to_string(), "two".to_string()];
        let values = vec![Value::integer(1), Value::integer(2)];

        store.put_many(&keys, &values, None, None).await.unwrap();

        assert_eq!(
            store.get_many(&keys, None).await.unwrap(),
            vec![Some(values[0].clone()), Some(values[1].clone()),]
        );
        for key in keys {
            let key = compound_key(store.collection_name(None), &key);
            let bytes = store.db().get(key).unwrap().unwrap();
            assert!(bytes.starts_with(b"OKVE1"));
        }
    }

    #[tokio::test]
    async fn test_rocksdb_batch_reads_preserve_order_and_expiration() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let persistent = Value::utf8("persistent");
        let expiring = Value::utf8("expiring");

        store
            .put("persistent", persistent.clone(), None, None)
            .await
            .unwrap();
        store
            .put("expiring", expiring.clone(), None, Some(60.0))
            .await
            .unwrap();
        store
            .put("expired", Value::null(), None, Some(-1.0))
            .await
            .unwrap();

        let keys = vec![
            "persistent".to_string(),
            "missing".to_string(),
            "expired".to_string(),
            "expiring".to_string(),
        ];
        assert_eq!(
            store.get_many(&keys, None).await.unwrap(),
            vec![Some(persistent.clone()), None, None, Some(expiring.clone()),]
        );

        let results = store.ttl_many(&keys, None).await.unwrap();
        assert_eq!(results[0], Some((persistent, 0.0)));
        assert_eq!(results[1], None);
        assert_eq!(results[2], None);
        let (value, ttl) = results[3].as_ref().unwrap();
        assert_eq!(value, &expiring);
        assert!(*ttl > 0.0 && *ttl <= 60.0);
    }

    #[tokio::test]
    async fn test_rocksdb_store_rejects_json_entry_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let key = compound_key(store.collection_name(None), "json-entry");
        store.db().put(key, br#"{"value":null}"#).unwrap();

        let err = store.get("json-entry", None).await.unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[tokio::test]
    async fn test_rocksdb_cull_rejects_corrupt_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let key = compound_key(store.collection_name(None), "corrupt");
        store.db().put(key, b"corrupt").unwrap();

        let err = store.cull().await.unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[tokio::test]
    async fn test_rocksdb_enumeration_rejects_invalid_key_encoding() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let mut key =
            format!("{}{}", store.collection_name(None), COLLECTION_SEPARATOR).into_bytes();
        key.push(0xff);
        store
            .db()
            .put(key, ManagedEntry::new(Value::null()).encode())
            .unwrap();

        assert!(store.keys(None, None).await.is_err());
        assert!(store.collections(None).await.is_err());
    }

    #[tokio::test]
    async fn test_rocksdb_store_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let value = Value::null();

        store.put("k", value, None, None).await.unwrap();
        assert!(store.delete("k", None).await.unwrap());
        assert!(!store.delete("k", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_rocksdb_store_collections() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
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
    async fn test_rocksdb_store_destroy_collection() {
        let tmp = tempfile::tempdir().unwrap();
        let store = RocksDBStore::new(tmp.path()).unwrap();
        let value = Value::null();

        store.put("k", value, Some("c1"), None).await.unwrap();
        assert!(store.destroy_collection("c1").await.unwrap());
        assert!(!store.destroy_collection("c1").await.unwrap());
    }
}
