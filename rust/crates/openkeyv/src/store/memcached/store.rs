use super::client::MemcachedClient;
use super::config::MemcachedConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{AsyncDestroyStore, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;

/// Memcached-backed key-value store.
///
/// Uses the `memcache` crate (sync client) under a Tokio mutex.
/// Values are stored as `OKVE1`-encoded `ManagedEntry` bytes by compound key.
pub struct MemcachedStore {
    client: MemcachedClient,
    config: MemcachedConfig,
}

impl MemcachedStore {
    pub fn new(url: &str) -> Result<Self> {
        let client = memcache::Client::connect(url).map_err(|error| Error::StoreConnection {
            message: format!("failed to connect to memcached: {error}"),
        })?;
        Ok(Self::with_config(client, MemcachedConfig::default()))
    }

    pub fn from_client(client: memcache::Client) -> Self {
        Self::with_config(client, MemcachedConfig::default())
    }

    pub fn with_config(client: memcache::Client, config: MemcachedConfig) -> Self {
        Self {
            client: MemcachedClient::new(client),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn client(&self) -> &tokio::sync::Mutex<memcache::Client> {
        self.client.client()
    }

    fn compound_key(collection: &str, key: &str) -> String {
        format!("{}:{}", collection, key)
    }
}

#[async_trait]
impl AsyncKeyValue for MemcachedStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let guard = self.client().lock().await;
        let raw: Option<Vec<u8>> = guard.get(&ck).map_err(|error| Error::StoreConnection {
            message: format!("failed to get memcached key {ck}: {error}"),
        })?;
        match raw {
            Some(raw) => {
                let entry = ManagedEntry::decode(Bytes::from(raw))?;
                if entry.is_expired() {
                    guard.delete(&ck).map_err(|error| Error::StoreConnection {
                        message: format!("failed to delete expired memcached key {ck}: {error}"),
                    })?;
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
        let ck = Self::compound_key(cname, key);
        let guard = self.client().lock().await;
        let raw: Option<Vec<u8>> = guard.get(&ck).map_err(|error| Error::StoreConnection {
            message: format!("failed to get memcached key {ck}: {error}"),
        })?;
        match raw {
            Some(raw) => {
                let entry = ManagedEntry::decode(Bytes::from(raw))?;
                if entry.is_expired() {
                    guard.delete(&ck).map_err(|error| Error::StoreConnection {
                        message: format!("failed to delete expired memcached key {ck}: {error}"),
                    })?;
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
        let ck = Self::compound_key(cname, key);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let encoded = entry.encode();
        let exptime = entry.ttl().map(|t| t.max(1.0) as u32).unwrap_or(0);
        let guard = self.client().lock().await;
        guard
            .set(&ck, encoded.as_slice(), exptime)
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to set memcached key {ck}: {error}"),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let guard = self.client().lock().await;
        guard.delete(&ck).map_err(|error| Error::StoreConnection {
            message: format!("failed to delete memcached key {ck}: {error}"),
        })
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let cname = self.collection_name(collection);
        let compound_keys: Vec<String> = keys
            .iter()
            .map(|key| Self::compound_key(cname, key))
            .collect();
        let key_refs: Vec<&str> = compound_keys.iter().map(String::as_str).collect();
        let guard = self.client().lock().await;
        let raw_entries: std::collections::HashMap<String, Vec<u8>> = guard
            .gets(&key_refs)
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to get multiple memcached keys: {error}"),
            })?;
        let mut entries = std::collections::HashMap::with_capacity(raw_entries.len());

        for (compound_key, raw) in raw_entries {
            let entry = ManagedEntry::decode(Bytes::from(raw))?;
            if entry.is_expired() {
                guard
                    .delete(&compound_key)
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to delete expired memcached key {compound_key}: {error}"
                        ),
                    })?;
            } else {
                entries.insert(compound_key, entry);
            }
        }

        Ok(compound_keys
            .iter()
            .map(|compound_key| entries.get(compound_key).map(|entry| entry.value.clone()))
            .collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let cname = self.collection_name(collection);
        let compound_keys: Vec<String> = keys
            .iter()
            .map(|key| Self::compound_key(cname, key))
            .collect();
        let key_refs: Vec<&str> = compound_keys.iter().map(String::as_str).collect();
        let guard = self.client().lock().await;
        let raw_entries: std::collections::HashMap<String, Vec<u8>> = guard
            .gets(&key_refs)
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to get multiple memcached keys: {error}"),
            })?;
        let mut entries = std::collections::HashMap::with_capacity(raw_entries.len());

        for (compound_key, raw) in raw_entries {
            let entry = ManagedEntry::decode(Bytes::from(raw))?;
            if entry.is_expired() {
                guard
                    .delete(&compound_key)
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to delete expired memcached key {compound_key}: {error}"
                        ),
                    })?;
            } else {
                entries.insert(compound_key, entry);
            }
        }

        Ok(compound_keys
            .iter()
            .map(|compound_key| {
                entries.get(compound_key).map(|entry| {
                    let ttl = entry.ttl().unwrap_or(0.0);
                    (entry.value.clone(), ttl)
                })
            })
            .collect())
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
        let exptime = ttl.map(|seconds| seconds.max(1.0) as u32).unwrap_or(0);
        let mut entries = Vec::with_capacity(keys.len());

        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let ck = Self::compound_key(cname, key);
            entries.push((ck, entry.encode()));
        }

        let guard = self.client().lock().await;
        for (ck, encoded) in entries {
            guard
                .set(&ck, encoded.as_slice(), exptime)
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to set memcached key {ck}: {error}"),
                })?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let compound_keys: Vec<String> = keys
            .iter()
            .map(|key| Self::compound_key(cname, key))
            .collect();
        let guard = self.client().lock().await;
        let mut count = 0;

        for compound_key in compound_keys {
            if guard
                .delete(&compound_key)
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to delete memcached key {compound_key}: {error}"),
                })?
            {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncDestroyStore for MemcachedStore {
    async fn destroy(&self) -> Result<bool> {
        let guard = self.client().lock().await;
        guard.flush().map_err(|error| Error::StoreConnection {
            message: format!("failed to flush memcached: {error}"),
        })?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeDelta, Utc};

    #[tokio::test]
    #[ignore = "requires OPENKEYV_MEMCACHED_URL"]
    async fn memcached_uses_binary_entries_and_native_batch_reads() {
        let url = std::env::var("OPENKEYV_MEMCACHED_URL").unwrap();
        let store = MemcachedStore::new(&url).unwrap();
        let collection = format!("openkeyv_binary_test_{}", std::process::id());
        let keys = vec![
            "missing".to_string(),
            "one".to_string(),
            "two".to_string(),
            "one".to_string(),
        ];
        let values = vec![
            Value::integer(1),
            Value::binary(Bytes::from_static(&[0, 255, 1])),
        ];
        let single_value = Value::utf8("single-value");

        store
            .put(
                "single",
                single_value.clone(),
                Some(&collection),
                Some(30.0),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get("single", Some(&collection)).await.unwrap(),
            Some(single_value.clone())
        );
        let (ttl_value, ttl) = store
            .ttl("single", Some(&collection))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ttl_value, single_value);
        assert!(ttl > 0.0 && ttl <= 30.0);

        let single_key = MemcachedStore::compound_key(&collection, "single");
        let raw: Option<Vec<u8>> = store.client().lock().await.get(&single_key).unwrap();
        assert!(raw.unwrap().starts_with(b"OKVE1"));

        store
            .put_many(
                &["one".to_string(), "two".to_string()],
                &values,
                Some(&collection),
                Some(30.0),
            )
            .await
            .unwrap();

        assert_eq!(
            store.get_many(&keys, Some(&collection)).await.unwrap(),
            vec![
                None,
                Some(values[0].clone()),
                Some(values[1].clone()),
                Some(values[0].clone()),
            ]
        );

        let ttl_results = store.ttl_many(&keys, Some(&collection)).await.unwrap();
        assert!(ttl_results[0].is_none());
        assert_eq!(ttl_results[1].as_ref().unwrap().0, values[0]);
        assert_eq!(ttl_results[2].as_ref().unwrap().0, values[1]);
        assert_eq!(ttl_results[3].as_ref().unwrap().0, values[0]);
        for result in ttl_results.into_iter().flatten() {
            assert!(result.1 > 0.0 && result.1 <= 30.0);
        }

        let one_key = MemcachedStore::compound_key(&collection, "one");
        let raw: Option<Vec<u8>> = store.client().lock().await.get(&one_key).unwrap();
        assert!(raw.unwrap().starts_with(b"OKVE1"));

        let expired_key = MemcachedStore::compound_key(&collection, "expired");
        let expired_entry = ManagedEntry {
            value: Value::utf8("expired"),
            created_at: Some(Utc::now() - TimeDelta::seconds(2)),
            expires_at: Some(Utc::now() - TimeDelta::seconds(1)),
        };
        store
            .client()
            .lock()
            .await
            .set(&expired_key, expired_entry.encode().as_slice(), 0)
            .unwrap();
        assert_eq!(store.get("expired", Some(&collection)).await.unwrap(), None);
        let raw: Option<Vec<u8>> = store.client().lock().await.get(&expired_key).unwrap();
        assert!(raw.is_none());

        assert_eq!(
            store
                .delete_many(
                    &[
                        "single".to_string(),
                        "one".to_string(),
                        "two".to_string(),
                        "missing".to_string(),
                    ],
                    Some(&collection),
                )
                .await
                .unwrap(),
            3
        );
        assert!(!store.delete("one", Some(&collection)).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_MEMCACHED_URL"]
    async fn memcached_rejects_json_entry_payload() {
        let url = std::env::var("OPENKEYV_MEMCACHED_URL").unwrap();
        let store = MemcachedStore::new(&url).unwrap();
        let collection = format!("openkeyv_json_test_{}", std::process::id());
        let key = MemcachedStore::compound_key(&collection, "json-entry");

        store
            .client()
            .lock()
            .await
            .set(&key, br#"{"value":null}"#.as_slice(), 0)
            .unwrap();

        let error = store
            .get("json-entry", Some(&collection))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid OpenKeyV entry magic"));

        assert!(store.delete("json-entry", Some(&collection)).await.unwrap());
    }
}
