use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{AsyncDestroyStore, AsyncKeyValue};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

const DEFAULT_COLLECTION: &str = "default_collection";

/// Memcached-backed key-value store.
///
/// Uses the `memcache` crate (sync client) under a Tokio mutex.
/// Values are JSON-serialized `ManagedEntry` strings stored by compound key.
pub struct MemcachedStore {
    client: tokio::sync::Mutex<memcache::Client>,
    default_collection: String,
}

impl MemcachedStore {
    pub fn new(url: &str) -> Result<Self> {
        let client = memcache::Client::connect(url).map_err(|e| Error::StoreConnection {
            message: format!("failed to connect to memcached: {e}"),
        })?;
        Ok(Self {
            client: tokio::sync::Mutex::new(client),
            default_collection: DEFAULT_COLLECTION.to_string(),
        })
    }

    pub fn from_client(client: memcache::Client) -> Self {
        Self {
            client: tokio::sync::Mutex::new(client),
            default_collection: DEFAULT_COLLECTION.to_string(),
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    fn compound_key(collection: &str, key: &str) -> String {
        format!("{}:{}", collection, key)
    }
}

#[async_trait]
impl AsyncKeyValue for MemcachedStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let guard = self.client.lock().await;
        let raw: Option<String> = guard.get(&ck).map_err(|e| Error::StoreConnection {
            message: format!("{e}"),
        })?;
        match raw {
            Some(json_str) => {
                let entry: ManagedEntry = serde_json::from_str(&json_str)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if entry.is_expired() {
                    let _ = guard.delete(&ck);
                    Ok(None)
                } else {
                    Ok(Some(entry.value))
                }
            }
            None => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let guard = self.client.lock().await;
        let raw: Option<String> = guard.get(&ck).map_err(|e| Error::StoreConnection {
            message: format!("{e}"),
        })?;
        match raw {
            Some(json_str) => {
                let entry: ManagedEntry = serde_json::from_str(&json_str)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if entry.is_expired() {
                    let _ = guard.delete(&ck);
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
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let json_str =
            serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
        let exptime = entry.ttl().map(|t| t.max(1.0) as u32).unwrap_or(0);
        let guard = self.client.lock().await;
        guard
            .set(&ck, json_str, exptime)
            .map_err(|e| Error::StoreConnection {
                message: format!("{e}"),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let guard = self.client.lock().await;
        guard.delete(&ck).map_err(|e| Error::StoreConnection {
            message: format!("{e}"),
        })
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let cname = self.collection_name(collection);
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key, Some(cname)).await?);
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
            results.push(self.ttl(key, Some(cname)).await?);
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
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let json_str =
                serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
            let exptime = entry.ttl().map(|t| t.max(1.0) as u32).unwrap_or(0);
            let ck = Self::compound_key(cname, key);
            let guard = self.client.lock().await;
            guard
                .set(&ck, json_str, exptime)
                .map_err(|e| Error::StoreConnection {
                    message: format!("{e}"),
                })?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let mut count = 0;
        for key in keys {
            if self.delete(key, Some(cname)).await? {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncDestroyStore for MemcachedStore {
    async fn destroy(&self) -> Result<bool> {
        let guard = self.client.lock().await;
        guard.flush().map_err(|e| Error::StoreConnection {
            message: format!("{e}"),
        })?;
        Ok(true)
    }
}
