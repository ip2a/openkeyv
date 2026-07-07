use super::client::MemcachedClient;
use super::config::MemcachedConfig;
use super::error::{Error, Result, memcached_connection_error};
use crate::entry::ManagedEntry;
use crate::protocol::{AsyncDestroyStore, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;

/// Memcached-backed key-value store.
///
/// Uses the `memcache` crate (sync client) under a Tokio mutex.
/// Values are JSON-serialized `ManagedEntry` strings stored by compound key.
pub struct MemcachedStore {
    client: MemcachedClient,
    config: MemcachedConfig,
}

impl MemcachedStore {
    pub fn new(url: &str) -> Result<Self> {
        let client = memcache::Client::connect(url).map_err(|e| {
            memcached_connection_error(format!("failed to connect to memcached: {e}"))
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
        let raw: Option<String> = guard
            .get(&ck)
            .map_err(|e| memcached_connection_error(format!("{e}")))?;
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

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let guard = self.client().lock().await;
        let raw: Option<String> = guard
            .get(&ck)
            .map_err(|e| memcached_connection_error(format!("{e}")))?;
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
        let json_str =
            serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
        let exptime = entry.ttl().map(|t| t.max(1.0) as u32).unwrap_or(0);
        let guard = self.client().lock().await;
        guard
            .set(&ck, json_str, exptime)
            .map_err(|e| memcached_connection_error(format!("{e}")))?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = Self::compound_key(cname, key);
        let guard = self.client().lock().await;
        guard
            .delete(&ck)
            .map_err(|e| memcached_connection_error(format!("{e}")))
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
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
    ) -> Result<Vec<Option<(Value, f64)>>> {
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
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let json_str =
                serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
            let exptime = entry.ttl().map(|t| t.max(1.0) as u32).unwrap_or(0);
            let ck = Self::compound_key(cname, key);
            let guard = self.client().lock().await;
            guard
                .set(&ck, json_str, exptime)
                .map_err(|e| memcached_connection_error(format!("{e}")))?;
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
        let guard = self.client().lock().await;
        guard
            .flush()
            .map_err(|e| memcached_connection_error(format!("{e}")))?;
        Ok(true)
    }
}
