use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::AsyncKeyValue;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

const DEFAULT_COLLECTION: &str = "default_collection";

fn compound_key(collection: &str, key: &str) -> String {
    format!("{}:{}", collection, key)
}

fn map_keyring_err(e: keyring::Error) -> Error {
    match e {
        keyring::Error::TooLong(_name, len) => Error::ValueTooLarge {
            size: len as usize,
            max: len as usize,
        },
        _ => Error::StoreConnection {
            message: e.to_string(),
        },
    }
}

/// System keyring-backed key-value store.
///
/// Uses the platform-specific secure credential store (macOS Keychain,
/// Windows Credential Manager, Linux Secret Service, etc.) via the
/// `keyring` crate. Each entry is stored as a password identified by
/// `(service_name, "{collection}:{key}")`.
pub struct KeyringStore {
    service_name: String,
    default_collection: String,
}

impl KeyringStore {
    pub fn new(service_name: Option<&str>) -> Self {
        Self {
            service_name: service_name.unwrap_or("openkeyv").to_string(),
            default_collection: DEFAULT_COLLECTION.to_string(),
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    fn entry(&self, collection: &str, key: &str) -> Result<keyring::Entry> {
        let username = compound_key(collection, key);
        keyring::Entry::new(&self.service_name, &username).map_err(map_keyring_err)
    }
}

#[async_trait]
impl AsyncKeyValue for KeyringStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        let entry = self.entry(cname, key)?;
        match entry.get_password() {
            Ok(json_str) => {
                let managed: ManagedEntry = serde_json::from_str(&json_str)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if managed.is_expired() {
                    let _ = entry.delete_credential();
                    Ok(None)
                } else {
                    Ok(Some(managed.value))
                }
            }
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(map_keyring_err(e)),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let cname = self.collection_name(collection);
        let entry = self.entry(cname, key)?;
        match entry.get_password() {
            Ok(json_str) => {
                let managed: ManagedEntry = serde_json::from_str(&json_str)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if managed.is_expired() {
                    let _ = entry.delete_credential();
                    Ok(None)
                } else {
                    let ttl = managed.ttl().unwrap_or(0.0);
                    Ok(Some((managed.value, ttl)))
                }
            }
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(map_keyring_err(e)),
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
        let entry = self.entry(cname, key)?;
        let managed = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let json_str =
            serde_json::to_string(&managed).map_err(|e| Error::Serialization(e.to_string()))?;
        entry.set_password(&json_str).map_err(map_keyring_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let entry = self.entry(cname, key)?;
        match entry.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring::Error::NoEntry) => Ok(false),
            Err(e) => Err(map_keyring_err(e)),
        }
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
            let managed = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let json_str =
                serde_json::to_string(&managed).map_err(|e| Error::Serialization(e.to_string()))?;
            let entry = self.entry(cname, key)?;
            entry.set_password(&json_str).map_err(map_keyring_err)?;
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
