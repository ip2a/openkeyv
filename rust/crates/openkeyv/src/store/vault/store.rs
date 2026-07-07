use super::client::VaultClient;
use super::config::VaultConfig;
use super::error::{Error, Result, map_vault_err};
use crate::entry::ManagedEntry;
use crate::protocol::AsyncKeyValue;
use crate::value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

fn compound_key(collection: &str, key: &str) -> String {
    format!("{}:{}", collection, key)
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultSecret {
    value: String,
}

/// HashiCorp Vault-backed key-value store using KV Secrets Engine v2.
///
/// Each entry is stored as a secret with the compound key as the path
/// and the JSON-serialized `ManagedEntry` inside a `value` field.
pub struct VaultStore {
    client: VaultClient,
    config: VaultConfig,
}

impl VaultStore {
    pub fn new(url: &str, token: &str, mount_point: Option<&str>) -> Result<Self> {
        let settings = vaultrs::client::VaultClientSettingsBuilder::default()
            .address(url)
            .token(token)
            .build()
            .map_err(|e| Error::StoreSetup {
                message: format!("failed to build vault client settings: {e}"),
            })?;
        let client =
            vaultrs::client::VaultClient::new(settings).map_err(|e| Error::StoreConnection {
                message: format!("failed to create vault client: {e}"),
            })?;
        let config = VaultConfig::new(None, mount_point.map(ToString::to_string));
        Ok(Self::with_config(client, config))
    }

    pub fn from_client(client: vaultrs::client::VaultClient, mount_point: Option<&str>) -> Self {
        let config = VaultConfig::new(None, mount_point.map(ToString::to_string));
        Self::with_config(client, config)
    }

    pub fn with_config(client: vaultrs::client::VaultClient, config: VaultConfig) -> Self {
        let mount_point = config.mount_point.clone();
        Self {
            client: VaultClient::new(client, mount_point),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    async fn get_secret(&self, path: &str) -> Result<Option<VaultSecret>> {
        match vaultrs::kv2::read::<VaultSecret>(
            self.client.client(),
            self.client.mount_point(),
            path,
        )
        .await
        {
            Ok(secret) => Ok(Some(secret)),
            Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => Ok(None),
            Err(e) => Err(map_vault_err(e)),
        }
    }
}

#[async_trait]
impl AsyncKeyValue for VaultStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let path = compound_key(cname, key);
        match self.get_secret(&path).await? {
            Some(secret) => {
                let entry: ManagedEntry = serde_json::from_str(&secret.value)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
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
        let path = compound_key(cname, key);
        match self.get_secret(&path).await? {
            Some(secret) => {
                let entry: ManagedEntry = serde_json::from_str(&secret.value)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
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
        let path = compound_key(cname, key);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let json_str =
            serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
        let secret = VaultSecret { value: json_str };
        vaultrs::kv2::set(
            self.client.client(),
            self.client.mount_point(),
            &path,
            &secret,
        )
        .await
        .map_err(map_vault_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let path = compound_key(cname, key);
        match self.get_secret(&path).await? {
            Some(_) => {
                vaultrs::kv2::delete_metadata(
                    self.client.client(),
                    self.client.mount_point(),
                    &path,
                )
                .await
                .map_err(map_vault_err)?;
                Ok(true)
            }
            None => Ok(false),
        }
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
            let path = compound_key(cname, key);
            let secret = VaultSecret { value: json_str };
            vaultrs::kv2::set(
                self.client.client(),
                self.client.mount_point(),
                &path,
                &secret,
            )
            .await
            .map_err(map_vault_err)?;
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
