use super::client::VaultClient;
use super::config::VaultConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::AsyncKeyValue;
use crate::value::Value;
use async_trait::async_trait;
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const BATCH_CONCURRENCY: usize = 32;

fn secret_path(collection: &str, key: &str) -> String {
    let mut identity = String::with_capacity(collection.len() + key.len() + 24);
    identity.push_str(&collection.len().to_string());
    identity.push(':');
    identity.push_str(collection);
    identity.push_str(key);
    URL_SAFE_NO_PAD.encode(identity.as_bytes())
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultSecret {
    entry: String,
}

/// HashiCorp Vault-backed key-value store using KV Secrets Engine v2.
///
/// Each secret contains one canonical base64 field whose decoded bytes are
/// the complete OpenKeyV `OKVE1` entry.
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
            Err(vaultrs::error::ClientError::JsonParseError { source }) => {
                Err(Error::Deserialization(format!(
                    "failed to decode Vault secret {}/{}: {source}",
                    self.client.mount_point(),
                    path
                )))
            }
            Err(error) => Err(Error::StoreConnection {
                message: format!(
                    "failed to read Vault secret {}/{}: {error}",
                    self.client.mount_point(),
                    path
                ),
            }),
        }
    }

    async fn read_entry(&self, path: &str) -> Result<Option<ManagedEntry>> {
        let Some(secret) = self.get_secret(path).await? else {
            return Ok(None);
        };
        let encoded = STANDARD.decode(secret.entry.as_bytes()).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode base64 Vault entry {}/{}: {error}",
                self.client.mount_point(),
                path
            ))
        })?;
        let entry = ManagedEntry::decode(Bytes::from(encoded)).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenKeyV entry in Vault secret {}/{}: {error}",
                self.client.mount_point(),
                path
            ))
        })?;

        if !entry.is_expired() {
            return Ok(Some(entry));
        }

        match vaultrs::kv2::delete_metadata(self.client.client(), self.client.mount_point(), path)
            .await
        {
            Ok(()) | Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => Ok(None),
            Err(error) => Err(Error::StoreConnection {
                message: format!(
                    "failed to delete expired Vault secret {}/{}: {error}",
                    self.client.mount_point(),
                    path
                ),
            }),
        }
    }

    async fn write_entry(&self, path: &str, entry: ManagedEntry) -> Result<()> {
        let secret = VaultSecret {
            entry: STANDARD.encode(entry.encode()),
        };
        vaultrs::kv2::set(
            self.client.client(),
            self.client.mount_point(),
            path,
            &secret,
        )
        .await
        .map_err(|error| match error {
            vaultrs::error::ClientError::JsonParseError { source } => {
                Error::Serialization(format!(
                    "failed to encode Vault secret {}/{}: {source}",
                    self.client.mount_point(),
                    path
                ))
            }
            error => Error::StoreConnection {
                message: format!(
                    "failed to write Vault secret {}/{}: {error}",
                    self.client.mount_point(),
                    path
                ),
            },
        })?;
        Ok(())
    }

    async fn delete_entry(&self, path: &str) -> Result<bool> {
        match vaultrs::kv2::read_metadata(self.client.client(), self.client.mount_point(), path)
            .await
        {
            Ok(_) => {}
            Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => return Ok(false),
            Err(error) => {
                return Err(Error::StoreConnection {
                    message: format!(
                        "failed to read Vault secret metadata {}/{}: {error}",
                        self.client.mount_point(),
                        path
                    ),
                });
            }
        }

        match vaultrs::kv2::delete_metadata(self.client.client(), self.client.mount_point(), path)
            .await
        {
            Ok(()) => Ok(true),
            Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => Ok(false),
            Err(error) => Err(Error::StoreConnection {
                message: format!(
                    "failed to delete Vault secret {}/{}: {error}",
                    self.client.mount_point(),
                    path
                ),
            }),
        }
    }
}

#[async_trait]
impl AsyncKeyValue for VaultStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let path = secret_path(self.collection_name(collection), key);
        Ok(self.read_entry(&path).await?.map(|entry| entry.value))
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let path = secret_path(self.collection_name(collection), key);
        Ok(self.read_entry(&path).await?.map(|entry| {
            let ttl = entry.ttl();
            (entry.value, ttl)
        }))
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let path = secret_path(self.collection_name(collection), key);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        self.write_entry(&path, entry).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let path = secret_path(self.collection_name(collection), key);
        self.delete_entry(&path).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key.clone());
            }
        }

        let collection = self.collection_name(collection);
        let entries: Vec<_> = stream::iter(unique_keys.into_iter().map(|key| {
            let path = secret_path(collection, &key);
            async move {
                let entry = self.read_entry(&path).await?;
                Ok::<_, Error>((key, entry))
            }
        }))
        .buffer_unordered(BATCH_CONCURRENCY)
        .try_collect()
        .await?;
        let entries: HashMap<_, _> = entries.into_iter().collect();

        Ok(keys
            .iter()
            .map(|key| {
                entries
                    .get(key)
                    .and_then(|entry| entry.as_ref().map(|entry| entry.value.clone()))
            })
            .collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key.clone());
            }
        }

        let collection = self.collection_name(collection);
        let entries: Vec<_> = stream::iter(unique_keys.into_iter().map(|key| {
            let path = secret_path(collection, &key);
            async move {
                let entry = self.read_entry(&path).await?;
                Ok::<_, Error>((key, entry))
            }
        }))
        .buffer_unordered(BATCH_CONCURRENCY)
        .try_collect()
        .await?;
        let entries: HashMap<_, _> = entries.into_iter().collect();

        Ok(keys
            .iter()
            .map(|key| {
                entries.get(key).and_then(|entry| {
                    entry
                        .as_ref()
                        .map(|entry| (entry.value.clone(), entry.ttl()))
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
        if let Some(seconds) = ttl {
            ManagedEntry::validate_ttl(seconds)?;
        }
        if keys.is_empty() {
            return Ok(());
        }

        let collection = self.collection_name(collection);
        let mut seen = HashSet::with_capacity(keys.len());
        let mut writes = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(values).rev() {
            if seen.insert(key.as_str()) {
                let entry = match ttl {
                    Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds)?,
                    None => ManagedEntry::new(value.clone()),
                };
                writes.push((secret_path(collection, key), entry));
            }
        }
        writes.reverse();

        stream::iter(
            writes
                .into_iter()
                .map(|(path, entry)| async move { self.write_entry(&path, entry).await }),
        )
        .buffer_unordered(BATCH_CONCURRENCY)
        .try_collect::<Vec<()>>()
        .await?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key.clone());
            }
        }

        let collection = self.collection_name(collection);
        let deleted: Vec<bool> = stream::iter(unique_keys.into_iter().map(|key| {
            let path = secret_path(collection, &key);
            async move { self.delete_entry(&path).await }
        }))
        .buffer_unordered(BATCH_CONCURRENCY)
        .try_collect()
        .await?;

        Ok(deleted.into_iter().filter(|deleted| *deleted).count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn integration_store() -> VaultStore {
        let url = std::env::var("OPENKEYV_VAULT_URL")
            .expect("OPENKEYV_VAULT_URL must point to a Vault dev server");
        let token = std::env::var("OPENKEYV_VAULT_TOKEN")
            .expect("OPENKEYV_VAULT_TOKEN must contain a Vault token");
        VaultStore::new(&url, &token, Some("secret")).unwrap()
    }

    #[test]
    fn vault_paths_are_collision_free_url_safe_segments() {
        let first = secret_path("a:b/集合", "c?%");
        let second = secret_path("a", "b/集合c?%");

        assert_ne!(first, second);
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        );
        assert!(
            second
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        );
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_VAULT_URL and OPENKEYV_VAULT_TOKEN"]
    async fn vault_uses_base64_okve1_and_strict_batch_semantics() {
        let store = integration_store();
        let collection = format!(
            "vault-binary-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );

        store
            .put(
                "single",
                Value::utf8("single-value"),
                Some(&collection),
                Some(30.0),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get("single", Some(&collection)).await.unwrap(),
            Some(Value::utf8("single-value"))
        );
        let (value, ttl) = store
            .ttl("single", Some(&collection))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value, Value::utf8("single-value"));
        let ttl = ttl.unwrap();
        assert!(ttl > 0.0 && ttl <= 30.0);

        let path = secret_path(&collection, "single");
        let raw: serde_json::Value =
            vaultrs::kv2::read(store.client.client(), store.client.mount_point(), &path)
                .await
                .unwrap();
        let raw = raw.as_object().unwrap();
        assert_eq!(raw.len(), 1);
        let encoded = raw.get("entry").unwrap().as_str().unwrap();
        assert!(STANDARD.decode(encoded).unwrap().starts_with(b"OKVE1"));

        store
            .put("c/值", Value::utf8("first-compound-key"), Some("a:b"), None)
            .await
            .unwrap();
        store
            .put(
                "b:c/值",
                Value::utf8("second-compound-key"),
                Some("a"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            store.get("c/值", Some("a:b")).await.unwrap(),
            Some(Value::utf8("first-compound-key"))
        );
        assert_eq!(
            store.get("b:c/值", Some("a")).await.unwrap(),
            Some(Value::utf8("second-compound-key"))
        );

        let write_keys = vec!["one".to_string(), "two".to_string(), "one".to_string()];
        let write_values = vec![
            Value::utf8("first-one"),
            Value::binary(Bytes::from_static(&[0, 255, 1])),
            Value::utf8("last-one"),
        ];
        store
            .put_many(&write_keys, &write_values, Some(&collection), Some(30.0))
            .await
            .unwrap();

        let read_keys = vec![
            "missing".to_string(),
            "one".to_string(),
            "two".to_string(),
            "one".to_string(),
        ];
        assert_eq!(
            store.get_many(&read_keys, Some(&collection)).await.unwrap(),
            vec![
                None,
                Some(Value::utf8("last-one")),
                Some(Value::binary(Bytes::from_static(&[0, 255, 1]))),
                Some(Value::utf8("last-one")),
            ]
        );
        let ttl_values = store.ttl_many(&read_keys, Some(&collection)).await.unwrap();
        assert!(ttl_values[0].is_none());
        assert_eq!(
            ttl_values[1].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-one"))
        );
        assert_eq!(
            ttl_values[2].as_ref().map(|(value, _)| value),
            Some(&Value::binary(Bytes::from_static(&[0, 255, 1])))
        );
        assert_eq!(
            ttl_values[3].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-one"))
        );
        for result in ttl_values.into_iter().flatten() {
            let ttl = result.1.unwrap();
            assert!(ttl > 0.0 && ttl <= 30.0);
        }

        let mut expired = ManagedEntry::new(Value::utf8("expired"));
        expired.expires_at = Some(Utc::now() - chrono::TimeDelta::seconds(1));
        store
            .write_entry(&secret_path(&collection, "expired"), expired)
            .await
            .unwrap();
        assert_eq!(store.get("expired", Some(&collection)).await.unwrap(), None);
        let expired_path = secret_path(&collection, "expired");
        assert!(matches!(
            vaultrs::kv2::read::<serde_json::Value>(
                store.client.client(),
                store.client.mount_point(),
                &expired_path,
            )
            .await,
            Err(vaultrs::error::ClientError::APIError { code: 404, .. })
        ));

        let delete_keys = vec![
            "single".to_string(),
            "one".to_string(),
            "one".to_string(),
            "missing".to_string(),
            "two".to_string(),
        ];
        assert_eq!(
            store
                .delete_many(&delete_keys, Some(&collection))
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            store
                .delete_many(&delete_keys, Some(&collection))
                .await
                .unwrap(),
            0
        );
        assert!(store.delete("c/值", Some("a:b")).await.unwrap());
        assert!(store.delete("b:c/值", Some("a")).await.unwrap());
    }

    #[derive(Serialize)]
    struct OldJsonSecret {
        value: String,
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_VAULT_URL and OPENKEYV_VAULT_TOKEN"]
    async fn vault_rejects_old_json_and_malformed_entries() {
        let store = integration_store();
        let collection = format!(
            "vault-invalid-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );

        let old_path = secret_path(&collection, "old-json");
        vaultrs::kv2::set(
            store.client.client(),
            store.client.mount_point(),
            &old_path,
            &OldJsonSecret {
                value: r#"{"value":null}"#.to_string(),
            },
        )
        .await
        .unwrap();
        let old_error = store.get("old-json", Some(&collection)).await.unwrap_err();
        assert!(matches!(old_error, Error::Deserialization(_)));
        assert!(store.delete("old-json", Some(&collection)).await.unwrap());

        let base64_path = secret_path(&collection, "invalid-base64");
        vaultrs::kv2::set(
            store.client.client(),
            store.client.mount_point(),
            &base64_path,
            &VaultSecret {
                entry: "not base64".to_string(),
            },
        )
        .await
        .unwrap();
        let base64_error = store
            .get("invalid-base64", Some(&collection))
            .await
            .unwrap_err();
        assert!(matches!(base64_error, Error::Deserialization(_)));
        assert!(
            base64_error
                .to_string()
                .contains("failed to decode base64 Vault entry")
        );
        assert!(
            store
                .delete("invalid-base64", Some(&collection))
                .await
                .unwrap()
        );

        let unpadded_path = secret_path(&collection, "unpadded-base64");
        vaultrs::kv2::set(
            store.client.client(),
            store.client.mount_point(),
            &unpadded_path,
            &VaultSecret {
                entry: "T0tWRTE".to_string(),
            },
        )
        .await
        .unwrap();
        let unpadded_error = store
            .get("unpadded-base64", Some(&collection))
            .await
            .unwrap_err();
        assert!(matches!(unpadded_error, Error::Deserialization(_)));
        assert!(
            unpadded_error
                .to_string()
                .contains("failed to decode base64 Vault entry")
        );
        assert!(
            store
                .delete("unpadded-base64", Some(&collection))
                .await
                .unwrap()
        );

        let entry_path = secret_path(&collection, "invalid-entry");
        vaultrs::kv2::set(
            store.client.client(),
            store.client.mount_point(),
            &entry_path,
            &VaultSecret {
                entry: STANDARD.encode(br#"{"value":null}"#),
            },
        )
        .await
        .unwrap();
        let entry_error = store
            .get("invalid-entry", Some(&collection))
            .await
            .unwrap_err();
        assert!(matches!(entry_error, Error::Deserialization(_)));
        assert!(
            entry_error
                .to_string()
                .contains("invalid OpenKeyV entry magic")
        );
        assert!(
            store
                .delete("invalid-entry", Some(&collection))
                .await
                .unwrap()
        );
    }
}
