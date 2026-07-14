use super::client::KeyringClient;
use super::config::KeyringConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::AsyncKeyValue;
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;

/// System keyring-backed key-value store.
///
/// Uses the platform-specific secure credential store (macOS Keychain,
/// Windows Credential Manager, Linux Secret Service, etc.) via the
/// `keyring` crate. Each entry is stored as a binary secret identified by
/// `(service_name, "{collection byte length}:{collection}{key}")`.
pub struct KeyringStore {
    client: KeyringClient,
    config: KeyringConfig,
}

impl KeyringStore {
    pub fn new(service_name: Option<&str>) -> Self {
        let config = KeyringConfig::new(service_name.map(ToString::to_string), None);
        Self::with_config(config)
    }

    pub fn with_config(config: KeyringConfig) -> Self {
        let service_name = config.service_name.clone();
        Self {
            client: KeyringClient::new(service_name),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }
}

#[async_trait]
impl AsyncKeyValue for KeyringStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let key = key.to_owned();

        tokio::task::spawn_blocking(move || {
            let entry = match client.entry(&collection, &key) {
                Ok(entry) => entry,
                Err(keyring::Error::TooLong(name, max)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' exceeds platform limit of {max} characters"
                    )));
                }
                Err(keyring::Error::Invalid(name, reason)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' is invalid: {reason}"
                    )));
                }
                Err(error) => {
                    return Err(Error::StoreConnection {
                        message: error.to_string(),
                    });
                }
            };

            let secret = match entry.get_secret() {
                Ok(secret) => secret,
                Err(keyring::Error::NoEntry) => return Ok(None),
                Err(keyring::Error::TooLong(name, max)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' exceeds platform limit of {max} characters"
                    )));
                }
                Err(keyring::Error::Invalid(name, reason)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' is invalid: {reason}"
                    )));
                }
                Err(error) => {
                    return Err(Error::StoreConnection {
                        message: error.to_string(),
                    });
                }
            };
            let managed = ManagedEntry::decode(Bytes::from(secret))?;

            if !managed.is_expired() {
                return Ok(Some(managed.value));
            }

            match entry.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => Ok(None),
                Err(keyring::Error::TooLong(name, max)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' exceeds platform limit of {max} characters"
                ))),
                Err(keyring::Error::Invalid(name, reason)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' is invalid: {reason}"
                ))),
                Err(error) => Err(Error::StoreConnection {
                    message: error.to_string(),
                }),
            }
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let key = key.to_owned();

        tokio::task::spawn_blocking(move || {
            let entry = match client.entry(&collection, &key) {
                Ok(entry) => entry,
                Err(keyring::Error::TooLong(name, max)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' exceeds platform limit of {max} characters"
                    )));
                }
                Err(keyring::Error::Invalid(name, reason)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' is invalid: {reason}"
                    )));
                }
                Err(error) => {
                    return Err(Error::StoreConnection {
                        message: error.to_string(),
                    });
                }
            };

            let secret = match entry.get_secret() {
                Ok(secret) => secret,
                Err(keyring::Error::NoEntry) => return Ok(None),
                Err(keyring::Error::TooLong(name, max)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' exceeds platform limit of {max} characters"
                    )));
                }
                Err(keyring::Error::Invalid(name, reason)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' is invalid: {reason}"
                    )));
                }
                Err(error) => {
                    return Err(Error::StoreConnection {
                        message: error.to_string(),
                    });
                }
            };
            let managed = ManagedEntry::decode(Bytes::from(secret))?;

            if !managed.is_expired() {
                let ttl = managed.ttl();
                return Ok(Some((managed.value, ttl)));
            }

            match entry.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => Ok(None),
                Err(keyring::Error::TooLong(name, max)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' exceeds platform limit of {max} characters"
                ))),
                Err(keyring::Error::Invalid(name, reason)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' is invalid: {reason}"
                ))),
                Err(error) => Err(Error::StoreConnection {
                    message: error.to_string(),
                }),
            }
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let managed = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        let encoded = managed.encode();
        let encoded_len = encoded.len();
        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let key = key.to_owned();

        tokio::task::spawn_blocking(move || {
            let entry = match client.entry(&collection, &key) {
                Ok(entry) => entry,
                Err(keyring::Error::TooLong(name, max)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' exceeds platform limit of {max} characters"
                    )));
                }
                Err(keyring::Error::Invalid(name, reason)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' is invalid: {reason}"
                    )));
                }
                Err(error) => {
                    return Err(Error::StoreConnection {
                        message: error.to_string(),
                    });
                }
            };

            match entry.set_secret(&encoded) {
                Ok(()) => Ok(()),
                Err(keyring::Error::TooLong(name, max)) if name == "secret" => {
                    Err(Error::ValueTooLarge {
                        size: encoded_len,
                        max: max as usize,
                    })
                }
                Err(keyring::Error::TooLong(name, max)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' exceeds platform limit of {max} characters"
                ))),
                Err(keyring::Error::Invalid(name, reason)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' is invalid: {reason}"
                ))),
                Err(error) => Err(Error::StoreConnection {
                    message: error.to_string(),
                }),
            }
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let key = key.to_owned();

        tokio::task::spawn_blocking(move || {
            let entry = match client.entry(&collection, &key) {
                Ok(entry) => entry,
                Err(keyring::Error::TooLong(name, max)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' exceeds platform limit of {max} characters"
                    )));
                }
                Err(keyring::Error::Invalid(name, reason)) => {
                    return Err(Error::InvalidKey(format!(
                        "keyring attribute '{name}' is invalid: {reason}"
                    )));
                }
                Err(error) => {
                    return Err(Error::StoreConnection {
                        message: error.to_string(),
                    });
                }
            };

            match entry.delete_credential() {
                Ok(()) => Ok(true),
                Err(keyring::Error::NoEntry) => Ok(false),
                Err(keyring::Error::TooLong(name, max)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' exceeds platform limit of {max} characters"
                ))),
                Err(keyring::Error::Invalid(name, reason)) => Err(Error::InvalidKey(format!(
                    "keyring attribute '{name}' is invalid: {reason}"
                ))),
                Err(error) => Err(Error::StoreConnection {
                    message: error.to_string(),
                }),
            }
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let keys = keys.to_vec();

        tokio::task::spawn_blocking(move || {
            let mut results = Vec::with_capacity(keys.len());

            for key in keys {
                let entry = match client.entry(&collection, &key) {
                    Ok(entry) => entry,
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                };

                let secret = match entry.get_secret() {
                    Ok(secret) => secret,
                    Err(keyring::Error::NoEntry) => {
                        results.push(None);
                        continue;
                    }
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                };
                let managed = ManagedEntry::decode(Bytes::from(secret))?;

                if managed.is_expired() {
                    match entry.delete_credential() {
                        Ok(()) | Err(keyring::Error::NoEntry) => {
                            results.push(None);
                        }
                        Err(keyring::Error::TooLong(name, max)) => {
                            return Err(Error::InvalidKey(format!(
                                "keyring attribute '{name}' exceeds platform limit of {max} characters"
                            )));
                        }
                        Err(keyring::Error::Invalid(name, reason)) => {
                            return Err(Error::InvalidKey(format!(
                                "keyring attribute '{name}' is invalid: {reason}"
                            )));
                        }
                        Err(error) => {
                            return Err(Error::StoreConnection {
                                message: error.to_string(),
                            });
                        }
                    }
                } else {
                    results.push(Some(managed.value));
                }
            }

            Ok(results)
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let keys = keys.to_vec();

        tokio::task::spawn_blocking(move || {
            let mut results = Vec::with_capacity(keys.len());

            for key in keys {
                let entry = match client.entry(&collection, &key) {
                    Ok(entry) => entry,
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                };

                let secret = match entry.get_secret() {
                    Ok(secret) => secret,
                    Err(keyring::Error::NoEntry) => {
                        results.push(None);
                        continue;
                    }
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                };
                let managed = ManagedEntry::decode(Bytes::from(secret))?;

                if managed.is_expired() {
                    match entry.delete_credential() {
                        Ok(()) | Err(keyring::Error::NoEntry) => {
                            results.push(None);
                        }
                        Err(keyring::Error::TooLong(name, max)) => {
                            return Err(Error::InvalidKey(format!(
                                "keyring attribute '{name}' exceeds platform limit of {max} characters"
                            )));
                        }
                        Err(keyring::Error::Invalid(name, reason)) => {
                            return Err(Error::InvalidKey(format!(
                                "keyring attribute '{name}' is invalid: {reason}"
                            )));
                        }
                        Err(error) => {
                            return Err(Error::StoreConnection {
                                message: error.to_string(),
                            });
                        }
                    }
                } else {
                    let ttl = managed.ttl();
                    results.push(Some((managed.value, ttl)));
                }
            }

            Ok(results)
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
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

        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let keys = keys.to_vec();
        let values = values.to_vec();

        tokio::task::spawn_blocking(move || {
            for (key, value) in keys.into_iter().zip(values) {
                let managed = match ttl {
                    Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
                    None => ManagedEntry::new(value),
                };
                let encoded = managed.encode();
                let encoded_len = encoded.len();
                let entry = match client.entry(&collection, &key) {
                    Ok(entry) => entry,
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                };

                match entry.set_secret(&encoded) {
                    Ok(()) => {}
                    Err(keyring::Error::TooLong(name, max)) if name == "secret" => {
                        return Err(Error::ValueTooLarge {
                            size: encoded_len,
                            max: max as usize,
                        });
                    }
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                }
            }

            Ok(())
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let client = self.client.clone();
        let collection = self.collection_name(collection).to_owned();
        let keys = keys.to_vec();

        tokio::task::spawn_blocking(move || {
            let mut count = 0;

            for key in keys {
                let entry = match client.entry(&collection, &key) {
                    Ok(entry) => entry,
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                };

                match entry.delete_credential() {
                    Ok(()) => count += 1,
                    Err(keyring::Error::NoEntry) => {}
                    Err(keyring::Error::TooLong(name, max)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' exceeds platform limit of {max} characters"
                        )));
                    }
                    Err(keyring::Error::Invalid(name, reason)) => {
                        return Err(Error::InvalidKey(format!(
                            "keyring attribute '{name}' is invalid: {reason}"
                        )));
                    }
                    Err(error) => {
                        return Err(Error::StoreConnection {
                            message: error.to_string(),
                        });
                    }
                }
            }

            Ok(count)
        })
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("keyring operation task failed: {error}"),
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeDelta, Utc};

    #[tokio::test]
    #[ignore = "requires access to the platform keyring"]
    async fn keyring_uses_binary_entries_and_strict_batch_semantics() {
        let service_name = format!(
            "openkeyv-test-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let collection = "binary";
        let store = KeyringStore::new(Some(&service_name));
        let values = vec![
            Value::integer(1),
            Value::binary(Bytes::from_static(&[0, 255, 1])),
        ];

        store
            .put(
                "single",
                Value::utf8("single-value"),
                Some(collection),
                Some(30.0),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get("single", Some(collection)).await.unwrap(),
            Some(Value::utf8("single-value"))
        );
        let (value, ttl) = store
            .ttl("single", Some(collection))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value, Value::utf8("single-value"));
        let ttl = ttl.unwrap();
        assert!(ttl > 0.0 && ttl <= 30.0);

        let client = store.client.clone();
        let raw = tokio::task::spawn_blocking(move || {
            client
                .entry(collection, "single")
                .unwrap()
                .get_secret()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(raw.starts_with(b"OKVE1"));

        store
            .put("c", Value::utf8("first-compound-key"), Some("a:b"), None)
            .await
            .unwrap();
        store
            .put("b:c", Value::utf8("second-compound-key"), Some("a"), None)
            .await
            .unwrap();
        assert_eq!(
            store.get("c", Some("a:b")).await.unwrap(),
            Some(Value::utf8("first-compound-key"))
        );
        assert_eq!(
            store.get("b:c", Some("a")).await.unwrap(),
            Some(Value::utf8("second-compound-key"))
        );

        store
            .put_many(
                &["one".to_string(), "two".to_string()],
                &values,
                Some(collection),
                Some(30.0),
            )
            .await
            .unwrap();

        let keys = vec![
            "missing".to_string(),
            "one".to_string(),
            "two".to_string(),
            "one".to_string(),
        ];
        assert_eq!(
            store.get_many(&keys, Some(collection)).await.unwrap(),
            vec![
                None,
                Some(values[0].clone()),
                Some(values[1].clone()),
                Some(values[0].clone()),
            ]
        );

        let ttl_results = store.ttl_many(&keys, Some(collection)).await.unwrap();
        assert!(ttl_results[0].is_none());
        assert_eq!(ttl_results[1].as_ref().unwrap().0, values[0]);
        assert_eq!(ttl_results[2].as_ref().unwrap().0, values[1]);
        assert_eq!(ttl_results[3].as_ref().unwrap().0, values[0]);
        for result in ttl_results.into_iter().flatten() {
            let ttl = result.1.unwrap();
            assert!(ttl > 0.0 && ttl <= 30.0);
        }

        let client = store.client.clone();
        let expired = ManagedEntry {
            value: Value::utf8("expired"),
            created_at: Some(Utc::now() - TimeDelta::seconds(2)),
            expires_at: Some(Utc::now() - TimeDelta::seconds(1)),
        }
        .encode();
        tokio::task::spawn_blocking(move || {
            client
                .entry(collection, "expired")
                .unwrap()
                .set_secret(&expired)
                .unwrap();
        })
        .await
        .unwrap();
        assert_eq!(store.get("expired", Some(collection)).await.unwrap(), None);

        let client = store.client.clone();
        tokio::task::spawn_blocking(move || {
            assert!(matches!(
                client.entry(collection, "expired").unwrap().get_secret(),
                Err(keyring::Error::NoEntry)
            ));
        })
        .await
        .unwrap();

        assert_eq!(
            store
                .delete_many(
                    &[
                        "single".to_string(),
                        "one".to_string(),
                        "two".to_string(),
                        "missing".to_string(),
                    ],
                    Some(collection),
                )
                .await
                .unwrap(),
            3
        );
        assert!(!store.delete("one", Some(collection)).await.unwrap());
        assert!(store.delete("c", Some("a:b")).await.unwrap());
        assert!(store.delete("b:c", Some("a")).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires access to the platform keyring"]
    async fn keyring_rejects_json_entry_payload() {
        let service_name = format!(
            "openkeyv-test-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let collection = "json";
        let store = KeyringStore::new(Some(&service_name));
        let client = store.client.clone();

        tokio::task::spawn_blocking(move || {
            client
                .entry(collection, "old-json")
                .unwrap()
                .set_secret(br#"{"value":null}"#)
                .unwrap();
        })
        .await
        .unwrap();

        let error = store.get("old-json", Some(collection)).await.unwrap_err();
        assert!(error.to_string().contains("invalid OpenKeyV entry magic"));
        assert!(store.delete("old-json", Some(collection)).await.unwrap());
    }
}
