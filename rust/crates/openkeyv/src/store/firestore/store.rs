use super::client::FirestoreClient;
use super::config::FirestoreConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::AsyncKeyValue;
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use firestore::errors::FirestoreError;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize)]
struct FirestoreDoc {
    entry: Bytes,
}

/// Google Firestore-backed key-value store.
///
/// Each Firestore document contains one native bytes field with the complete
/// OpenKeyV `OKVE1` entry.
pub struct FirestoreStore {
    client: FirestoreClient,
    config: FirestoreConfig,
}

impl FirestoreStore {
    pub async fn new(project_id: &str) -> Result<Self> {
        let db =
            firestore::FirestoreDb::new(project_id)
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!(
                        "failed to create Firestore client for project {project_id}: {e}"
                    ),
                })?;
        Ok(Self::with_config(db, FirestoreConfig::new(None)))
    }

    pub fn from_db(db: firestore::FirestoreDb) -> Self {
        Self::with_config(db, FirestoreConfig::new(None))
    }

    pub fn with_config(db: firestore::FirestoreDb, config: FirestoreConfig) -> Self {
        Self {
            client: FirestoreClient::new(db),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn db(&self) -> &firestore::FirestoreDb {
        self.client.db()
    }
}

#[async_trait]
impl AsyncKeyValue for FirestoreStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let doc: Option<FirestoreDoc> = self
            .db()
            .fluent()
            .select()
            .by_id_in(cname)
            .obj()
            .one(key)
            .await
            .map_err(|e| match e {
                FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                    "failed to decode Firestore document {cname}/{key}: {e}"
                )),
                _ => Error::StoreConnection {
                    message: format!("failed to read Firestore document {cname}/{key}: {e}"),
                },
            })?;

        let Some(doc) = doc else {
            return Ok(None);
        };
        let entry = ManagedEntry::decode(doc.entry).map_err(|e| {
            Error::Deserialization(format!(
                "failed to decode OpenKeyV entry in Firestore document {cname}/{key}: {e}"
            ))
        })?;
        if entry.is_expired() {
            self.db()
                .fluent()
                .delete()
                .from(cname)
                .document_id(key)
                .execute()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!(
                        "failed to delete expired Firestore document {cname}/{key}: {e}"
                    ),
                })?;
            Ok(None)
        } else {
            Ok(Some(entry.value))
        }
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let cname = self.collection_name(collection);
        let doc: Option<FirestoreDoc> = self
            .db()
            .fluent()
            .select()
            .by_id_in(cname)
            .obj()
            .one(key)
            .await
            .map_err(|e| match e {
                FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                    "failed to decode Firestore document {cname}/{key}: {e}"
                )),
                _ => Error::StoreConnection {
                    message: format!("failed to read Firestore document {cname}/{key}: {e}"),
                },
            })?;

        let Some(doc) = doc else {
            return Ok(None);
        };
        let entry = ManagedEntry::decode(doc.entry).map_err(|e| {
            Error::Deserialization(format!(
                "failed to decode OpenKeyV entry in Firestore document {cname}/{key}: {e}"
            ))
        })?;
        if entry.is_expired() {
            self.db()
                .fluent()
                .delete()
                .from(cname)
                .document_id(key)
                .execute()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!(
                        "failed to delete expired Firestore document {cname}/{key}: {e}"
                    ),
                })?;
            Ok(None)
        } else {
            let ttl = entry.ttl().unwrap_or(0.0);
            Ok(Some((entry.value, ttl)))
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
        let doc = FirestoreDoc {
            entry: Bytes::from(entry.encode()),
        };

        self.db()
            .fluent()
            .update()
            .in_col(cname)
            .document_id(key)
            .object(&doc)
            .execute::<FirestoreDoc>()
            .await
            .map_err(|e| match e {
                FirestoreError::SerializeError(_) => Error::Serialization(format!(
                    "failed to encode Firestore document {cname}/{key}: {e}"
                )),
                _ => Error::StoreConnection {
                    message: format!("failed to write Firestore document {cname}/{key}: {e}"),
                },
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let exists: Option<FirestoreDoc> = self
            .db()
            .fluent()
            .select()
            .by_id_in(cname)
            .obj()
            .one(key)
            .await
            .map_err(|e| match e {
                FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                    "failed to decode Firestore document {cname}/{key}: {e}"
                )),
                _ => Error::StoreConnection {
                    message: format!("failed to read Firestore document {cname}/{key}: {e}"),
                },
            })?;

        if exists.is_none() {
            return Ok(false);
        }
        self.db()
            .fluent()
            .delete()
            .from(cname)
            .document_id(key)
            .execute()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to delete Firestore document {cname}/{key}: {e}"),
            })?;
        Ok(true)
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
        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key.clone());
            }
        }

        let mut stream = self
            .db()
            .fluent()
            .select()
            .by_id_in(cname)
            .obj::<FirestoreDoc>()
            .batch_with_errors(unique_keys.clone())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to start Firestore batch read in {cname}: {e}"),
            })?;
        let mut entries = HashMap::with_capacity(unique_keys.len());
        let mut expired = Vec::new();
        while let Some((key, doc)) = stream.try_next().await.map_err(|e| match e {
            FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                "failed to decode a Firestore batch document in {cname}: {e}"
            )),
            _ => Error::StoreConnection {
                message: format!("failed during Firestore batch read in {cname}: {e}"),
            },
        })? {
            if entries.contains_key(&key) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore batch read returned document {cname}/{key} more than once"
                    ),
                });
            }
            let entry = doc
                .map(|doc| {
                    ManagedEntry::decode(doc.entry).map_err(|e| {
                        Error::Deserialization(format!(
                            "failed to decode OpenKeyV entry in Firestore document {cname}/{key}: {e}"
                        ))
                    })
                })
                .transpose()?;
            if entry.as_ref().is_some_and(ManagedEntry::is_expired) {
                expired.push(key.clone());
                entries.insert(key, None);
            } else {
                entries.insert(key, entry);
            }
        }

        for key in &unique_keys {
            if !entries.contains_key(key) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore batch read returned no result for document {cname}/{key}"
                    ),
                });
            }
        }

        if !expired.is_empty() {
            let writer = self.db().create_simple_batch_writer().await.map_err(|e| {
                Error::StoreConnection {
                    message: format!(
                        "failed to create Firestore expired-entry batch for {cname}: {e}"
                    ),
                }
            })?;
            let mut batch = writer.new_batch();
            for key in &expired {
                batch.delete_by_id(cname, key, None).map_err(|e| {
                    Error::StoreConnection {
                        message: format!(
                            "failed to prepare expired Firestore document deletion {cname}/{key}: {e}"
                        ),
                    }
                })?;
            }
            let expected = batch.writes.len();
            let response = batch.write().await.map_err(|e| Error::StoreConnection {
                message: format!("failed to delete expired Firestore documents in {cname}: {e}"),
            })?;
            if response.statuses.len() != expected {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore expired-entry batch in {cname} returned {} statuses for {expected} writes",
                        response.statuses.len()
                    ),
                });
            }
            for (index, status) in response.statuses.iter().enumerate() {
                if status.code != 0 {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "Firestore expired-entry deletion {index} in {cname} failed with status {}: {}",
                            status.code, status.message
                        ),
                    });
                }
            }
        }

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
    ) -> Result<Vec<Option<(Value, f64)>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let cname = self.collection_name(collection);
        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key.clone());
            }
        }

        let mut stream = self
            .db()
            .fluent()
            .select()
            .by_id_in(cname)
            .obj::<FirestoreDoc>()
            .batch_with_errors(unique_keys.clone())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to start Firestore TTL batch read in {cname}: {e}"),
            })?;
        let mut entries = HashMap::with_capacity(unique_keys.len());
        let mut expired = Vec::new();
        while let Some((key, doc)) = stream.try_next().await.map_err(|e| match e {
            FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                "failed to decode a Firestore TTL batch document in {cname}: {e}"
            )),
            _ => Error::StoreConnection {
                message: format!("failed during Firestore TTL batch read in {cname}: {e}"),
            },
        })? {
            if entries.contains_key(&key) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore TTL batch read returned document {cname}/{key} more than once"
                    ),
                });
            }
            let entry = doc
                .map(|doc| {
                    ManagedEntry::decode(doc.entry).map_err(|e| {
                        Error::Deserialization(format!(
                            "failed to decode OpenKeyV entry in Firestore document {cname}/{key}: {e}"
                        ))
                    })
                })
                .transpose()?;
            if entry.as_ref().is_some_and(ManagedEntry::is_expired) {
                expired.push(key.clone());
                entries.insert(key, None);
            } else {
                entries.insert(key, entry);
            }
        }

        for key in &unique_keys {
            if !entries.contains_key(key) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore TTL batch read returned no result for document {cname}/{key}"
                    ),
                });
            }
        }

        if !expired.is_empty() {
            let writer = self.db().create_simple_batch_writer().await.map_err(|e| {
                Error::StoreConnection {
                    message: format!(
                        "failed to create Firestore expired-entry batch for {cname}: {e}"
                    ),
                }
            })?;
            let mut batch = writer.new_batch();
            for key in &expired {
                batch.delete_by_id(cname, key, None).map_err(|e| {
                    Error::StoreConnection {
                        message: format!(
                            "failed to prepare expired Firestore document deletion {cname}/{key}: {e}"
                        ),
                    }
                })?;
            }
            let expected = batch.writes.len();
            let response = batch.write().await.map_err(|e| Error::StoreConnection {
                message: format!("failed to delete expired Firestore documents in {cname}: {e}"),
            })?;
            if response.statuses.len() != expected {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore expired-entry batch in {cname} returned {} statuses for {expected} writes",
                        response.statuses.len()
                    ),
                });
            }
            for (index, status) in response.statuses.iter().enumerate() {
                if status.code != 0 {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "Firestore expired-entry deletion {index} in {cname} failed with status {}: {}",
                            status.code, status.message
                        ),
                    });
                }
            }
        }

        Ok(keys
            .iter()
            .map(|key| {
                entries.get(key).and_then(|entry| {
                    entry
                        .as_ref()
                        .map(|entry| (entry.value.clone(), entry.ttl().unwrap_or(0.0)))
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
        if keys.is_empty() {
            return Ok(());
        }

        let cname = self.collection_name(collection);
        let mut seen = HashSet::with_capacity(keys.len());
        let mut writes = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(values.iter()).rev() {
            if seen.insert(key.as_str()) {
                writes.push((key, value));
            }
        }
        writes.reverse();

        let writer =
            self.db()
                .create_simple_batch_writer()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to create Firestore write batch for {cname}: {e}"),
                })?;
        let mut batch = writer.new_batch();
        for (key, value) in writes {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let doc = FirestoreDoc {
                entry: Bytes::from(entry.encode()),
            };
            batch
                .update_object(cname, key, &doc, None, None, Vec::new())
                .map_err(|e| match e {
                    FirestoreError::SerializeError(_) => Error::Serialization(format!(
                        "failed to encode Firestore document {cname}/{key}: {e}"
                    )),
                    _ => Error::StoreConnection {
                        message: format!(
                            "failed to prepare Firestore document write {cname}/{key}: {e}"
                        ),
                    },
                })?;
        }

        let expected = batch.writes.len();
        let response = batch.write().await.map_err(|e| Error::StoreConnection {
            message: format!("failed to write Firestore batch in {cname}: {e}"),
        })?;
        if response.statuses.len() != expected {
            return Err(Error::StoreConnection {
                message: format!(
                    "Firestore write batch in {cname} returned {} statuses for {expected} writes",
                    response.statuses.len()
                ),
            });
        }
        for (index, status) in response.statuses.iter().enumerate() {
            if status.code != 0 {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore batch write {index} in {cname} failed with status {}: {}",
                        status.code, status.message
                    ),
                });
            }
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let cname = self.collection_name(collection);
        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key.clone());
            }
        }

        let mut stream = self
            .db()
            .fluent()
            .select()
            .by_id_in(cname)
            .obj::<FirestoreDoc>()
            .batch_with_errors(unique_keys.clone())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!(
                    "failed to start Firestore delete existence batch in {cname}: {e}"
                ),
            })?;
        let mut existing = Vec::new();
        let mut returned = HashSet::with_capacity(unique_keys.len());
        while let Some((key, doc)) = stream.try_next().await.map_err(|e| match e {
            FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                "failed to decode a Firestore document before batch delete in {cname}: {e}"
            )),
            _ => Error::StoreConnection {
                message: format!("failed during Firestore delete existence batch in {cname}: {e}"),
            },
        })? {
            if !returned.insert(key.clone()) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore delete existence batch returned document {cname}/{key} more than once"
                    ),
                });
            }
            if doc.is_some() {
                existing.push(key);
            }
        }
        for key in &unique_keys {
            if !returned.contains(key) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore delete existence batch returned no result for document {cname}/{key}"
                    ),
                });
            }
        }
        if existing.is_empty() {
            return Ok(0);
        }

        let writer =
            self.db()
                .create_simple_batch_writer()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to create Firestore delete batch for {cname}: {e}"),
                })?;
        let mut batch = writer.new_batch();
        for key in &existing {
            batch
                .delete_by_id(cname, key, None)
                .map_err(|e| Error::StoreConnection {
                    message: format!(
                        "failed to prepare Firestore document deletion {cname}/{key}: {e}"
                    ),
                })?;
        }
        let expected = batch.writes.len();
        let response = batch.write().await.map_err(|e| Error::StoreConnection {
            message: format!("failed to delete Firestore batch in {cname}: {e}"),
        })?;
        if response.statuses.len() != expected {
            return Err(Error::StoreConnection {
                message: format!(
                    "Firestore delete batch in {cname} returned {} statuses for {expected} writes",
                    response.statuses.len()
                ),
            });
        }
        for (index, status) in response.statuses.iter().enumerate() {
            if status.code != 0 {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore batch deletion {index} in {cname} failed with status {}: {}",
                        status.code, status.message
                    ),
                });
            }
        }
        Ok(existing.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::value::ValueType;
    use gcloud_sdk::{ExternalJwtFunctionSource, SecretValue, Token, TokenSourceType};

    async fn emulator_db(project: String) -> firestore::FirestoreDb {
        let token_source = ExternalJwtFunctionSource::new(|| async {
            Ok(Token::new(
                "Bearer".to_string(),
                SecretValue::from("owner"),
                chrono::Utc::now() + chrono::Duration::hours(1),
            ))
        });

        firestore::FirestoreDb::with_options_token_source(
            firestore::FirestoreDbOptions::new(project),
            Vec::new(),
            TokenSourceType::ExternalSource(Box::new(token_source)),
        )
        .await
        .unwrap()
    }

    async fn emulator_store(test_name: &str) -> (firestore::FirestoreDb, FirestoreStore, String) {
        let project = format!(
            "openkeyv-{test_name}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_millis()
        );
        let collection = "entries".to_string();
        let db = emulator_db(project).await;
        let store =
            FirestoreStore::with_config(db.clone(), FirestoreConfig::new(Some(collection.clone())));
        (db, store, collection)
    }

    #[tokio::test]
    #[ignore = "requires FIRESTORE_EMULATOR_HOST"]
    async fn firestore_stores_native_okve1_bytes_and_overwrites() {
        let (db, store, collection) = emulator_store("native-bytes").await;

        store
            .put("entry", Value::utf8("first"), None, None)
            .await
            .unwrap();
        assert_eq!(
            store.get("entry", None).await.unwrap(),
            Some(Value::utf8("first"))
        );

        let raw = db
            .fluent()
            .select()
            .by_id_in(&collection)
            .one("entry")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw.fields.len(), 1);
        let stored = raw
            .fields
            .get("entry")
            .unwrap()
            .value_type
            .as_ref()
            .unwrap();
        match stored {
            ValueType::BytesValue(bytes) => assert!(bytes.starts_with(b"OKVE1")),
            other => panic!("expected native Firestore bytes, got {other:?}"),
        }

        store
            .put(
                "entry",
                Value::binary(Bytes::from_static(&[1, 2, 3])),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            store.get("entry", None).await.unwrap(),
            Some(Value::binary(Bytes::from_static(&[1, 2, 3])))
        );
    }

    #[tokio::test]
    #[ignore = "requires FIRESTORE_EMULATOR_HOST"]
    async fn firestore_batches_preserve_order_duplicates_ttl_and_delete_counts() {
        let (_, store, _) = emulator_store("batch").await;
        let keys = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        let values = vec![
            Value::utf8("first-a"),
            Value::utf8("b"),
            Value::utf8("last-a"),
        ];
        store
            .put_many(&keys, &values, None, Some(60.0))
            .await
            .unwrap();

        let read_keys = vec![
            "a".to_string(),
            "missing".to_string(),
            "b".to_string(),
            "a".to_string(),
        ];
        assert_eq!(
            store.get_many(&read_keys, None).await.unwrap(),
            vec![
                Some(Value::utf8("last-a")),
                None,
                Some(Value::utf8("b")),
                Some(Value::utf8("last-a")),
            ]
        );
        let ttl_values = store.ttl_many(&read_keys, None).await.unwrap();
        assert_eq!(ttl_values.len(), 4);
        assert_eq!(
            ttl_values[0].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-a"))
        );
        assert!(ttl_values[0].as_ref().unwrap().1 > 0.0);
        assert!(ttl_values[1].is_none());
        assert_eq!(
            ttl_values[2].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("b"))
        );
        assert_eq!(
            ttl_values[3].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-a"))
        );

        let delete_keys = vec![
            "a".to_string(),
            "a".to_string(),
            "missing".to_string(),
            "b".to_string(),
        ];
        assert_eq!(store.delete_many(&delete_keys, None).await.unwrap(), 2);
        assert_eq!(
            store
                .get_many(&["a".to_string(), "b".to_string()], None)
                .await
                .unwrap(),
            vec![None, None]
        );
        assert_eq!(store.delete_many(&delete_keys, None).await.unwrap(), 0);
    }

    #[tokio::test]
    #[ignore = "requires FIRESTORE_EMULATOR_HOST"]
    async fn firestore_expired_reads_delete_documents() {
        let (db, store, collection) = emulator_store("expired").await;
        store
            .put("get-expired", Value::utf8("value"), None, Some(-1.0))
            .await
            .unwrap();
        store
            .put("ttl-expired", Value::utf8("value"), None, Some(-1.0))
            .await
            .unwrap();

        assert_eq!(store.get("get-expired", None).await.unwrap(), None);
        assert_eq!(store.ttl("ttl-expired", None).await.unwrap(), None);
        let get_raw = db
            .fluent()
            .select()
            .by_id_in(&collection)
            .one("get-expired")
            .await
            .unwrap();
        let ttl_raw = db
            .fluent()
            .select()
            .by_id_in(&collection)
            .one("ttl-expired")
            .await
            .unwrap();
        assert!(get_raw.is_none());
        assert!(ttl_raw.is_none());

        let batch_keys = vec!["batch-a".to_string(), "batch-b".to_string()];
        store
            .put_many(
                &batch_keys,
                &[Value::utf8("a"), Value::utf8("b")],
                None,
                Some(-1.0),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_many(&batch_keys, None).await.unwrap(),
            vec![None, None]
        );
        for key in &batch_keys {
            assert!(
                db.fluent()
                    .select()
                    .by_id_in(&collection)
                    .one(key)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[derive(Serialize, Deserialize)]
    struct OldJsonDocument {
        value: String,
    }

    #[tokio::test]
    #[ignore = "requires FIRESTORE_EMULATOR_HOST"]
    async fn firestore_rejects_old_json_documents() {
        let (db, store, collection) = emulator_store("old-json").await;
        db.fluent()
            .update()
            .in_col(&collection)
            .document_id("legacy")
            .object(&OldJsonDocument {
                value: r#"{"value":{"bytes":[]}}"#.to_string(),
            })
            .execute::<OldJsonDocument>()
            .await
            .unwrap();

        let error = store.get("legacy", None).await.unwrap_err();
        assert!(matches!(error, Error::Deserialization(_)));
    }
}
