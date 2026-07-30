use super::client::FirestoreClient;
use super::config::FirestoreConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::AsyncKeyValue;
use crate::value::Value;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use firestore::errors::FirestoreError;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const IDENTITY_PREFIX: &str = "okv1-";
const MAX_IDENTIFIER_BYTES: usize = 1_500;
const MAX_DOCUMENT_NAME_BYTES: usize = 6 * 1024;

fn encode_identifier(value: &str, kind: &str) -> Result<String> {
    let encoded = URL_SAFE_NO_PAD.encode(value.as_bytes());
    let final_len = IDENTITY_PREFIX.len() + encoded.len();
    if final_len > MAX_IDENTIFIER_BYTES {
        return Err(Error::InvalidKey(format!(
            "Firestore {kind} identity encodes to {final_len} bytes (max {MAX_IDENTIFIER_BYTES})"
        )));
    }
    Ok(format!("{IDENTITY_PREFIX}{encoded}"))
}

fn validate_document_name(
    documents_path: &str,
    collection_id: &str,
    document_id: &str,
) -> Result<()> {
    let final_len = documents_path.len() + collection_id.len() + document_id.len() + 2;
    if final_len > MAX_DOCUMENT_NAME_BYTES {
        return Err(Error::InvalidKey(format!(
            "Firestore document name encodes to {final_len} bytes (max {MAX_DOCUMENT_NAME_BYTES})"
        )));
    }
    Ok(())
}

fn encode_batch_identifiers(
    documents_path: &str,
    collection: &str,
    keys: &[String],
) -> Result<(String, Vec<String>, HashMap<String, String>)> {
    let collection_id = encode_identifier(collection, "collection")?;
    let mut seen = HashSet::with_capacity(keys.len());
    let mut document_ids = Vec::with_capacity(keys.len());
    let mut logical_by_document_id = HashMap::with_capacity(keys.len());

    for key in keys {
        if seen.insert(key.as_str()) {
            let document_id = encode_identifier(key, "key")?;
            validate_document_name(documents_path, &collection_id, &document_id)?;
            logical_by_document_id.insert(document_id.clone(), key.clone());
            document_ids.push(document_id);
        }
    }

    Ok((collection_id, document_ids, logical_by_document_id))
}

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
        let collection_id = encode_identifier(cname, "collection")?;
        let document_id = encode_identifier(key, "key")?;
        validate_document_name(self.db().get_documents_path(), &collection_id, &document_id)?;
        let doc: Option<FirestoreDoc> = self
            .db()
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .obj()
            .one(&document_id)
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
                .from(&collection_id)
                .document_id(&document_id)
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

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let cname = self.collection_name(collection);
        let collection_id = encode_identifier(cname, "collection")?;
        let document_id = encode_identifier(key, "key")?;
        validate_document_name(self.db().get_documents_path(), &collection_id, &document_id)?;
        let doc: Option<FirestoreDoc> = self
            .db()
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .obj()
            .one(&document_id)
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
                .from(&collection_id)
                .document_id(&document_id)
                .execute()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!(
                        "failed to delete expired Firestore document {cname}/{key}: {e}"
                    ),
                })?;
            Ok(None)
        } else {
            let ttl = entry.ttl();
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
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        let doc = FirestoreDoc {
            entry: Bytes::from(entry.encode()),
        };
        let collection_id = encode_identifier(cname, "collection")?;
        let document_id = encode_identifier(key, "key")?;
        validate_document_name(self.db().get_documents_path(), &collection_id, &document_id)?;

        self.db()
            .fluent()
            .update()
            .in_col(&collection_id)
            .document_id(&document_id)
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
        let collection_id = encode_identifier(cname, "collection")?;
        let document_id = encode_identifier(key, "key")?;
        validate_document_name(self.db().get_documents_path(), &collection_id, &document_id)?;
        let exists: Option<FirestoreDoc> = self
            .db()
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .obj()
            .one(&document_id)
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
            .from(&collection_id)
            .document_id(&document_id)
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
        let (collection_id, document_ids, logical_by_document_id) =
            encode_batch_identifiers(self.db().get_documents_path(), cname, keys)?;

        let mut stream = self
            .db()
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .obj::<FirestoreDoc>()
            .batch_with_errors(document_ids.clone())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to start Firestore batch read in {cname}: {e}"),
            })?;
        let mut entries = HashMap::with_capacity(document_ids.len());
        let mut returned = HashSet::with_capacity(document_ids.len());
        let mut expired = Vec::new();
        while let Some((document_id, doc)) = stream.try_next().await.map_err(|e| match e {
            FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                "failed to decode a Firestore batch document in {cname}: {e}"
            )),
            _ => Error::StoreConnection {
                message: format!("failed during Firestore batch read in {cname}: {e}"),
            },
        })? {
            let logical_key = logical_by_document_id.get(&document_id).ok_or_else(|| {
                Error::StoreConnection {
                    message: format!(
                        "Firestore batch read returned unknown document ID {document_id:?} in {cname}"
                    ),
                }
            })?;
            if !returned.insert(document_id.clone()) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore batch read returned document {cname}/{logical_key} more than once"
                    ),
                });
            }
            let entry = doc
                .map(|doc| {
                    ManagedEntry::decode(doc.entry).map_err(|e| {
                        Error::Deserialization(format!(
                            "failed to decode OpenKeyV entry in Firestore document {cname}/{logical_key}: {e}"
                        ))
                    })
                })
                .transpose()?;
            if entry.as_ref().is_some_and(ManagedEntry::is_expired) {
                expired.push((logical_key.clone(), document_id));
                entries.insert(logical_key.clone(), None);
            } else {
                entries.insert(logical_key.clone(), entry);
            }
        }

        for document_id in &document_ids {
            if !returned.contains(document_id) {
                let logical_key = &logical_by_document_id[document_id];
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore batch read returned no result for document {cname}/{logical_key}"
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
            for (logical_key, document_id) in &expired {
                batch
                    .delete_by_id(&collection_id, document_id, None)
                    .map_err(|e| Error::StoreConnection {
                        message: format!(
                            "failed to prepare expired Firestore document deletion {cname}/{logical_key}: {e}"
                        ),
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
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let cname = self.collection_name(collection);
        let (collection_id, document_ids, logical_by_document_id) =
            encode_batch_identifiers(self.db().get_documents_path(), cname, keys)?;

        let mut stream = self
            .db()
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .obj::<FirestoreDoc>()
            .batch_with_errors(document_ids.clone())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to start Firestore TTL batch read in {cname}: {e}"),
            })?;
        let mut entries = HashMap::with_capacity(document_ids.len());
        let mut returned = HashSet::with_capacity(document_ids.len());
        let mut expired = Vec::new();
        while let Some((document_id, doc)) = stream.try_next().await.map_err(|e| match e {
            FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                "failed to decode a Firestore TTL batch document in {cname}: {e}"
            )),
            _ => Error::StoreConnection {
                message: format!("failed during Firestore TTL batch read in {cname}: {e}"),
            },
        })? {
            let logical_key = logical_by_document_id.get(&document_id).ok_or_else(|| {
                Error::StoreConnection {
                    message: format!(
                        "Firestore TTL batch read returned unknown document ID {document_id:?} in {cname}"
                    ),
                }
            })?;
            if !returned.insert(document_id.clone()) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore TTL batch read returned document {cname}/{logical_key} more than once"
                    ),
                });
            }
            let entry = doc
                .map(|doc| {
                    ManagedEntry::decode(doc.entry).map_err(|e| {
                        Error::Deserialization(format!(
                            "failed to decode OpenKeyV entry in Firestore document {cname}/{logical_key}: {e}"
                        ))
                    })
                })
                .transpose()?;
            if entry.as_ref().is_some_and(ManagedEntry::is_expired) {
                expired.push((logical_key.clone(), document_id));
                entries.insert(logical_key.clone(), None);
            } else {
                entries.insert(logical_key.clone(), entry);
            }
        }

        for document_id in &document_ids {
            if !returned.contains(document_id) {
                let logical_key = &logical_by_document_id[document_id];
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore TTL batch read returned no result for document {cname}/{logical_key}"
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
            for (logical_key, document_id) in &expired {
                batch
                    .delete_by_id(&collection_id, document_id, None)
                    .map_err(|e| Error::StoreConnection {
                        message: format!(
                            "failed to prepare expired Firestore document deletion {cname}/{logical_key}: {e}"
                        ),
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

        let cname = self.collection_name(collection);
        let collection_id = encode_identifier(cname, "collection")?;
        let mut seen = HashSet::with_capacity(keys.len());
        let mut write_indices = Vec::with_capacity(keys.len());
        for index in (0..keys.len()).rev() {
            if seen.insert(keys[index].as_str()) {
                write_indices.push(index);
            }
        }
        write_indices.reverse();

        let mut writes = Vec::with_capacity(write_indices.len());
        for index in write_indices {
            let key = &keys[index];
            let document_id = encode_identifier(key, "key")?;
            validate_document_name(self.db().get_documents_path(), &collection_id, &document_id)?;
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(values[index].clone(), seconds)?,
                None => ManagedEntry::new(values[index].clone()),
            };
            writes.push((
                key,
                document_id,
                FirestoreDoc {
                    entry: Bytes::from(entry.encode()),
                },
            ));
        }

        let writer =
            self.db()
                .create_simple_batch_writer()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to create Firestore write batch for {cname}: {e}"),
                })?;
        let mut batch = writer.new_batch();
        for (key, document_id, doc) in &writes {
            batch
                .update_object(&collection_id, document_id, doc, None, None, Vec::new())
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
        let (collection_id, document_ids, logical_by_document_id) =
            encode_batch_identifiers(self.db().get_documents_path(), cname, keys)?;

        let mut stream = self
            .db()
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .obj::<FirestoreDoc>()
            .batch_with_errors(document_ids.clone())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!(
                    "failed to start Firestore delete existence batch in {cname}: {e}"
                ),
            })?;
        let mut existing = Vec::new();
        let mut returned = HashSet::with_capacity(document_ids.len());
        while let Some((document_id, doc)) = stream.try_next().await.map_err(|e| match e {
            FirestoreError::DeserializeError(_) => Error::Deserialization(format!(
                "failed to decode a Firestore document before batch delete in {cname}: {e}"
            )),
            _ => Error::StoreConnection {
                message: format!("failed during Firestore delete existence batch in {cname}: {e}"),
            },
        })? {
            let logical_key = logical_by_document_id.get(&document_id).ok_or_else(|| {
                Error::StoreConnection {
                    message: format!(
                        "Firestore delete existence batch returned unknown document ID {document_id:?} in {cname}"
                    ),
                }
            })?;
            if !returned.insert(document_id.clone()) {
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore delete existence batch returned document {cname}/{logical_key} more than once"
                    ),
                });
            }
            if doc.is_some() {
                existing.push((logical_key, document_id));
            }
        }
        for document_id in &document_ids {
            if !returned.contains(document_id) {
                let logical_key = &logical_by_document_id[document_id];
                return Err(Error::StoreConnection {
                    message: format!(
                        "Firestore delete existence batch returned no result for document {cname}/{logical_key}"
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
        for (logical_key, document_id) in &existing {
            batch
                .delete_by_id(&collection_id, document_id, None)
                .map_err(|e| Error::StoreConnection {
                    message: format!(
                        "failed to prepare Firestore document deletion {cname}/{logical_key}: {e}"
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

#[cfg(all(test, feature = "firestore-tests"))]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::value::ValueType;
    use gcloud_sdk::{ExternalJwtFunctionSource, SecretValue, Token, TokenSourceType};

    #[test]
    fn firestore_identity_transport_is_reversible_and_bounded() {
        let logical_values = [
            "",
            "Users",
            "users",
            "é",
            "e\u{301}",
            "/",
            ".",
            "..",
            "__name__",
            "nul\0control\u{1f}",
        ];
        let mut encoded_values = HashSet::new();

        for logical in logical_values {
            let encoded = encode_identifier(logical, "test").unwrap();
            assert!(encoded_values.insert(encoded.clone()));
            assert!(encoded.starts_with(IDENTITY_PREFIX));
            assert!(!encoded.contains('/'));
            assert_ne!(encoded, ".");
            assert_ne!(encoded, "..");
            assert!(!(encoded.starts_with("__") && encoded.ends_with("__")));

            let decoded = URL_SAFE_NO_PAD
                .decode(encoded.strip_prefix(IDENTITY_PREFIX).unwrap())
                .unwrap();
            assert_eq!(decoded, logical.as_bytes());
        }

        let accepted = encode_identifier(&"a".repeat(1121), "test").unwrap();
        assert_eq!(accepted.len(), MAX_IDENTIFIER_BYTES);
        assert!(matches!(
            encode_identifier(&"a".repeat(1122), "test"),
            Err(Error::InvalidKey(_))
        ));
    }

    #[test]
    fn firestore_document_name_enforces_exact_byte_boundary() {
        let collection_id = encode_identifier("collection", "collection").unwrap();
        let document_id = encode_identifier("key", "key").unwrap();
        let separators = 2;
        let accepted_parent = "p"
            .repeat(MAX_DOCUMENT_NAME_BYTES - collection_id.len() - document_id.len() - separators);
        validate_document_name(&accepted_parent, &collection_id, &document_id).unwrap();
        assert!(matches!(
            validate_document_name(&format!("{accepted_parent}p"), &collection_id, &document_id,),
            Err(Error::InvalidKey(_))
        ));
    }

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

        let collection_id = encode_identifier(&collection, "collection").unwrap();
        let document_id = encode_identifier("entry", "key").unwrap();
        let raw = db
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .one(&document_id)
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
        assert!(ttl_values[0].as_ref().unwrap().1.unwrap() > 0.0);
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
    async fn firestore_roundtrips_exact_identities_without_raw_fallback() {
        let (db, store, _) = emulator_store("identities").await;
        let keys = vec![
            "".to_string(),
            "Users".to_string(),
            "users".to_string(),
            "é".to_string(),
            "e\u{301}".to_string(),
            "/".to_string(),
            ".".to_string(),
            "..".to_string(),
            "__name__".to_string(),
            "nul\0control\u{1f}".to_string(),
        ];
        let values: Vec<Value> = keys.iter().map(|key| Value::utf8(key.clone())).collect();
        store.put_many(&keys, &values, None, None).await.unwrap();
        assert_eq!(
            store.get_many(&keys, None).await.unwrap(),
            values.into_iter().map(Some).collect::<Vec<_>>()
        );

        store
            .put("shared", Value::utf8("upper"), Some("Users"), None)
            .await
            .unwrap();
        store
            .put("shared", Value::utf8("lower"), Some("users"), None)
            .await
            .unwrap();
        assert_eq!(
            store.get("shared", Some("Users")).await.unwrap(),
            Some(Value::utf8("upper"))
        );
        assert_eq!(
            store.get("shared", Some("users")).await.unwrap(),
            Some(Value::utf8("lower"))
        );

        let raw_doc = FirestoreDoc {
            entry: Bytes::from(ManagedEntry::new(Value::utf8("raw")).encode()),
        };
        db.fluent()
            .update()
            .in_col("raw-collection")
            .document_id("raw-key")
            .object(&raw_doc)
            .execute::<FirestoreDoc>()
            .await
            .unwrap();
        assert_eq!(
            store.get("raw-key", Some("raw-collection")).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    #[ignore = "requires FIRESTORE_EMULATOR_HOST"]
    async fn firestore_prevalidates_identity_boundaries_before_batch_side_effects() {
        let (_, store, _) = emulator_store("identity-boundaries").await;
        let accepted_key = "a".repeat(1121);
        store
            .put(&accepted_key, Value::utf8("accepted"), None, None)
            .await
            .unwrap();
        assert_eq!(
            store.get(&accepted_key, None).await.unwrap(),
            Some(Value::utf8("accepted"))
        );

        let oversized_key = "a".repeat(1122);
        let put_keys = vec!["must-not-be-written".to_string(), oversized_key.clone()];
        let put_values = vec![Value::utf8("first"), Value::utf8("invalid")];
        assert!(matches!(
            store.put_many(&put_keys, &put_values, None, None).await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(store.get("must-not-be-written", None).await.unwrap(), None);

        store
            .put("must-not-be-deleted", Value::utf8("kept"), None, None)
            .await
            .unwrap();
        let delete_keys = vec!["must-not-be-deleted".to_string(), oversized_key];
        assert!(matches!(
            store.delete_many(&delete_keys, None).await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(
            store.get("must-not-be-deleted", None).await.unwrap(),
            Some(Value::utf8("kept"))
        );

        let oversized_collection = "c".repeat(1122);
        assert!(matches!(
            store
                .put(
                    "key",
                    Value::utf8("value"),
                    Some(&oversized_collection),
                    None,
                )
                .await,
            Err(Error::InvalidKey(_))
        ));
    }

    #[tokio::test]
    #[ignore = "requires FIRESTORE_EMULATOR_HOST"]
    async fn firestore_expired_reads_delete_documents() {
        let (db, store, collection) = emulator_store("expired").await;
        let mut expired = ManagedEntry::new(Value::utf8("value"));
        expired.expires_at = Some(chrono::Utc::now() - chrono::TimeDelta::seconds(1));
        let doc = FirestoreDoc {
            entry: Bytes::from(expired.encode()),
        };
        let collection_id = encode_identifier(&collection, "collection").unwrap();
        for key in ["get-expired", "ttl-expired"] {
            let document_id = encode_identifier(key, "key").unwrap();
            db.fluent()
                .update()
                .in_col(&collection_id)
                .document_id(&document_id)
                .object(&doc)
                .execute::<FirestoreDoc>()
                .await
                .unwrap();
        }

        assert_eq!(store.get("get-expired", None).await.unwrap(), None);
        assert_eq!(store.ttl("ttl-expired", None).await.unwrap(), None);
        let get_document_id = encode_identifier("get-expired", "key").unwrap();
        let ttl_document_id = encode_identifier("ttl-expired", "key").unwrap();
        let get_raw = db
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .one(&get_document_id)
            .await
            .unwrap();
        let ttl_raw = db
            .fluent()
            .select()
            .by_id_in(&collection_id)
            .one(&ttl_document_id)
            .await
            .unwrap();
        assert!(get_raw.is_none());
        assert!(ttl_raw.is_none());

        let batch_keys = vec!["batch-a".to_string(), "batch-b".to_string()];
        for key in &batch_keys {
            let document_id = encode_identifier(key, "key").unwrap();
            db.fluent()
                .update()
                .in_col(&collection_id)
                .document_id(&document_id)
                .object(&doc)
                .execute::<FirestoreDoc>()
                .await
                .unwrap();
        }
        assert_eq!(
            store.get_many(&batch_keys, None).await.unwrap(),
            vec![None, None]
        );
        for key in &batch_keys {
            let document_id = encode_identifier(key, "key").unwrap();
            assert!(
                db.fluent()
                    .select()
                    .by_id_in(&collection_id)
                    .one(&document_id)
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
        let collection_id = encode_identifier(&collection, "collection").unwrap();
        let document_id = encode_identifier("legacy", "key").unwrap();
        db.fluent()
            .update()
            .in_col(&collection_id)
            .document_id(&document_id)
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
