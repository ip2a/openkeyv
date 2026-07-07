use super::client::FirestoreClient;
use super::config::FirestoreConfig;
use super::error::{Error, Result, map_firestore_err};
use crate::entry::ManagedEntry;
use crate::protocol::AsyncKeyValue;
use crate::value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct FirestoreDoc {
    value: String,
}

/// Google Firestore-backed key-value store.
///
/// Each entry is stored as a Firestore document in a collection.
/// The document contains a single `value` field with the JSON-serialized
/// `ManagedEntry` string.
pub struct FirestoreStore {
    client: FirestoreClient,
    config: FirestoreConfig,
}

impl FirestoreStore {
    pub async fn new(project_id: &str) -> Result<Self> {
        let db = firestore::FirestoreDb::new(project_id)
            .await
            .map_err(map_firestore_err)?;
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
            .map_err(map_firestore_err)?;
        match doc {
            Some(d) => {
                let entry: ManagedEntry = serde_json::from_str(&d.value)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if entry.is_expired() {
                    let _ = self
                        .db()
                        .fluent()
                        .delete()
                        .from(cname)
                        .document_id(key)
                        .execute()
                        .await;
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
        let doc: Option<FirestoreDoc> = self
            .db()
            .fluent()
            .select()
            .by_id_in(cname)
            .obj()
            .one(key)
            .await
            .map_err(map_firestore_err)?;
        match doc {
            Some(d) => {
                let entry: ManagedEntry = serde_json::from_str(&d.value)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if entry.is_expired() {
                    let _ = self
                        .db()
                        .fluent()
                        .delete()
                        .from(cname)
                        .document_id(key)
                        .execute()
                        .await;
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
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let json_str =
            serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
        let doc = FirestoreDoc { value: json_str };

        // Upsert via update (merge); if document doesn't exist, fall back to insert
        match self
            .db()
            .fluent()
            .update()
            .in_col(cname)
            .document_id(key)
            .object(&doc)
            .execute::<FirestoreDoc>()
            .await
        {
            Ok(_) => Ok(()),
            Err(firestore::errors::FirestoreError::DataNotFoundError(_)) => {
                self.db()
                    .fluent()
                    .insert()
                    .into(cname)
                    .document_id(key)
                    .object(&doc)
                    .execute::<FirestoreDoc>()
                    .await
                    .map_err(map_firestore_err)?;
                Ok(())
            }
            Err(e) => Err(map_firestore_err(e)),
        }
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
            .map_err(map_firestore_err)?;
        if exists.is_some() {
            self.db()
                .fluent()
                .delete()
                .from(cname)
                .document_id(key)
                .execute()
                .await
                .map_err(map_firestore_err)?;
            Ok(true)
        } else {
            Ok(false)
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
            let doc = FirestoreDoc { value: json_str };
            match self
                .db()
                .fluent()
                .update()
                .in_col(cname)
                .document_id(key)
                .object(&doc)
                .execute::<FirestoreDoc>()
                .await
            {
                Ok(_) => {}
                Err(firestore::errors::FirestoreError::DataNotFoundError(_)) => {
                    self.db()
                        .fluent()
                        .insert()
                        .into(cname)
                        .document_id(key)
                        .object(&doc)
                        .execute::<FirestoreDoc>()
                        .await
                        .map_err(map_firestore_err)?;
                }
                Err(e) => return Err(map_firestore_err(e)),
            }
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
