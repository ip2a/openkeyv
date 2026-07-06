use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use async_trait::async_trait;
use futures_util::stream::StreamExt;
use mongodb::{
    Client, Database, IndexModel,
    bson::{DateTime as BsonDateTime, Document, doc},
    options::UpdateOptions,
};
use serde_json::Value;
use std::collections::HashMap;

const DEFAULT_COLLECTION: &str = "default_collection";
const DEFAULT_DB: &str = "kv_store";

/// MongoDB-backed key-value store.
///
/// Each Rust collection maps to a MongoDB collection.
/// Documents store `key`, `value` (as BSON), `created_at`, and `expires_at`.
pub struct MongoStore {
    db: Database,
    default_collection: String,
}

impl MongoStore {
    pub async fn new(url: impl AsRef<str>) -> Result<Self> {
        let client =
            Client::with_uri_str(url.as_ref())
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to connect to mongodb: {e}"),
                })?;
        let db = client.database(DEFAULT_DB);
        Self::from_database(db).await
    }

    pub async fn from_database(db: Database) -> Result<Self> {
        Ok(Self {
            db,
            default_collection: DEFAULT_COLLECTION.to_string(),
        })
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    async fn get_collection(&self, name: &str) -> Result<mongodb::Collection<Document>> {
        let coll = self.db.collection::<Document>(name);

        let key_index = IndexModel::builder().keys(doc! {"key": 1}).build();
        coll.create_index(key_index)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create key index: {e}"),
            })?;

        let ttl_index = IndexModel::builder()
            .keys(doc! {"expires_at": 1})
            .options(
                mongodb::options::IndexOptions::builder()
                    .expire_after(std::time::Duration::from_secs(0))
                    .build(),
            )
            .build();
        coll.create_index(ttl_index)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create ttl index: {e}"),
            })?;

        Ok(coll)
    }

    fn entry_to_doc(key: &str, entry: &ManagedEntry) -> Result<Document> {
        let value_bson = mongodb::bson::to_bson(&entry.value)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let mut doc = doc! {
            "key": key,
            "value": value_bson,
        };
        if let Some(dt) = entry.created_at {
            let st: std::time::SystemTime = dt.into();
            doc.insert("created_at", BsonDateTime::from(st));
        }
        if let Some(dt) = entry.expires_at {
            let st: std::time::SystemTime = dt.into();
            doc.insert("expires_at", BsonDateTime::from(st));
        }
        Ok(doc)
    }

    fn doc_to_entry(doc: &Document) -> Result<ManagedEntry> {
        let value_bson = doc
            .get("value")
            .ok_or_else(|| Error::Deserialization("missing value field".to_string()))?;
        let value: HashMap<String, Value> = mongodb::bson::from_bson(value_bson.clone())
            .map_err(|e| Error::Deserialization(e.to_string()))?;
        let created_at = doc
            .get_datetime("created_at")
            .ok()
            .map(|dt| chrono::DateTime::<chrono::Utc>::from(dt.to_system_time()));
        let expires_at = doc
            .get_datetime("expires_at")
            .ok()
            .map(|dt| chrono::DateTime::<chrono::Utc>::from(dt.to_system_time()));
        Ok(ManagedEntry {
            value,
            created_at,
            expires_at,
        })
    }
}

#[async_trait]
impl AsyncKeyValue for MongoStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        let coll = self.get_collection(cname).await?;
        let doc = coll
            .find_one(doc! {"key": key})
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to get: {e}"),
            })?;
        match doc {
            Some(doc) => {
                let entry = Self::doc_to_entry(&doc)?;
                if entry.is_expired() {
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
        let coll = self.get_collection(cname).await?;
        let doc = coll
            .find_one(doc! {"key": key})
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to get: {e}"),
            })?;
        match doc {
            Some(doc) => {
                let entry = Self::doc_to_entry(&doc)?;
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
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        let coll = self.get_collection(cname).await?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let doc = Self::entry_to_doc(key, &entry)?;
        let update = doc! {"$set": doc};
        let options = UpdateOptions::builder().upsert(true).build();
        coll.update_one(doc! {"key": key}, update)
            .with_options(options)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to put: {e}"),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let coll = self.get_collection(cname).await?;
        let res = coll
            .delete_one(doc! {"key": key})
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to delete: {e}"),
            })?;
        Ok(res.deleted_count > 0)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let cname = self.collection_name(collection);
        let coll = self.get_collection(cname).await?;
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let mut cursor =
            coll.find(doc! {"key": {"$in": keys}})
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to get_many: {e}"),
                })?;
        let mut map = HashMap::with_capacity(keys.len());
        while let Some(doc) = cursor.next().await {
            let doc = doc.map_err(|e| Error::StoreConnection {
                message: format!("failed to read cursor: {e}"),
            })?;
            if let Ok(entry) = Self::doc_to_entry(&doc) {
                if !entry.is_expired() {
                    if let Ok(key) = doc.get_str("key") {
                        map.insert(key.to_string(), entry.value);
                    }
                }
            }
        }
        Ok(keys.iter().map(|k| map.get(k).cloned()).collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let cname = self.collection_name(collection);
        let coll = self.get_collection(cname).await?;
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let mut cursor =
            coll.find(doc! {"key": {"$in": keys}})
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to ttl_many: {e}"),
                })?;
        let mut map = HashMap::with_capacity(keys.len());
        while let Some(doc) = cursor.next().await {
            let doc = doc.map_err(|e| Error::StoreConnection {
                message: format!("failed to read cursor: {e}"),
            })?;
            if let Ok(entry) = Self::doc_to_entry(&doc) {
                if !entry.is_expired() {
                    if let Ok(key) = doc.get_str("key") {
                        let ttl = entry.ttl().unwrap_or(0.0);
                        map.insert(key.to_string(), (entry.value, ttl));
                    }
                }
            }
        }
        Ok(keys.iter().map(|k| map.get(k).cloned()).collect())
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
        let coll = self.get_collection(cname).await?;
        let options = UpdateOptions::builder().upsert(true).build();
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let doc = Self::entry_to_doc(key, &entry)?;
            let update = doc! {"$set": doc};
            coll.update_one(doc! {"key": key}, update)
                .with_options(options.clone())
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to put_many: {e}"),
                })?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let coll = self.get_collection(cname).await?;
        if keys.is_empty() {
            return Ok(0);
        }
        let res = coll
            .delete_many(doc! {"key": {"$in": keys}})
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to delete_many: {e}"),
            })?;
        Ok(res.deleted_count as usize)
    }
}

#[async_trait]
impl AsyncCull for MongoStore {
    async fn cull(&self) -> Result<()> {
        // MongoDB TTL index handles expiration natively; no manual culling needed.
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for MongoStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let coll = self.get_collection(cname).await?;
        let limit = limit.unwrap_or(10_000).min(10_000) as i64;
        let mut cursor = coll
            .find(doc! {})
            .with_options(
                mongodb::options::FindOptions::builder()
                    .projection(doc! {"key": 1})
                    .limit(limit)
                    .build(),
            )
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to list keys: {e}"),
            })?;
        let mut keys = Vec::new();
        while let Some(doc) = cursor.next().await {
            let doc = doc.map_err(|e| Error::StoreConnection {
                message: format!("failed to read cursor: {e}"),
            })?;
            if let Ok(key) = doc.get_str("key") {
                keys.push(key.to_string());
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for MongoStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000);
        let names = self
            .db
            .list_collection_names()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to list collections: {e}"),
            })?;
        Ok(names.into_iter().take(limit).collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for MongoStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let coll = self.db.collection::<Document>(collection);
        coll.drop().await.map_err(|e| Error::StoreConnection {
            message: format!("failed to drop collection: {e}"),
        })?;
        Ok(true)
    }
}

#[async_trait]
impl AsyncDestroyStore for MongoStore {
    async fn destroy(&self) -> Result<bool> {
        let names = self
            .db
            .list_collection_names()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to list collections: {e}"),
            })?;
        for name in names {
            let coll = self.db.collection::<Document>(&name);
            coll.drop().await.map_err(|e| Error::StoreConnection {
                message: format!("failed to drop collection: {e}"),
            })?;
        }
        Ok(true)
    }
}
