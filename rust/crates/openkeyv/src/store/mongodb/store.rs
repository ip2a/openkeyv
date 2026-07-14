use super::client::MongoDBClient;
use super::config::{DEFAULT_DB, MongoDBConfig};
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::StreamExt;
use mongodb::{
    Client, Collection, IndexModel,
    bson::{
        Binary, Bson, DateTime as BsonDateTime, Document, doc, oid::ObjectId, spec::BinarySubtype,
    },
    options::{DeleteOneModel, IndexOptions, ReplaceOneModel},
};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

const KEY_INDEX_NAME: &str = "openkeyv_key_unique";
const TTL_INDEX_NAME: &str = "openkeyv_expires_ttl";
const DEFAULT_PAGE_SIZE: usize = 10_000;
const PAGE_LIMIT: usize = 10_000;

struct StoredDocument {
    id: ObjectId,
    key: String,
    raw_entry: Bytes,
    entry: ManagedEntry,
    expires_at: Option<BsonDateTime>,
}

impl StoredDocument {
    fn observed_filter(&self) -> Document {
        let mut filter = doc! {
            "_id": self.id,
            "key": &self.key,
            "entry": Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: self.raw_entry.to_vec(),
            }),
        };
        match self.expires_at {
            Some(expires_at) => {
                filter.insert("expires_at", expires_at);
            }
            None => {
                filter.insert("expires_at", doc! { "$exists": false });
            }
        }
        filter
    }
}

/// MongoDB-backed key-value store.
///
/// Each OpenKeyV collection maps to one MongoDB collection. Every document stores
/// a unique string `key`, one generic BSON binary `entry` containing complete
/// `OKVE1`, and an optional BSON datetime `expires_at` used only by MongoDB's TTL
/// index.
pub struct MongoDBStore {
    client: MongoDBClient,
    config: MongoDBConfig,
}

impl MongoDBStore {
    pub async fn new(url: impl AsRef<str>) -> Result<Self> {
        let client =
            Client::with_uri_str(url.as_ref())
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to connect to MongoDB: {error}"),
                })?;
        Self::from_database(client.database(DEFAULT_DB)).await
    }

    pub async fn from_database(db: mongodb::Database) -> Result<Self> {
        Ok(Self::with_config(db, MongoDBConfig::new(None)))
    }

    pub fn with_config(db: mongodb::Database, config: MongoDBConfig) -> Self {
        Self {
            client: MongoDBClient::new(db),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn db(&self) -> &mongodb::Database {
        self.client.db()
    }

    async fn inspect_collection_indexes(
        &self,
        collection: &Collection<Document>,
    ) -> Result<(bool, bool)> {
        let key_pattern = doc! { "key": 1 };
        let ttl_pattern = doc! { "expires_at": 1 };
        let mut key_index_found = false;
        let mut ttl_index_found = false;
        let mut indexes = collection
            .list_indexes()
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to inspect MongoDB indexes for {}: {error}",
                    collection.name()
                ),
            })?;

        while let Some(index) = indexes.next().await {
            let index = index.map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to read MongoDB index metadata for {}: {error}",
                    collection.name()
                ),
            })?;
            let options = index.options.unwrap_or_default();
            let name = options.name.as_deref();

            if index.keys == key_pattern || name == Some(KEY_INDEX_NAME) {
                let valid = index.keys == key_pattern
                    && name == Some(KEY_INDEX_NAME)
                    && options.unique == Some(true)
                    && options.expire_after.is_none()
                    && options.sparse != Some(true)
                    && options.partial_filter_expression.is_none()
                    && options.collation.is_none()
                    && options.hidden != Some(true);
                if !valid || key_index_found {
                    return Err(Error::StoreSetup {
                        message: format!(
                            "MongoDB collection {} has an invalid or duplicate key index",
                            collection.name()
                        ),
                    });
                }
                key_index_found = true;
                continue;
            }

            if index.keys == ttl_pattern || name == Some(TTL_INDEX_NAME) {
                let valid = index.keys == ttl_pattern
                    && name == Some(TTL_INDEX_NAME)
                    && options.expire_after == Some(Duration::ZERO)
                    && options.unique != Some(true)
                    && options.sparse != Some(true)
                    && options.partial_filter_expression.is_none()
                    && options.collation.is_none()
                    && options.hidden != Some(true);
                if !valid || ttl_index_found {
                    return Err(Error::StoreSetup {
                        message: format!(
                            "MongoDB collection {} has an invalid or duplicate TTL index",
                            collection.name()
                        ),
                    });
                }
                ttl_index_found = true;
            }
        }

        Ok((key_index_found, ttl_index_found))
    }

    async fn collection(&self, name: &str) -> Result<Collection<Document>> {
        let collection = self.db().collection::<Document>(name);
        let mut initialized = self.client.initialized_collections().lock().await;
        if initialized.contains(name) {
            return Ok(collection);
        }

        let exists = !self
            .db()
            .list_collection_names()
            .filter(doc! { "name": name })
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!("failed to inspect MongoDB collection {name}: {error}"),
            })?
            .is_empty();
        let (key_index_found, ttl_index_found) = if exists {
            self.inspect_collection_indexes(&collection).await?
        } else {
            (false, false)
        };

        let mut missing_indexes = Vec::with_capacity(2);
        if !key_index_found {
            missing_indexes.push(
                IndexModel::builder()
                    .keys(doc! { "key": 1 })
                    .options(
                        IndexOptions::builder()
                            .name(KEY_INDEX_NAME.to_string())
                            .unique(true)
                            .build(),
                    )
                    .build(),
            );
        }
        if !ttl_index_found {
            missing_indexes.push(
                IndexModel::builder()
                    .keys(doc! { "expires_at": 1 })
                    .options(
                        IndexOptions::builder()
                            .name(TTL_INDEX_NAME.to_string())
                            .expire_after(Duration::ZERO)
                            .build(),
                    )
                    .build(),
            );
        }
        if !missing_indexes.is_empty() {
            collection
                .create_indexes(missing_indexes)
                .await
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to create MongoDB indexes for collection {name}: {error}"
                    ),
                })?;
        }

        let (key_index_found, ttl_index_found) =
            self.inspect_collection_indexes(&collection).await?;
        if !key_index_found || !ttl_index_found {
            return Err(Error::StoreSetup {
                message: format!("MongoDB collection {name} is missing required OpenKeyV indexes"),
            });
        }

        initialized.insert(name.to_string());
        drop(initialized);
        Ok(collection)
    }

    fn entry_document(key: &str, entry: &ManagedEntry) -> Document {
        let mut document = doc! {
            "key": key,
            "entry": Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: entry.encode(),
            }),
        };
        if let Some(expires_at) = entry.expires_at {
            document.insert(
                "expires_at",
                BsonDateTime::from_millis(expires_at.timestamp_millis()),
            );
        }
        document
    }

    fn decode_document(
        mut document: Document,
        expected_key: Option<&str>,
    ) -> Result<StoredDocument> {
        let has_expires_at = document.contains_key("expires_at");
        let expected_fields = if has_expires_at { 4 } else { 3 };
        if document.len() != expected_fields
            || !document.contains_key("_id")
            || !document.contains_key("key")
            || !document.contains_key("entry")
        {
            let mut fields = document.keys().cloned().collect::<Vec<_>>();
            fields.sort();
            return Err(Error::Deserialization(format!(
                "invalid MongoDB document fields: {}",
                fields.join(", ")
            )));
        }

        let id = match document.remove("_id") {
            Some(Bson::ObjectId(id)) => id,
            Some(value) => {
                return Err(Error::Deserialization(format!(
                    "MongoDB _id must be an ObjectId, found {:?}",
                    value.element_type()
                )));
            }
            None => unreachable!("field presence was checked"),
        };
        let key = match document.remove("key") {
            Some(Bson::String(key)) => key,
            Some(value) => {
                return Err(Error::Deserialization(format!(
                    "MongoDB key must be a string, found {:?}",
                    value.element_type()
                )));
            }
            None => unreachable!("field presence was checked"),
        };
        if expected_key.is_some_and(|expected| expected != key) {
            return Err(Error::Deserialization(format!(
                "MongoDB query for key {} returned key {key}",
                expected_key.expect("expected key is present")
            )));
        }
        let binary = match document.remove("entry") {
            Some(Bson::Binary(binary)) if binary.subtype == BinarySubtype::Generic => binary,
            Some(Bson::Binary(binary)) => {
                return Err(Error::Deserialization(format!(
                    "MongoDB entry for key {key} has non-generic binary subtype {:?}",
                    binary.subtype
                )));
            }
            Some(value) => {
                return Err(Error::Deserialization(format!(
                    "MongoDB entry for key {key} must be binary, found {:?}",
                    value.element_type()
                )));
            }
            None => unreachable!("field presence was checked"),
        };
        let expires_at = match document.remove("expires_at") {
            Some(Bson::DateTime(expires_at)) => Some(expires_at),
            Some(value) => {
                return Err(Error::Deserialization(format!(
                    "MongoDB expires_at for key {key} must be a datetime, found {:?}",
                    value.element_type()
                )));
            }
            None => None,
        };

        let raw_entry = Bytes::from(binary.bytes);
        let entry = ManagedEntry::decode(raw_entry.clone()).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode MongoDB OKVE1 entry for key {key}: {error}"
            ))
        })?;
        let embedded_expires_at = entry
            .expires_at
            .map(|expires_at| expires_at.timestamp_millis());
        let indexed_expires_at = expires_at.map(|expires_at| expires_at.timestamp_millis());
        if indexed_expires_at != embedded_expires_at {
            return Err(Error::Deserialization(format!(
                "MongoDB expires_at does not match OKVE1 metadata for key {key}"
            )));
        }

        Ok(StoredDocument {
            id,
            key,
            raw_entry,
            entry,
            expires_at,
        })
    }

    async fn delete_observed_expired(
        &self,
        collection: &Collection<Document>,
        document: &StoredDocument,
    ) -> Result<()> {
        collection
            .delete_one(document.observed_filter())
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to conditionally delete expired MongoDB key {} from {}: {error}",
                    document.key,
                    collection.name()
                ),
            })?;
        Ok(())
    }

    async fn delete_observed_expired_many(
        &self,
        collection: &Collection<Document>,
        documents: &[StoredDocument],
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        let namespace = collection.namespace();
        let models = documents.iter().map(|document| {
            DeleteOneModel::builder()
                .namespace(namespace.clone())
                .filter(document.observed_filter())
                .build()
        });
        self.db()
            .client()
            .bulk_write(models)
            .ordered(false)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to conditionally delete expired MongoDB documents from {}: {error}",
                    collection.name()
                ),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for MongoDBStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let collection = self.collection(self.collection_name(collection)).await?;
        let document = collection
            .find_one(doc! { "key": key })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to get MongoDB key {key} from {}: {error}",
                    collection.name()
                ),
            })?;
        let Some(document) = document else {
            return Ok(None);
        };
        let document = Self::decode_document(document, Some(key))?;
        if document.entry.is_expired() {
            self.delete_observed_expired(&collection, &document).await?;
            return Ok(None);
        }
        Ok(Some(document.entry.value))
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let collection = self.collection(self.collection_name(collection)).await?;
        let document = collection
            .find_one(doc! { "key": key })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to get MongoDB TTL for key {key} from {}: {error}",
                    collection.name()
                ),
            })?;
        let Some(document) = document else {
            return Ok(None);
        };
        let document = Self::decode_document(document, Some(key))?;
        if document.entry.is_expired() {
            self.delete_observed_expired(&collection, &document).await?;
            return Ok(None);
        }
        let ttl = document.entry.ttl().unwrap_or(0.0);
        Ok(Some((document.entry.value, ttl)))
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let collection = self.collection(self.collection_name(collection)).await?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        collection
            .replace_one(doc! { "key": key }, Self::entry_document(key, &entry))
            .upsert(true)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to put MongoDB key {key} into {}: {error}",
                    collection.name()
                ),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let collection = self.collection(self.collection_name(collection)).await?;
        let result = collection
            .delete_one(doc! { "key": key })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to delete MongoDB key {key} from {}: {error}",
                    collection.name()
                ),
            })?;
        Ok(result.deleted_count == 1)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let collection = self.collection(self.collection_name(collection)).await?;
        let requested = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let query_keys = requested.iter().copied().collect::<Vec<_>>();
        let mut cursor = collection
            .find(doc! { "key": { "$in": query_keys } })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to get MongoDB keys from {}: {error}",
                    collection.name()
                ),
            })?;
        let mut values = HashMap::with_capacity(requested.len());
        let mut expired = Vec::new();
        while let Some(document) = cursor.next().await {
            let document = document.map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to read MongoDB batch cursor for {}: {error}",
                    collection.name()
                ),
            })?;
            let document = Self::decode_document(document, None)?;
            if !requested.contains(document.key.as_str()) {
                return Err(Error::Deserialization(format!(
                    "MongoDB batch query returned unrequested key {}",
                    document.key
                )));
            }
            if document.entry.is_expired() {
                expired.push(document);
                continue;
            }
            if values
                .insert(document.key.clone(), document.entry.value.clone())
                .is_some()
            {
                return Err(Error::Deserialization(format!(
                    "MongoDB batch query returned duplicate key {}",
                    document.key
                )));
            }
        }
        self.delete_observed_expired_many(&collection, &expired)
            .await?;
        Ok(keys
            .iter()
            .map(|key| values.get(key.as_str()).cloned())
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

        let collection = self.collection(self.collection_name(collection)).await?;
        let requested = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let query_keys = requested.iter().copied().collect::<Vec<_>>();
        let mut cursor = collection
            .find(doc! { "key": { "$in": query_keys } })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to get MongoDB TTL batch from {}: {error}",
                    collection.name()
                ),
            })?;
        let mut values = HashMap::with_capacity(requested.len());
        let mut expired = Vec::new();
        while let Some(document) = cursor.next().await {
            let document = document.map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to read MongoDB TTL cursor for {}: {error}",
                    collection.name()
                ),
            })?;
            let document = Self::decode_document(document, None)?;
            if !requested.contains(document.key.as_str()) {
                return Err(Error::Deserialization(format!(
                    "MongoDB TTL batch returned unrequested key {}",
                    document.key
                )));
            }
            if document.entry.is_expired() {
                expired.push(document);
                continue;
            }
            let ttl = document.entry.ttl().unwrap_or(0.0);
            if values
                .insert(document.key.clone(), (document.entry.value.clone(), ttl))
                .is_some()
            {
                return Err(Error::Deserialization(format!(
                    "MongoDB TTL batch returned duplicate key {}",
                    document.key
                )));
            }
        }
        self.delete_observed_expired_many(&collection, &expired)
            .await?;
        Ok(keys
            .iter()
            .map(|key| values.get(key.as_str()).cloned())
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

        let collection = self.collection(self.collection_name(collection)).await?;
        let mut last_indices = HashMap::with_capacity(keys.len());
        for (index, key) in keys.iter().enumerate() {
            last_indices.insert(key.as_str(), index);
        }

        let namespace = collection.namespace();
        let mut models = Vec::with_capacity(last_indices.len());
        for index in last_indices.into_values() {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(values[index].clone(), seconds)?,
                None => ManagedEntry::new(values[index].clone()),
            };
            models.push(
                ReplaceOneModel::builder()
                    .namespace(namespace.clone())
                    .filter(doc! { "key": &keys[index] })
                    .replacement(Self::entry_document(&keys[index], &entry))
                    .upsert(true)
                    .build(),
            );
        }
        let expected_writes = models.len() as i64;
        let result = self
            .db()
            .client()
            .bulk_write(models)
            .ordered(false)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to put MongoDB batch into {}: {error}",
                    collection.name()
                ),
            })?;
        if result.matched_count + result.upserted_count != expected_writes {
            return Err(Error::StoreConnection {
                message: format!(
                    "MongoDB batch write for {} acknowledged {} of {expected_writes} replacements",
                    collection.name(),
                    result.matched_count + result.upserted_count
                ),
            });
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let collection = self.collection(self.collection_name(collection)).await?;
        let unique = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let query_keys = unique.iter().copied().collect::<Vec<_>>();
        let result = collection
            .delete_many(doc! { "key": { "$in": query_keys } })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to delete MongoDB batch from {}: {error}",
                    collection.name()
                ),
            })?;
        usize::try_from(result.deleted_count).map_err(|_| Error::StoreConnection {
            message: format!(
                "MongoDB deleted count for {} does not fit usize: {}",
                collection.name(),
                result.deleted_count
            ),
        })
    }
}

#[async_trait]
impl AsyncCull for MongoDBStore {
    async fn cull(&self) -> Result<()> {
        let collection_names =
            self.db()
                .list_collection_names()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to list MongoDB collections for cull: {error}"),
                })?;
        let now = BsonDateTime::from_millis(chrono::Utc::now().timestamp_millis());
        for collection_name in collection_names {
            let collection = self.collection(&collection_name).await?;
            let mut cursor = collection
                .find(doc! { "expires_at": { "$lte": now } })
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to find expired MongoDB documents in {collection_name}: {error}"
                    ),
                })?;
            let mut expired = Vec::new();
            while let Some(document) = cursor.next().await {
                let document = document.map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to read expired MongoDB cursor for {collection_name}: {error}"
                    ),
                })?;
                let document = Self::decode_document(document, None)?;
                if !document.entry.is_expired() {
                    return Err(Error::Deserialization(format!(
                        "MongoDB expiration query returned live key {} from {collection_name}",
                        document.key
                    )));
                }
                expired.push(document);
            }
            self.delete_observed_expired_many(&collection, &expired)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for MongoDBStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let collection = self.collection(self.collection_name(collection)).await?;
        let now = BsonDateTime::from_millis(chrono::Utc::now().timestamp_millis());
        let mut cursor = collection
            .find(doc! {
                "$or": [
                    { "expires_at": { "$exists": false } },
                    { "expires_at": { "$gt": now } },
                ]
            })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to enumerate MongoDB keys in {}: {error}",
                    collection.name()
                ),
            })?;
        let mut keys = Vec::with_capacity(limit);
        let mut expired = Vec::new();
        while let Some(document) = cursor.next().await {
            let document = document.map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to read MongoDB key cursor for {}: {error}",
                    collection.name()
                ),
            })?;
            let document = Self::decode_document(document, None)?;
            if document.entry.is_expired() {
                expired.push(document);
                continue;
            }
            keys.push(document.key.clone());
            if keys.len() == limit {
                break;
            }
        }
        self.delete_observed_expired_many(&collection, &expired)
            .await?;
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for MongoDBStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let names =
            self.db()
                .list_collection_names()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to enumerate MongoDB collections: {error}"),
                })?;
        Ok(names.into_iter().take(limit).collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for MongoDBStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let exists = !self
            .db()
            .list_collection_names()
            .filter(doc! { "name": collection })
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to inspect MongoDB collection {collection}: {error}"),
            })?
            .is_empty();
        if !exists {
            self.client
                .initialized_collections()
                .lock()
                .await
                .remove(collection);
            return Ok(false);
        }

        self.db()
            .collection::<Document>(collection)
            .drop()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to drop MongoDB collection {collection}: {error}"),
            })?;
        self.client
            .initialized_collections()
            .lock()
            .await
            .remove(collection);
        Ok(true)
    }
}

#[async_trait]
impl AsyncDestroyStore for MongoDBStore {
    async fn destroy(&self) -> Result<bool> {
        let names =
            self.db()
                .list_collection_names()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to list MongoDB collections for destruction: {error}"),
                })?;
        if names.is_empty() {
            self.client.initialized_collections().lock().await.clear();
            return Ok(false);
        }

        for name in names {
            self.db()
                .collection::<Document>(&name)
                .drop()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to drop MongoDB collection {name}: {error}"),
                })?;
            self.client
                .initialized_collections()
                .lock()
                .await
                .remove(&name);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn fixed_entry(expires_at_millis: Option<i64>) -> ManagedEntry {
        ManagedEntry {
            value: Value::utf8("value"),
            created_at: Some(Utc.timestamp_millis_opt(1_784_000_000_000).unwrap()),
            expires_at: expires_at_millis.map(|millis| Utc.timestamp_millis_opt(millis).unwrap()),
        }
    }

    fn stored_document(entry: &ManagedEntry) -> Document {
        let mut document = MongoDBStore::entry_document("key", entry);
        document.insert("_id", ObjectId::new());
        document
    }

    #[test]
    fn mongodb_document_uses_exact_binary_shape() {
        let entry = fixed_entry(Some(1_784_000_030_000));
        let document = stored_document(&entry);

        assert_eq!(document.len(), 4);
        assert!(matches!(document.get("_id"), Some(Bson::ObjectId(_))));
        assert_eq!(document.get_str("key").unwrap(), "key");
        let binary = document.get_binary_generic("entry").unwrap();
        assert_eq!(&binary[..5], b"OKVE1");
        assert_eq!(
            document
                .get_datetime("expires_at")
                .unwrap()
                .timestamp_millis(),
            1_784_000_030_000
        );
        assert!(!document.contains_key("value"));
        assert!(!document.contains_key("created_at"));

        let decoded = MongoDBStore::decode_document(document, Some("key")).unwrap();
        assert_eq!(decoded.entry, entry);
    }

    #[test]
    fn mongodb_document_rejects_old_extra_and_malformed_fields() {
        let old = doc! {
            "_id": ObjectId::new(),
            "key": "old",
            "value": { "bytes": [1, 2, 3] },
            "created_at": BsonDateTime::now(),
        };
        assert!(MongoDBStore::decode_document(old, Some("old")).is_err());

        let mut extra = stored_document(&fixed_entry(None));
        extra.insert("collection", "default");
        assert!(MongoDBStore::decode_document(extra, Some("key")).is_err());

        let mut wrong_subtype = stored_document(&fixed_entry(None));
        wrong_subtype.insert(
            "entry",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Uuid,
                bytes: vec![0; 16],
            }),
        );
        assert!(MongoDBStore::decode_document(wrong_subtype, Some("key")).is_err());

        let mut wrong_id = stored_document(&fixed_entry(None));
        wrong_id.insert("_id", "not-an-object-id");
        assert!(MongoDBStore::decode_document(wrong_id, Some("key")).is_err());
    }

    #[test]
    fn mongodb_document_rejects_expiration_mismatch() {
        let entry = fixed_entry(Some(1_784_000_030_000));
        let mut missing = stored_document(&entry);
        missing.remove("expires_at");
        assert!(MongoDBStore::decode_document(missing, Some("key")).is_err());

        let mut different = stored_document(&entry);
        different.insert("expires_at", BsonDateTime::from_millis(1_784_000_030_001));
        assert!(MongoDBStore::decode_document(different, Some("key")).is_err());

        let mut unexpected = stored_document(&fixed_entry(None));
        unexpected.insert("expires_at", BsonDateTime::from_millis(1_784_000_030_000));
        assert!(MongoDBStore::decode_document(unexpected, Some("key")).is_err());
    }

    fn mongodb_url() -> String {
        std::env::var("OPENKEYV_MONGODB_URL")
            .unwrap_or_else(|_| "mongodb://127.0.0.1:27018".to_string())
    }

    async fn isolated_database() -> mongodb::Database {
        let client = Client::with_uri_str(mongodb_url()).await.unwrap();
        client.database(&format!("openkeyv_test_{}", ObjectId::new().to_hex()))
    }

    #[tokio::test]
    #[ignore = "requires MongoDB 8.0 configured by OPENKEYV_MONGODB_URL"]
    async fn mongodb_binary_shape_indexes_and_replacement_are_strict() {
        let db = isolated_database().await;
        let store = MongoDBStore::from_database(db.clone()).await.unwrap();

        store
            .put("key", Value::utf8("ttl"), Some("entries"), Some(60.0))
            .await
            .unwrap();
        let collection = db.collection::<Document>("entries");
        let document = collection
            .find_one(doc! { "key": "key" })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(document.len(), 4);
        assert_eq!(
            document.get_binary_generic("entry").unwrap()[..5],
            *b"OKVE1"
        );
        assert!(document.get_datetime("expires_at").is_ok());

        let (key_index, ttl_index) = store.inspect_collection_indexes(&collection).await.unwrap();
        assert!(key_index);
        assert!(ttl_index);
        assert!(
            collection
                .insert_one(MongoDBStore::entry_document(
                    "key",
                    &ManagedEntry::new(Value::utf8("duplicate"))
                ))
                .await
                .is_err()
        );

        store
            .put("key", Value::utf8("without-ttl"), Some("entries"), None)
            .await
            .unwrap();
        let document = collection
            .find_one(doc! { "key": "key" })
            .await
            .unwrap()
            .unwrap();
        assert!(!document.contains_key("expires_at"));
        assert_eq!(
            store.get("key", Some("entries")).await.unwrap(),
            Some(Value::utf8("without-ttl"))
        );

        assert!(store.destroy_collection("entries").await.unwrap());
        assert!(!store.destroy_collection("entries").await.unwrap());
        db.drop().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires MongoDB 8.0 configured by OPENKEYV_MONGODB_URL"]
    async fn mongodb_native_batches_cleanup_and_destroy_are_strict() {
        let db = isolated_database().await;
        let store = MongoDBStore::from_database(db.clone()).await.unwrap();
        let keys = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        let values = vec![
            Value::utf8("first"),
            Value::utf8("second"),
            Value::utf8("last"),
        ];
        store
            .put_many(&keys, &values, Some("entries"), None)
            .await
            .unwrap();

        let requested = vec![
            "b".to_string(),
            "missing".to_string(),
            "a".to_string(),
            "b".to_string(),
        ];
        assert_eq!(
            store.get_many(&requested, Some("entries")).await.unwrap(),
            vec![
                Some(Value::utf8("second")),
                None,
                Some(Value::utf8("last")),
                Some(Value::utf8("second")),
            ]
        );
        assert_eq!(
            store
                .delete_many(
                    &["a".to_string(), "a".to_string(), "missing".to_string()],
                    Some("entries")
                )
                .await
                .unwrap(),
            1
        );

        let collection = store.collection("entries").await.unwrap();
        let expired = ManagedEntry {
            value: Value::utf8("expired"),
            created_at: Some(Utc::now() - chrono::TimeDelta::seconds(10)),
            expires_at: Some(Utc::now() - chrono::TimeDelta::seconds(5)),
        };
        collection
            .replace_one(
                doc! { "key": "expired" },
                MongoDBStore::entry_document("expired", &expired),
            )
            .upsert(true)
            .await
            .unwrap();
        assert_eq!(store.get("expired", Some("entries")).await.unwrap(), None);
        assert!(
            collection
                .find_one(doc! { "key": "expired" })
                .await
                .unwrap()
                .is_none()
        );

        collection
            .replace_one(
                doc! { "key": "cull" },
                MongoDBStore::entry_document("cull", &expired),
            )
            .upsert(true)
            .await
            .unwrap();
        store.cull().await.unwrap();
        assert!(
            collection
                .find_one(doc! { "key": "cull" })
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.keys(Some("entries"), None).await.unwrap(),
            vec!["b".to_string()]
        );

        collection
            .insert_one(doc! {
                "key": "legacy",
                "value": { "object": "old-json" },
            })
            .await
            .unwrap();
        assert!(store.get("legacy", Some("entries")).await.is_err());
        assert!(store.keys(Some("entries"), None).await.is_err());

        assert!(store.destroy().await.unwrap());
        assert!(!store.destroy().await.unwrap());
        db.drop().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires MongoDB 8.0 configured by OPENKEYV_MONGODB_URL"]
    async fn mongodb_conditional_cleanup_preserves_concurrent_replacement() {
        let db = isolated_database().await;
        let store = MongoDBStore::from_database(db.clone()).await.unwrap();
        let collection = store.collection("entries").await.unwrap();
        let expired = ManagedEntry {
            value: Value::utf8("expired"),
            created_at: Some(Utc::now() - chrono::TimeDelta::seconds(10)),
            expires_at: Some(Utc::now() - chrono::TimeDelta::seconds(5)),
        };
        collection
            .insert_one(MongoDBStore::entry_document("race", &expired))
            .await
            .unwrap();
        let observed = MongoDBStore::decode_document(
            collection
                .find_one(doc! { "key": "race" })
                .await
                .unwrap()
                .unwrap(),
            Some("race"),
        )
        .unwrap();

        store
            .put("race", Value::utf8("replacement"), Some("entries"), None)
            .await
            .unwrap();
        store
            .delete_observed_expired(&collection, &observed)
            .await
            .unwrap();
        assert_eq!(
            store.get("race", Some("entries")).await.unwrap(),
            Some(Value::utf8("replacement"))
        );
        db.drop().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires MongoDB 8.0 configured by OPENKEYV_MONGODB_URL"]
    async fn mongodb_rejects_conflicting_indexes() {
        let db = isolated_database().await;
        let collection = db.collection::<Document>("entries");
        collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "key": 1 })
                    .options(
                        IndexOptions::builder()
                            .name("wrong_key_index".to_string())
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();
        let store = MongoDBStore::from_database(db.clone()).await.unwrap();

        let error = store.get("key", Some("entries")).await.unwrap_err();
        assert!(matches!(error, Error::StoreSetup { .. }));
        db.drop().await.unwrap();
    }
}
