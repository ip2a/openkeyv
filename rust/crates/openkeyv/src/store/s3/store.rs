use super::client::S3Client;
use super::config::S3Config;
use super::error::{Error, Result, build_err, is_s3_not_found};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use std::collections::HashMap;

/// AWS S3-backed key-value store.
///
/// Each entry is stored as an S3 object with the path `{collection}/{key}`.
/// Values are JSON-serialized `ManagedEntry` bytes.
/// TTL is checked client-side; S3 lifecycle policies can be configured separately.
pub struct S3Store {
    client: S3Client,
    config: S3Config,
}

impl S3Store {
    pub async fn new(bucket_name: impl Into<String>) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);
        let store = Self::with_config(client, S3Config::new(bucket_name, None));
        store.ensure_bucket().await?;
        Ok(store)
    }

    pub fn from_client(client: aws_sdk_s3::Client, bucket_name: impl Into<String>) -> Self {
        Self::with_config(client, S3Config::new(bucket_name, None))
    }

    pub fn with_config(client: aws_sdk_s3::Client, config: S3Config) -> Self {
        Self {
            client: S3Client::new(client),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn bucket_name(&self) -> &str {
        &self.config.bucket_name
    }

    fn client(&self) -> &aws_sdk_s3::Client {
        self.client.client()
    }

    fn s3_key(collection: &str, key: &str) -> String {
        format!("{}/{}", collection, key)
    }

    async fn ensure_bucket(&self) -> Result<()> {
        let exists = self
            .client()
            .head_bucket()
            .bucket(self.bucket_name())
            .send()
            .await
            .is_ok();
        if !exists {
            self.client()
                .create_bucket()
                .bucket(self.bucket_name())
                .send()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("{}", e),
                })?;
        }
        Ok(())
    }

    async fn get_object_bytes(&self, s3_key: &str) -> Result<Option<Vec<u8>>> {
        match self
            .client()
            .get_object()
            .bucket(self.bucket_name())
            .key(s3_key)
            .send()
            .await
        {
            Ok(output) => {
                let bytes = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| Error::StoreConnection {
                        message: e.to_string(),
                    })?
                    .into_bytes();
                Ok(Some(bytes.to_vec()))
            }
            Err(ref e) if is_s3_not_found(e) => Ok(None),
            Err(e) => Err(Error::StoreConnection {
                message: format!("{}", e),
            }),
        }
    }

    async fn put_object_bytes(
        &self,
        s3_key: &str,
        bytes: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let mut req = self
            .client()
            .put_object()
            .bucket(self.bucket_name())
            .key(s3_key)
            .body(bytes.into());
        if let Some(meta) = metadata {
            for (k, v) in meta {
                req = req.metadata(k, v);
            }
        }
        req.send().await.map_err(|e| Error::StoreConnection {
            message: format!("{}", e),
        })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for S3Store {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let sk = Self::s3_key(cname, key);
        match self.get_object_bytes(&sk).await? {
            Some(bytes) => {
                let entry: ManagedEntry = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if entry.is_expired() {
                    let _ = self
                        .client()
                        .delete_object()
                        .bucket(self.bucket_name())
                        .key(&sk)
                        .send()
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
        let sk = Self::s3_key(cname, key);
        match self.get_object_bytes(&sk).await? {
            Some(bytes) => {
                let entry: ManagedEntry = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                if entry.is_expired() {
                    let _ = self
                        .client()
                        .delete_object()
                        .bucket(self.bucket_name())
                        .key(&sk)
                        .send()
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
        let sk = Self::s3_key(cname, key);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let bytes = serde_json::to_vec(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
        let mut metadata = HashMap::new();
        if let Some(dt) = entry.created_at {
            metadata.insert("created-at".to_string(), dt.to_rfc3339());
        }
        if let Some(dt) = entry.expires_at {
            metadata.insert("expires-at".to_string(), dt.to_rfc3339());
        }
        self.put_object_bytes(&sk, bytes, Some(metadata)).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let sk = Self::s3_key(cname, key);
        // S3 delete is idempotent; attempt deletion and treat as success
        self.client()
            .delete_object()
            .bucket(self.bucket_name())
            .key(&sk)
            .send()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
        Ok(true)
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
            let sk = Self::s3_key(cname, key);
            let bytes =
                serde_json::to_vec(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
            let mut metadata = HashMap::new();
            if let Some(dt) = entry.created_at {
                metadata.insert("created-at".to_string(), dt.to_rfc3339());
            }
            if let Some(dt) = entry.expires_at {
                metadata.insert("expires-at".to_string(), dt.to_rfc3339());
            }
            self.put_object_bytes(&sk, bytes, Some(metadata)).await?;
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
impl AsyncCull for S3Store {
    async fn cull(&self) -> Result<()> {
        let mut continuation = None;
        loop {
            let mut req = self.client().list_objects_v2().bucket(self.bucket_name());
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let res = req.send().await.map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
            if let Some(contents) = res.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        if let Some(bytes) = self.get_object_bytes(&key).await? {
                            if let Ok(entry) = serde_json::from_slice::<ManagedEntry>(&bytes) {
                                if entry.is_expired() {
                                    let _ = self
                                        .client()
                                        .delete_object()
                                        .bucket(self.bucket_name())
                                        .key(&key)
                                        .send()
                                        .await;
                                }
                            }
                        }
                    }
                }
            }
            if res.is_truncated != Some(true) {
                break;
            }
            continuation = res.next_continuation_token;
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for S3Store {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let prefix = format!("{}/", cname);
        let limit = limit.unwrap_or(10_000).min(10_000);
        let mut keys = Vec::new();
        let mut continuation = None;
        loop {
            let mut req = self
                .client()
                .list_objects_v2()
                .bucket(self.bucket_name())
                .prefix(&prefix);
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let res = req.send().await.map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
            if let Some(contents) = res.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        if let Some(stripped) = key.strip_prefix(&prefix) {
                            keys.push(stripped.to_string());
                        }
                    }
                }
            }
            if res.is_truncated != Some(true) || keys.len() >= limit {
                break;
            }
            continuation = res.next_continuation_token;
        }
        keys.truncate(limit);
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for S3Store {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000);
        let mut collections = std::collections::HashSet::new();
        let mut continuation = None;
        loop {
            let mut req = self.client().list_objects_v2().bucket(self.bucket_name());
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let res = req.send().await.map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
            if let Some(contents) = res.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        if let Some(pos) = key.find('/') {
                            collections.insert(key[..pos].to_string());
                        }
                    }
                }
            }
            if res.is_truncated != Some(true) || collections.len() >= limit {
                break;
            }
            continuation = res.next_continuation_token;
        }
        let mut result: Vec<String> = collections.into_iter().collect();
        result.truncate(limit);
        Ok(result)
    }
}

#[async_trait]
impl AsyncDestroyCollection for S3Store {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let prefix = format!("{}/", collection);
        let mut keys_to_delete = Vec::new();
        let mut continuation = None;
        loop {
            let mut req = self
                .client()
                .list_objects_v2()
                .bucket(self.bucket_name())
                .prefix(&prefix);
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let res = req.send().await.map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
            if let Some(contents) = res.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        keys_to_delete.push(key);
                    }
                }
            }
            if res.is_truncated != Some(true) {
                break;
            }
            continuation = res.next_continuation_token;
        }
        if keys_to_delete.is_empty() {
            return Ok(false);
        }
        // Batch delete in chunks of 1000 (S3 limit)
        for chunk in keys_to_delete.chunks(1000) {
            let objects: Vec<ObjectIdentifier> = chunk
                .iter()
                .map(|k| {
                    ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .map_err(build_err)
                })
                .collect::<Result<_>>()?;
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .build()
                .map_err(build_err)?;
            self.client()
                .delete_objects()
                .bucket(self.bucket_name())
                .delete(delete)
                .send()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("{}", e),
                })?;
        }
        Ok(true)
    }
}

#[async_trait]
impl AsyncDestroyStore for S3Store {
    async fn destroy(&self) -> Result<bool> {
        // Delete all objects then the bucket
        let mut continuation = None;
        loop {
            let mut req = self.client().list_objects_v2().bucket(self.bucket_name());
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let res = req.send().await.map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
            if let Some(contents) = res.contents {
                let keys: Vec<String> = contents.into_iter().filter_map(|o| o.key).collect();
                if !keys.is_empty() {
                    let objects: Vec<ObjectIdentifier> = keys
                        .iter()
                        .map(|k| {
                            ObjectIdentifier::builder()
                                .key(k)
                                .build()
                                .map_err(build_err)
                        })
                        .collect::<Result<_>>()?;
                    let delete = Delete::builder()
                        .set_objects(Some(objects))
                        .build()
                        .map_err(build_err)?;
                    self.client()
                        .delete_objects()
                        .bucket(self.bucket_name())
                        .delete(delete)
                        .send()
                        .await
                        .map_err(|e| Error::StoreConnection {
                            message: format!("{}", e),
                        })?;
                }
            }
            if res.is_truncated != Some(true) {
                break;
            }
            continuation = res.next_continuation_token;
        }
        self.client()
            .delete_bucket()
            .bucket(self.bucket_name())
            .send()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
        Ok(true)
    }
}
