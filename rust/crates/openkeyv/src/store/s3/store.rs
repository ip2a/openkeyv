use super::client::S3Client;
use super::config::S3Config;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use bytes::Bytes;

/// AWS S3-backed key-value store.
///
/// Each entry is stored as an S3 object with the path `{collection}/{key}`.
/// Object bodies use the OpenKeyV `OKVE1` binary entry format.
/// TTL is checked and enforced client-side.
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
        match self
            .client()
            .head_bucket()
            .bucket(self.bucket_name())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_not_found()) =>
            {
                self.client()
                    .create_bucket()
                    .bucket(self.bucket_name())
                    .send()
                    .await
                    .map_err(|error| Error::StoreConnection {
                        message: error.to_string(),
                    })?;
                Ok(())
            }
            Err(error) => Err(Error::StoreConnection {
                message: error.to_string(),
            }),
        }
    }

    async fn get_object_bytes(&self, s3_key: &str) -> Result<Option<Bytes>> {
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
                Ok(Some(bytes))
            }
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_no_such_key()) =>
            {
                Ok(None)
            }
            Err(error) => Err(Error::StoreConnection {
                message: error.to_string(),
            }),
        }
    }

    async fn put_object_bytes(&self, s3_key: &str, bytes: Vec<u8>) -> Result<()> {
        self.client()
            .put_object()
            .bucket(self.bucket_name())
            .key(s3_key)
            .body(bytes.into())
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: error.to_string(),
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
                let entry = ManagedEntry::decode(bytes)?;
                if entry.is_expired() {
                    self.client()
                        .delete_object()
                        .bucket(self.bucket_name())
                        .key(&sk)
                        .send()
                        .await
                        .map_err(|error| Error::StoreConnection {
                            message: error.to_string(),
                        })?;
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
                let entry = ManagedEntry::decode(bytes)?;
                if entry.is_expired() {
                    self.client()
                        .delete_object()
                        .bucket(self.bucket_name())
                        .key(&sk)
                        .send()
                        .await
                        .map_err(|error| Error::StoreConnection {
                            message: error.to_string(),
                        })?;
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
        self.put_object_bytes(&sk, entry.encode()).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let sk = Self::s3_key(cname, key);

        match self
            .client()
            .head_object()
            .bucket(self.bucket_name())
            .key(&sk)
            .send()
            .await
        {
            Ok(_) => {}
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_not_found()) =>
            {
                return Ok(false);
            }
            Err(error) => {
                return Err(Error::StoreConnection {
                    message: error.to_string(),
                });
            }
        }

        self.client()
            .delete_object()
            .bucket(self.bucket_name())
            .key(&sk)
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: error.to_string(),
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
            self.put_object_bytes(&sk, entry.encode()).await?;
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
                            let entry = ManagedEntry::decode(bytes)?;
                            if entry.is_expired() {
                                self.client()
                                    .delete_object()
                                    .bucket(self.bucket_name())
                                    .key(&key)
                                    .send()
                                    .await
                                    .map_err(|error| Error::StoreConnection {
                                        message: error.to_string(),
                                    })?;
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
                        .map_err(|error| Error::StoreSetup {
                            message: error.to_string(),
                        })
                })
                .collect::<Result<_>>()?;
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .build()
                .map_err(|error| Error::StoreSetup {
                    message: error.to_string(),
                })?;
            let output = self
                .client()
                .delete_objects()
                .bucket(self.bucket_name())
                .delete(delete)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: error.to_string(),
                })?;
            if !output.errors().is_empty() {
                let details = output
                    .errors()
                    .iter()
                    .map(|error| {
                        format!(
                            "key={} code={} message={}",
                            error.key().unwrap_or("<unknown>"),
                            error.code().unwrap_or("<unknown>"),
                            error.message().unwrap_or("<unknown>")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::StoreConnection {
                    message: format!("S3 batch delete failed: {details}"),
                });
            }
        }
        Ok(true)
    }
}

#[async_trait]
impl AsyncDestroyStore for S3Store {
    async fn destroy(&self) -> Result<bool> {
        loop {
            let res = self
                .client()
                .list_objects_v2()
                .bucket(self.bucket_name())
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: error.to_string(),
                })?;
            let keys: Vec<String> = res
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|object| object.key)
                .collect();
            if keys.is_empty() {
                break;
            }

            let objects: Vec<ObjectIdentifier> = keys
                .iter()
                .map(|key| {
                    ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .map_err(|error| Error::StoreSetup {
                            message: error.to_string(),
                        })
                })
                .collect::<Result<_>>()?;
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .build()
                .map_err(|error| Error::StoreSetup {
                    message: error.to_string(),
                })?;
            let output = self
                .client()
                .delete_objects()
                .bucket(self.bucket_name())
                .delete(delete)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: error.to_string(),
                })?;
            if !output.errors().is_empty() {
                let details = output
                    .errors()
                    .iter()
                    .map(|error| {
                        format!(
                            "key={} code={} message={}",
                            error.key().unwrap_or("<unknown>"),
                            error.code().unwrap_or("<unknown>"),
                            error.message().unwrap_or("<unknown>")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::StoreConnection {
                    message: format!("S3 batch delete failed: {details}"),
                });
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{Credentials, Region};

    #[tokio::test]
    #[ignore = "requires an S3-compatible service configured by OPENKEYV_S3_ENDPOINT"]
    async fn s3_uses_binary_entries_and_strict_delete_semantics() {
        let endpoint = std::env::var("OPENKEYV_S3_ENDPOINT").unwrap();
        let bucket_name = format!(
            "openkeyv-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_millis()
        );
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "openkeyv",
                "openkeyv",
                None,
                None,
                "openkeyv-tests",
            ))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);
        let store = S3Store::from_client(client.clone(), &bucket_name);
        store.ensure_bucket().await.unwrap();

        store
            .put("binary", Value::utf8("value"), None, Some(30.0))
            .await
            .unwrap();
        let raw = client
            .get_object()
            .bucket(&bucket_name)
            .key(S3Store::s3_key(store.collection_name(None), "binary"))
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert!(raw.starts_with(b"OKVE1"));
        assert_eq!(
            store.get("binary", None).await.unwrap(),
            Some(Value::utf8("value"))
        );
        assert!(store.delete("binary", None).await.unwrap());
        assert!(!store.delete("binary", None).await.unwrap());

        client
            .put_object()
            .bucket(&bucket_name)
            .key(S3Store::s3_key(store.collection_name(None), "old-json"))
            .body(Bytes::from_static(br#"{"value":null}"#).into())
            .send()
            .await
            .unwrap();
        let error = store.get("old-json", None).await.unwrap_err();
        assert!(error.to_string().contains("invalid OpenKeyV entry magic"));

        store.destroy().await.unwrap();
    }
}
