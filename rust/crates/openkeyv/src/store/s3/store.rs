use super::client::S3Client;
use super::config::S3Config;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::utils::compound::{collection_prefix, decompound_key};
use crate::value::Value;
use async_trait::async_trait;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;

const S3_OBJECT_KEY_MAX_BYTES: usize = 1_024;
// `okv1` uses fixed-size chunks because some S3 implementations reject very long path segments.
const S3_OBJECT_KEY_SEGMENT_BYTES: usize = 200;

fn s3_collection_prefix(collection: &str) -> Result<String> {
    let encoded_collection = URL_SAFE_NO_PAD.encode(collection_prefix(collection).as_bytes());
    let chunked_collection = encoded_collection
        .as_bytes()
        .chunks(S3_OBJECT_KEY_SEGMENT_BYTES)
        .map(|chunk| std::str::from_utf8(chunk).expect("Base64URL is valid UTF-8"))
        .collect::<Vec<_>>()
        .join("/");
    let prefix = format!(
        "okv1/{}/{chunked_collection}/key/",
        encoded_collection.len()
    );

    if prefix.len() > S3_OBJECT_KEY_MAX_BYTES {
        return Err(Error::InvalidKey(format!(
            "encoded S3 collection prefix is {} bytes (max {S3_OBJECT_KEY_MAX_BYTES})",
            prefix.len()
        )));
    }

    Ok(prefix)
}

fn s3_key(collection: &str, key: &str) -> Result<String> {
    let encoded_key = URL_SAFE_NO_PAD.encode(key.as_bytes());
    let chunked_key = encoded_key
        .as_bytes()
        .chunks(S3_OBJECT_KEY_SEGMENT_BYTES)
        .map(|chunk| std::str::from_utf8(chunk).expect("Base64URL is valid UTF-8"))
        .collect::<Vec<_>>()
        .join("/");
    let mut object_key = s3_collection_prefix(collection)?;
    object_key.push_str(&chunked_key);

    if object_key.len() > S3_OBJECT_KEY_MAX_BYTES {
        return Err(Error::InvalidKey(format!(
            "encoded S3 object key is {} bytes (max {S3_OBJECT_KEY_MAX_BYTES})",
            object_key.len()
        )));
    }

    Ok(object_key)
}

fn decompose_s3_key(object_key: &str) -> Result<(String, String)> {
    if object_key.len() > S3_OBJECT_KEY_MAX_BYTES {
        return Err(Error::InvalidKey(format!(
            "encoded S3 object key is {} bytes (max {S3_OBJECT_KEY_MAX_BYTES})",
            object_key.len()
        )));
    }

    let mut segments = object_key.split('/');
    if segments.next() != Some("okv1") {
        return Err(Error::InvalidKey(
            "S3 object key has an invalid identity version".to_string(),
        ));
    }

    let collection_len = segments.next().ok_or_else(|| {
        Error::InvalidKey("S3 object key is missing its collection length".to_string())
    })?;
    if collection_len.is_empty()
        || !collection_len.bytes().all(|byte| byte.is_ascii_digit())
        || (collection_len.len() > 1 && collection_len.starts_with('0'))
    {
        return Err(Error::InvalidKey(
            "S3 object key has an invalid collection length".to_string(),
        ));
    }
    let collection_len = collection_len.parse::<usize>().map_err(|_| {
        Error::InvalidKey("S3 object key collection length is too large".to_string())
    })?;
    if collection_len == 0 {
        return Err(Error::InvalidKey(
            "S3 object key has an empty collection frame".to_string(),
        ));
    }
    if collection_len > object_key.len() {
        return Err(Error::InvalidKey(
            "S3 object key has a truncated collection encoding".to_string(),
        ));
    }

    let collection_chunk_count = collection_len.div_ceil(S3_OBJECT_KEY_SEGMENT_BYTES);
    let mut encoded_collection = String::with_capacity(collection_len);
    for index in 0..collection_chunk_count {
        let chunk = segments.next().ok_or_else(|| {
            Error::InvalidKey("S3 object key has a truncated collection encoding".to_string())
        })?;
        let expected_len = if index + 1 == collection_chunk_count {
            collection_len - (index * S3_OBJECT_KEY_SEGMENT_BYTES)
        } else {
            S3_OBJECT_KEY_SEGMENT_BYTES
        };
        if chunk.len() != expected_len {
            return Err(Error::InvalidKey(
                "S3 object key has non-canonical collection chunks".to_string(),
            ));
        }
        encoded_collection.push_str(chunk);
    }

    if segments.next() != Some("key") {
        return Err(Error::InvalidKey(
            "S3 object key is missing its key marker".to_string(),
        ));
    }
    let key_chunks = segments.collect::<Vec<_>>();
    if key_chunks.is_empty() {
        return Err(Error::InvalidKey(
            "S3 object key is missing its key encoding".to_string(),
        ));
    }
    let encoded_key = if key_chunks == [""] {
        String::new()
    } else {
        if key_chunks.iter().any(|chunk| chunk.is_empty())
            || key_chunks
                .iter()
                .take(key_chunks.len() - 1)
                .any(|chunk| chunk.len() != S3_OBJECT_KEY_SEGMENT_BYTES)
            || key_chunks
                .last()
                .is_none_or(|chunk| chunk.len() > S3_OBJECT_KEY_SEGMENT_BYTES)
        {
            return Err(Error::InvalidKey(
                "S3 object key has non-canonical key chunks".to_string(),
            ));
        }
        key_chunks.concat()
    };

    let collection_frame = URL_SAFE_NO_PAD.decode(&encoded_collection).map_err(|_| {
        Error::InvalidKey("S3 object key has an invalid collection encoding".to_string())
    })?;
    if URL_SAFE_NO_PAD.encode(&collection_frame) != encoded_collection {
        return Err(Error::InvalidKey(
            "S3 object key has a non-canonical collection encoding".to_string(),
        ));
    }
    let collection_frame = String::from_utf8(collection_frame).map_err(|_| {
        Error::InvalidKey("S3 object key collection is not valid UTF-8".to_string())
    })?;
    let (collection, embedded_key) = decompound_key(&collection_frame)?;
    if !embedded_key.is_empty() {
        return Err(Error::InvalidKey(
            "S3 object key collection frame contains key data".to_string(),
        ));
    }

    let key = URL_SAFE_NO_PAD
        .decode(&encoded_key)
        .map_err(|_| Error::InvalidKey("S3 object key has an invalid key encoding".to_string()))?;
    if URL_SAFE_NO_PAD.encode(&key) != encoded_key {
        return Err(Error::InvalidKey(
            "S3 object key has a non-canonical key encoding".to_string(),
        ));
    }
    let key = String::from_utf8(key)
        .map_err(|_| Error::InvalidKey("S3 object key is not valid UTF-8".to_string()))?;

    Ok((collection.to_string(), key))
}

/// AWS S3-backed key-value store.
///
/// Each entry is stored under a transported canonical collection/key identity.
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
        let sk = s3_key(cname, key)?;
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

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let cname = self.collection_name(collection);
        let sk = s3_key(cname, key)?;
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
                    let ttl = entry.ttl();
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
        let sk = s3_key(cname, key)?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        self.put_object_bytes(&sk, entry.encode()).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let sk = s3_key(cname, key)?;

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
        for key in keys {
            s3_key(cname, key)?;
        }
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
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let cname = self.collection_name(collection);
        for key in keys {
            s3_key(cname, key)?;
        }
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
        if let Some(seconds) = ttl {
            ManagedEntry::validate_ttl(seconds)?;
        }
        let cname = self.collection_name(collection);
        let mut entries = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds)?,
                None => ManagedEntry::new(value.clone()),
            };
            let sk = s3_key(cname, key)?;
            entries.push((sk, entry.encode()));
        }
        for (sk, entry) in entries {
            self.put_object_bytes(&sk, entry).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        for key in keys {
            s3_key(cname, key)?;
        }
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
                        decompose_s3_key(&key)?;
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
        let prefix = s3_collection_prefix(cname)?;
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
                    if let Some(object_key) = obj.key {
                        let (collection, key) = decompose_s3_key(&object_key)?;
                        if collection != cname {
                            return Err(Error::InvalidKey(format!(
                                "S3 object key belongs to collection {collection:?}, expected {cname:?}"
                            )));
                        }
                        keys.push(key);
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
                    if let Some(object_key) = obj.key {
                        let (collection, _) = decompose_s3_key(&object_key)?;
                        collections.insert(collection);
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
        let prefix = s3_collection_prefix(collection)?;
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
                    if let Some(object_key) = obj.key {
                        let (decoded_collection, _) = decompose_s3_key(&object_key)?;
                        if decoded_collection != collection {
                            return Err(Error::InvalidKey(format!(
                                "S3 object key belongs to collection {decoded_collection:?}, expected {collection:?}"
                            )));
                        }
                        keys_to_delete.push(object_key);
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

    #[test]
    fn s3_transport_roundtrips_exact_identities() {
        let cases = [
            ("a/b", "c"),
            ("a", "b/c"),
            ("a:b", "c"),
            ("a", "b:c"),
            ("Users", "Key"),
            ("users", "Key"),
            ("é", "e\u{301}"),
            ("集合", "键🔑"),
            ("", ""),
            ("space collection", "line\nnull\0/:*?[]\\"),
        ];

        for (collection, key) in cases {
            let object_key = s3_key(collection, key).unwrap();
            assert!(object_key.starts_with("okv1/"));
            assert!(object_key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'/')
            }));
            assert!(
                object_key
                    .split('/')
                    .all(|segment| segment.len() <= S3_OBJECT_KEY_SEGMENT_BYTES)
            );
            assert_eq!(
                decompose_s3_key(&object_key).unwrap(),
                (collection.to_string(), key.to_string())
            );
        }

        assert_ne!(s3_key("a/b", "c").unwrap(), s3_key("a", "b/c").unwrap());
        assert_ne!(s3_key("a:b", "c").unwrap(), s3_key("a", "b:c").unwrap());
    }

    #[test]
    fn s3_transport_enforces_exact_object_key_limit() {
        let accepted = s3_key("a", &"k".repeat(752)).unwrap();
        assert_eq!(accepted.len(), S3_OBJECT_KEY_MAX_BYTES);

        assert!(matches!(
            s3_key("a", &"k".repeat(753)),
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            s3_collection_prefix(&"c".repeat(1_024)),
            Err(Error::InvalidKey(_))
        ));
    }

    #[test]
    fn s3_transport_rejects_malformed_physical_identities() {
        let collection_frame = URL_SAFE_NO_PAD.encode(collection_prefix("collection").as_bytes());
        let key = URL_SAFE_NO_PAD.encode("key");
        let valid = s3_key("collection", "key").unwrap();
        let collection_len = collection_frame.len();
        let invalid_utf8_collection = URL_SAFE_NO_PAD.encode([0xff]);
        let invalid_utf8_key = URL_SAFE_NO_PAD.encode([0xff]);
        let frame_with_key = URL_SAFE_NO_PAD.encode(b"10:collectionkey");
        let long_collection_frame =
            URL_SAFE_NO_PAD.encode(collection_prefix(&"c".repeat(200)).as_bytes());
        let split_at = S3_OBJECT_KEY_SEGMENT_BYTES;
        let noncanonical_collection_chunks = format!(
            "okv1/{}/{}/{}/key/{key}",
            long_collection_frame.len(),
            &long_collection_frame[..split_at - 1],
            &long_collection_frame[split_at - 1..]
        );
        let long_key = URL_SAFE_NO_PAD.encode("k".repeat(200));
        let noncanonical_key_chunks = format!(
            "okv1/{collection_len}/{collection_frame}/key/{}//{}",
            &long_key[..S3_OBJECT_KEY_SEGMENT_BYTES],
            &long_key[S3_OBJECT_KEY_SEGMENT_BYTES..]
        );

        let malformed = [
            String::new(),
            "collection/key".to_string(),
            valid.replacen("okv1", "okv2", 1),
            format!("okv1//{collection_frame}/key/{key}"),
            format!("okv1/0{collection_len}/{collection_frame}/key/{key}"),
            format!("okv1/{}/{collection_frame}/key/{key}", collection_len + 1),
            format!("okv1/{}/{collection_frame}/key/{key}", collection_len - 1),
            format!("okv1/999999999/{collection_frame}/key/{key}"),
            noncanonical_collection_chunks,
            format!("okv1/{collection_len}/{collection_frame}/value/{key}"),
            format!("okv1/{collection_len}/{collection_frame}/key"),
            noncanonical_key_chunks,
            format!(
                "okv1/{collection_len}/{collection_frame}/key/{}",
                "a".repeat(201)
            ),
            format!("{valid}/"),
            format!(
                "okv1/{collection_len}/{}/key/{key}",
                "*".repeat(collection_len)
            ),
            format!("okv1/2/{invalid_utf8_collection}/key/{key}"),
            format!("okv1/{}/{frame_with_key}/key/{key}", frame_with_key.len()),
            format!("okv1/{collection_len}/{collection_frame}/key/*"),
            format!("okv1/{collection_len}/{collection_frame}/key/{invalid_utf8_key}"),
            "a".repeat(S3_OBJECT_KEY_MAX_BYTES + 1),
        ];

        for object_key in malformed {
            assert!(matches!(
                decompose_s3_key(&object_key),
                Err(Error::InvalidKey(_))
            ));
        }
    }

    #[tokio::test]
    #[ignore = "requires an S3-compatible service configured by OPENKEYV_S3_ENDPOINT"]
    async fn s3_uses_canonical_identities_and_strict_entries() {
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
            .key(s3_key(store.collection_name(None), "binary").unwrap())
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

        let cases = [
            ("a/b", "c", Value::utf8("slash-left")),
            ("a", "b/c", Value::utf8("slash-right")),
            ("a:b", "c", Value::utf8("colon-left")),
            ("a", "b:c", Value::utf8("colon-right")),
            ("Users", "Key", Value::utf8("upper")),
            ("users", "Key", Value::utf8("lower")),
            ("é", "e\u{301}", Value::utf8("composed")),
            ("e\u{301}", "é", Value::utf8("decomposed")),
            ("", "", Value::utf8("empty")),
            ("*?[]\\", "line\nnull\0/:*?[]\\", Value::utf8("special")),
        ];
        for (collection, key, value) in &cases {
            store
                .put(key, value.clone(), Some(collection), None)
                .await
                .unwrap();
        }
        for (collection, key, value) in &cases {
            assert_eq!(
                store.get(key, Some(collection)).await.unwrap(),
                Some(value.clone())
            );
            assert!(
                store
                    .keys(Some(collection), None)
                    .await
                    .unwrap()
                    .contains(&key.to_string())
            );
        }
        let collections = store.collections(None).await.unwrap();
        for (collection, _, _) in &cases {
            assert!(collections.contains(&collection.to_string()));
        }

        assert!(store.destroy_collection("a/b").await.unwrap());
        assert_eq!(store.get("c", Some("a/b")).await.unwrap(), None);
        assert_eq!(
            store.get("b/c", Some("a")).await.unwrap(),
            Some(Value::utf8("slash-right"))
        );

        let batch_keys = vec!["one".to_string(), "two".to_string()];
        let batch_values = vec![Value::integer(1), Value::integer(2)];
        store
            .put_many(&batch_keys, &batch_values, Some("batch"), None)
            .await
            .unwrap();
        assert_eq!(
            store.get_many(&batch_keys, Some("batch")).await.unwrap(),
            vec![Some(batch_values[0].clone()), Some(batch_values[1].clone())]
        );
        assert_eq!(
            store.delete_many(&batch_keys, Some("batch")).await.unwrap(),
            2
        );

        let accepted_key = "k".repeat(752);
        store
            .put(&accepted_key, Value::utf8("boundary"), Some("a"), None)
            .await
            .unwrap();
        assert_eq!(
            store.get(&accepted_key, Some("a")).await.unwrap(),
            Some(Value::utf8("boundary"))
        );

        let oversized_key = "k".repeat(753);
        assert!(matches!(
            store
                .put(&oversized_key, Value::utf8("oversized"), Some("a"), None)
                .await,
            Err(Error::InvalidKey(_))
        ));
        let valid_batch_key = "valid-before-oversized".to_string();
        store.delete(&valid_batch_key, Some("a")).await.unwrap();
        assert!(matches!(
            store
                .put_many(
                    &[valid_batch_key.clone(), oversized_key],
                    &[Value::integer(1), Value::integer(2)],
                    Some("a"),
                    None,
                )
                .await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(store.get(&valid_batch_key, Some("a")).await.unwrap(), None);

        let malformed_prefix = s3_collection_prefix("malformed").unwrap();
        let malformed_key = format!("{malformed_prefix}*");
        client
            .put_object()
            .bucket(&bucket_name)
            .key(&malformed_key)
            .body(ManagedEntry::new(Value::utf8("malformed")).encode().into())
            .send()
            .await
            .unwrap();
        assert!(matches!(
            store.keys(Some("malformed"), None).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            store.destroy_collection("malformed").await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(store.cull().await, Err(Error::InvalidKey(_))));
        client
            .delete_object()
            .bucket(&bucket_name)
            .key(&malformed_key)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket_name)
            .key("old-collection/old-key")
            .body(ManagedEntry::new(Value::utf8("old-format")).encode().into())
            .send()
            .await
            .unwrap();
        assert_eq!(
            store.get("old-key", Some("old-collection")).await.unwrap(),
            None
        );
        assert!(matches!(
            store.collections(None).await,
            Err(Error::InvalidKey(_))
        ));

        client
            .put_object()
            .bucket(&bucket_name)
            .key(s3_key(store.collection_name(None), "old-json").unwrap())
            .body(Bytes::from_static(br#"{"value":null}"#).into())
            .send()
            .await
            .unwrap();
        let error = store.get("old-json", None).await.unwrap_err();
        assert!(error.to_string().contains("invalid OpenKeyV entry magic"));

        store.destroy().await.unwrap();
    }
}
