use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use async_trait::async_trait;
use base64::Engine;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Write};

const COMPRESSED_DATA_KEY: &str = "__compressed_data__";
const COMPRESSION_VERSION_KEY: &str = "__compression_version__";
const COMPRESSION_ALGORITHM_KEY: &str = "__compression_algorithm__";
const COMPRESSION_VERSION: i32 = 1;

/// A wrapper that compresses values with gzip before storing.
pub struct CompressionWrapper<T: AsyncKeyValue> {
    inner: T,
    min_size_to_compress: usize,
}

impl<T: AsyncKeyValue> CompressionWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            min_size_to_compress: 1024,
        }
    }

    pub fn with_min_size(inner: T, min_size: usize) -> Self {
        Self {
            inner,
            min_size_to_compress: min_size,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn should_compress(&self, value: &HashMap<String, Value>) -> bool {
        if value.contains_key(COMPRESSED_DATA_KEY) {
            return false;
        }
        let json = serde_json::to_string(value).unwrap_or_default();
        json.len() >= self.min_size_to_compress
    }

    fn compress(&self, value: HashMap<String, Value>) -> Result<HashMap<String, Value>> {
        if !self.should_compress(&value) {
            return Ok(value);
        }
        let json = serde_json::to_string(&value)
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder
            .write_all(json.as_bytes())
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;
        let compressed = encoder
            .finish()
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;
        let base64 = base64::engine::general_purpose::STANDARD.encode(compressed);
        let mut wrapped = HashMap::new();
        wrapped.insert(COMPRESSED_DATA_KEY.to_string(), Value::String(base64));
        wrapped.insert(
            COMPRESSION_VERSION_KEY.to_string(),
            Value::Number(COMPRESSION_VERSION.into()),
        );
        wrapped.insert(
            COMPRESSION_ALGORITHM_KEY.to_string(),
            Value::String("gzip".to_string()),
        );
        Ok(wrapped)
    }

    fn decompress(
        &self,
        value: Option<HashMap<String, Value>>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };
        if !value.contains_key(COMPRESSED_DATA_KEY) {
            return Ok(Some(value));
        }
        let base64_str = match value.get(COMPRESSED_DATA_KEY) {
            Some(Value::String(s)) => s,
            _ => return Ok(Some(value)),
        };
        let compressed = match base64::engine::general_purpose::STANDARD.decode(base64_str) {
            Ok(c) => c,
            Err(_) => return Ok(Some(value)),
        };
        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut buf = Vec::new();
        if decoder.read_to_end(&mut buf).is_err() {
            return Ok(Some(value));
        }
        let json_str = String::from_utf8_lossy(&buf);
        let decoded: HashMap<String, Value> = serde_json::from_str(&json_str)
            .map_err(|e| crate::error::Error::Deserialization(e.to_string()))?;
        Ok(Some(decoded))
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for CompressionWrapper<T> {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let value = self.inner.get(key, collection).await?;
        self.decompress(value)
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let result = self.inner.ttl(key, collection).await?;
        match result {
            Some((value, ttl)) => {
                let decompressed = self.decompress(Some(value))?;
                Ok(decompressed.map(|v| (v, ttl)))
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
        let compressed = self.compress(value)?;
        self.inner.put(key, compressed, collection, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let results = self.inner.get_many(keys, collection).await?;
        results.into_iter().map(|v| self.decompress(v)).collect()
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let results = self.inner.ttl_many(keys, collection).await?;
        results
            .into_iter()
            .map(|opt| match opt {
                Some((value, ttl)) => {
                    let decompressed = self.decompress(Some(value))?;
                    Ok(decompressed.map(|v| (v, ttl)))
                }
                None => Ok(None),
            })
            .collect()
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[HashMap<String, Value>],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let compressed: Result<Vec<_>> = values.iter().cloned().map(|v| self.compress(v)).collect();
        self.inner
            .put_many(keys, &compressed?, collection, ttl)
            .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.inner.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for CompressionWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for CompressionWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.collections(limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    #[tokio::test]
    async fn test_compression_small_value_skipped() {
        let mem = MemoryStore::new();
        let wrapper = CompressionWrapper::with_min_size(mem, 1024);

        let mut value = HashMap::new();
        value.insert("key".to_string(), Value::String("short".to_string()));

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_compression_large_value_roundtrip() {
        let mem = MemoryStore::new();
        let wrapper = CompressionWrapper::with_min_size(mem, 10);

        let mut value = HashMap::new();
        value.insert(
            "key".to_string(),
            Value::String(
                "this is a very long string that should definitely be compressed by the wrapper"
                    .to_string(),
            ),
        );

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }
}
