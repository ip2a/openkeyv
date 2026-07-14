use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::{Value, ValueKind};
use async_trait::async_trait;
use std::io::{Read, Write};

const COMPRESSION_MAGIC: &[u8] = b"OKVZ1";

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

    fn should_compress(&self, value: &Value) -> bool {
        if value.bytes().starts_with(COMPRESSION_MAGIC) {
            return false;
        }
        value.len() >= self.min_size_to_compress
    }

    fn compress(&self, value: Value) -> Result<Value> {
        if !self.should_compress(&value) {
            return Ok(value);
        }
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder
            .write_all(value.bytes())
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;
        let compressed = encoder
            .finish()
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;

        let mut bytes = Vec::with_capacity(COMPRESSION_MAGIC.len() + 1 + compressed.len());
        bytes.extend_from_slice(COMPRESSION_MAGIC);
        bytes.push(value.kind().tag());
        bytes.extend_from_slice(&compressed);
        Ok(Value::binary(bytes))
    }

    fn decompress(&self, value: Option<Value>) -> Result<Option<Value>> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };
        if !value.bytes().starts_with(COMPRESSION_MAGIC) {
            return Ok(Some(value));
        }
        let bytes = value.bytes();
        if bytes.len() <= COMPRESSION_MAGIC.len() {
            return Ok(Some(value));
        }
        let kind = match ValueKind::from_tag(bytes[COMPRESSION_MAGIC.len()]) {
            Some(kind) => kind,
            None => return Ok(Some(value)),
        };
        let mut decoder = flate2::read::GzDecoder::new(&bytes[COMPRESSION_MAGIC.len() + 1..]);
        let mut buf = Vec::new();
        if decoder.read_to_end(&mut buf).is_err() {
            return Ok(Some(value));
        }
        Ok(Some(Value::new(kind, buf)))
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for CompressionWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let value = self.inner.get(key, collection).await?;
        self.decompress(value)
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
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
        value: Value,
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
    ) -> Result<Vec<Option<Value>>> {
        let results = self.inner.get_many(keys, collection).await?;
        results.into_iter().map(|v| self.decompress(v)).collect()
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
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
        values: &[Value],
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

        let value = Value::utf8("short");

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_compression_large_value_roundtrip() {
        let mem = MemoryStore::new();
        let wrapper = CompressionWrapper::with_min_size(mem, 10);

        let value = Value::utf8(
            "this is a very long string that should definitely be compressed by the wrapper",
        );

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }
}
