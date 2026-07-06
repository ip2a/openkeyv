use crate::error::{Error, Result};
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use async_trait::async_trait;
use base64::Engine;
use serde_json::Value;
use std::collections::HashMap;

const ENCRYPTED_DATA_KEY: &str = "__encrypted_data__";
const ENCRYPTION_VERSION_KEY: &str = "__encryption_version__";

/// Wrapper that encrypts values before storing and decrypts on retrieval.
///
/// Values are JSON-serialized, encrypted via the provided closure, base64-encoded,
/// and stored as a special two-key dictionary. On retrieval the process is reversed.
///
/// Non-encrypted values are passed through transparently.
pub struct EncryptionWrapper<T, E, D> {
    inner: T,
    encrypt_fn: E,
    decrypt_fn: D,
    version: u32,
    raise_on_error: bool,
}

impl<T, E, D> EncryptionWrapper<T, E, D>
where
    E: Fn(&[u8]) -> Result<Vec<u8>>,
    D: Fn(&[u8], u32) -> Result<Vec<u8>>,
{
    pub fn new(inner: T, encrypt_fn: E, decrypt_fn: D, version: u32) -> Self {
        Self {
            inner,
            encrypt_fn,
            decrypt_fn,
            version,
            raise_on_error: true,
        }
    }

    pub fn with_options(
        inner: T,
        encrypt_fn: E,
        decrypt_fn: D,
        version: u32,
        raise_on_error: bool,
    ) -> Self {
        Self {
            inner,
            encrypt_fn,
            decrypt_fn,
            version,
            raise_on_error,
        }
    }

    fn encrypt_value(&self, value: &HashMap<String, Value>) -> Result<HashMap<String, Value>> {
        let json_bytes =
            serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        let encrypted = (self.encrypt_fn)(&json_bytes)?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&encrypted);
        let mut result = HashMap::new();
        result.insert(ENCRYPTED_DATA_KEY.to_string(), Value::String(b64));
        result.insert(
            ENCRYPTION_VERSION_KEY.to_string(),
            Value::Number(serde_json::Number::from(self.version)),
        );
        Ok(result)
    }

    fn decrypt_value(
        &self,
        value: Option<HashMap<String, Value>>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };
        if !value.contains_key(ENCRYPTED_DATA_KEY) {
            return Ok(Some(value));
        }

        let decrypt = || -> Result<HashMap<String, Value>> {
            let version = value
                .get(ENCRYPTION_VERSION_KEY)
                .and_then(|v| v.as_u64())
                .ok_or(Error::CorruptedData)? as u32;
            let data = value
                .get(ENCRYPTED_DATA_KEY)
                .and_then(|v| v.as_str())
                .ok_or(Error::CorruptedData)?;
            let encrypted = base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|e| Error::Decryption(e.to_string()))?;
            let json_bytes = (self.decrypt_fn)(&encrypted, version)?;
            let decrypted: HashMap<String, Value> = serde_json::from_slice(&json_bytes)
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            Ok(decrypted)
        };

        match decrypt() {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                if self.raise_on_error {
                    Err(e)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[async_trait]
impl<T, E, D> AsyncKeyValue for EncryptionWrapper<T, E, D>
where
    T: AsyncKeyValue,
    E: Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync,
    D: Fn(&[u8], u32) -> Result<Vec<u8>> + Send + Sync,
{
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let value = self.inner.get(key, collection).await?;
        self.decrypt_value(value)
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        match self.inner.ttl(key, collection).await? {
            Some((value, ttl)) => Ok(self.decrypt_value(Some(value))?.map(|v| (v, ttl))),
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
        let encrypted = self.encrypt_value(&value)?;
        self.inner.put(key, encrypted, collection, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let values = self.inner.get_many(keys, collection).await?;
        values.into_iter().map(|v| self.decrypt_value(v)).collect()
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
                Some((value, ttl)) => Ok(self.decrypt_value(Some(value))?.map(|v| (v, ttl))),
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
        if keys.len() != values.len() {
            return Err(Error::BatchSizeMismatch {
                keys: keys.len(),
                values: values.len(),
            });
        }
        let encrypted: Result<Vec<_>> = values.iter().map(|v| self.encrypt_value(v)).collect();
        self.inner
            .put_many(keys, &encrypted?, collection, ttl)
            .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.inner.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T, E, D> AsyncEnumerateKeys for EncryptionWrapper<T, E, D>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
    E: Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync,
    D: Fn(&[u8], u32) -> Result<Vec<u8>> + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T, E, D> AsyncEnumerateCollections for EncryptionWrapper<T, E, D>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
    E: Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync,
    D: Fn(&[u8], u32) -> Result<Vec<u8>> + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.collections(limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    fn noop_encrypt(data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn noop_decrypt(data: &[u8], _version: u32) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    #[tokio::test]
    async fn test_encryption_wrapper_roundtrip() {
        let inner = MemoryStore::new();
        let wrapper = EncryptionWrapper::new(inner, noop_encrypt, noop_decrypt, 1);
        let mut value = HashMap::new();
        value.insert("name".to_string(), Value::String("Alice".to_string()));

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_encryption_wrapper_decrypt_error() {
        let inner = MemoryStore::new();
        let wrapper = EncryptionWrapper::with_options(inner, noop_encrypt, noop_decrypt, 1, false);

        // Insert a non-encrypted value directly into inner store
        let mut plain = HashMap::new();
        plain.insert("x".to_string(), Value::Number(42.into()));
        wrapper
            .inner
            .put("k", plain.clone(), None, None)
            .await
            .unwrap();

        // Should pass through transparently
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(plain));
    }
}
