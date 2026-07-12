use crate::error::{Error, Result};
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::{Value, ValueKind};
use async_trait::async_trait;

const ENCRYPTION_MAGIC: &[u8] = b"OKVE1";

/// Wrapper that encrypts values before storing and decrypts on retrieval.
///
/// Value bytes are encrypted via the provided closure and stored with a small
/// binary envelope that preserves the original value kind.
pub struct EncryptionWrapper<T, E, D> {
    inner: T,
    encrypt_fn: E,
    decrypt_fn: D,
    version: u32,
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
        }
    }

    fn encrypt_value(&self, value: &Value) -> Result<Value> {
        let encrypted = (self.encrypt_fn)(value.bytes())?;
        let mut bytes = Vec::with_capacity(ENCRYPTION_MAGIC.len() + 5 + encrypted.len());
        bytes.extend_from_slice(ENCRYPTION_MAGIC);
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.push(value.kind().tag());
        bytes.extend_from_slice(&encrypted);
        Ok(Value::binary(bytes))
    }

    fn decrypt_value(&self, value: Option<Value>) -> Result<Option<Value>> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };
        if !value.bytes().starts_with(ENCRYPTION_MAGIC) {
            return Err(Error::CorruptedData);
        }

        let bytes = value.bytes();
        let header_len = ENCRYPTION_MAGIC.len() + 5;
        if bytes.len() < header_len {
            return Err(Error::CorruptedData);
        }
        let mut version = [0_u8; 4];
        version.copy_from_slice(&bytes[ENCRYPTION_MAGIC.len()..ENCRYPTION_MAGIC.len() + 4]);
        let version = u32::from_le_bytes(version);
        let kind =
            ValueKind::from_tag(bytes[ENCRYPTION_MAGIC.len() + 4]).ok_or(Error::CorruptedData)?;
        let decrypted = (self.decrypt_fn)(&bytes[header_len..], version)?;
        Ok(Some(Value::new(kind, decrypted)))
    }
}

#[async_trait]
impl<T, E, D> AsyncKeyValue for EncryptionWrapper<T, E, D>
where
    T: AsyncKeyValue,
    E: Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync,
    D: Fn(&[u8], u32) -> Result<Vec<u8>> + Send + Sync,
{
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let value = self.inner.get(key, collection).await?;
        self.decrypt_value(value)
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        match self.inner.ttl(key, collection).await? {
            Some((value, ttl)) => Ok(self.decrypt_value(Some(value))?.map(|v| (v, ttl))),
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
    ) -> Result<Vec<Option<Value>>> {
        let values = self.inner.get_many(keys, collection).await?;
        values.into_iter().map(|v| self.decrypt_value(v)).collect()
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
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

    fn fail_decrypt(_data: &[u8], _version: u32) -> Result<Vec<u8>> {
        Err(Error::Decryption("failed".to_owned()))
    }

    #[tokio::test]
    async fn test_encryption_wrapper_roundtrip() {
        let inner = MemoryStore::new();
        let wrapper = EncryptionWrapper::new(inner, noop_encrypt, noop_decrypt, 1);
        let value = Value::utf8("Alice");

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_encryption_wrapper_rejects_unencrypted_value() {
        let inner = MemoryStore::new();
        let wrapper = EncryptionWrapper::new(inner, noop_encrypt, noop_decrypt, 1);

        wrapper
            .inner
            .put("k", Value::integer(42), None, None)
            .await
            .unwrap();

        assert_eq!(
            wrapper.get("k", None).await.unwrap_err(),
            Error::CorruptedData
        );
    }

    #[tokio::test]
    async fn test_encryption_wrapper_propagates_decrypt_error() {
        let inner = MemoryStore::new();
        let wrapper = EncryptionWrapper::new(inner, noop_encrypt, fail_decrypt, 1);

        wrapper
            .put("k", Value::utf8("secret"), None, None)
            .await
            .unwrap();

        assert_eq!(
            wrapper.get("k", None).await.unwrap_err(),
            Error::Decryption("failed".to_owned())
        );
    }

    #[tokio::test]
    async fn test_encryption_wrapper_preserves_missing_value() {
        let inner = MemoryStore::new();
        let wrapper = EncryptionWrapper::new(inner, noop_encrypt, noop_decrypt, 1);

        assert_eq!(wrapper.get("missing", None).await.unwrap(), None);
    }
}
