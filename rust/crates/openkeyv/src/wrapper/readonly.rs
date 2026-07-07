use crate::error::{Error, Result};
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;

/// A wrapper that makes an underlying store read-only.
///
/// All write operations (`put`, `put_many`, `delete`, `delete_many`) return
/// a `ReadOnly` error.
pub struct ReadOnlyWrapper<T: AsyncKeyValue> {
    inner: T,
}

impl<T: AsyncKeyValue> ReadOnlyWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for ReadOnlyWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        self.inner.get(key, collection).await
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        self.inner.ttl(key, collection).await
    }

    async fn put(
        &self,
        _key: &str,
        _value: Value,
        _collection: Option<&str>,
        _ttl: Option<f64>,
    ) -> Result<()> {
        Err(Error::ReadOnly)
    }

    async fn delete(&self, _key: &str, _collection: Option<&str>) -> Result<bool> {
        Err(Error::ReadOnly)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        self.inner.get_many(keys, collection).await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        self.inner.ttl_many(keys, collection).await
    }

    async fn put_many(
        &self,
        _keys: &[String],
        _values: &[Value],
        _collection: Option<&str>,
        _ttl: Option<f64>,
    ) -> Result<()> {
        Err(Error::ReadOnly)
    }

    async fn delete_many(&self, _keys: &[String], _collection: Option<&str>) -> Result<usize> {
        Err(Error::ReadOnly)
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for ReadOnlyWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for ReadOnlyWrapper<T>
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
    async fn test_readonly_get() {
        let mem = MemoryStore::new();
        let value = Value::utf8("b");
        mem.put("k", value.clone(), None, None).await.unwrap();

        let ro = ReadOnlyWrapper::new(mem);
        let got = ro.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_readonly_put_fails() {
        let mem = MemoryStore::new();
        let ro = ReadOnlyWrapper::new(mem);
        let value = Value::null();

        let err = ro.put("k", value, None, None).await.unwrap_err();
        assert_eq!(err, Error::ReadOnly);
    }

    #[tokio::test]
    async fn test_readonly_delete_fails() {
        let mem = MemoryStore::new();
        let ro = ReadOnlyWrapper::new(mem);

        let err = ro.delete("k", None).await.unwrap_err();
        assert_eq!(err, Error::ReadOnly);
    }
}
