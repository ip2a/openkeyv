use crate::error::{Error, Result};
use crate::protocol::AsyncKeyValue;
use crate::value::Value;
use serde::{Serialize, de::DeserializeOwned};

/// Adapter that provides type-safe access on top of an `AsyncKeyValue` store.
///
/// Values are serialized into opaque structured bytes before entering the core
/// protocol. This adapter is transitional while the Python boundary gets its
/// OpenKeyV-owned structured encoding.
pub struct TypedAdapter<T: Serialize + DeserializeOwned> {
    inner: Box<dyn AsyncKeyValue>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> TypedAdapter<T> {
    pub fn new(inner: Box<dyn AsyncKeyValue>) -> Self {
        Self {
            inner,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_store_value(&self, value: &T) -> Result<Value> {
        serde_json::to_vec(value)
            .map(Value::structured)
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_store_value(&self, value: Value) -> Result<T> {
        serde_json::from_slice(value.bytes()).map_err(|e| Error::Deserialization(e.to_string()))
    }

    pub async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<T>> {
        match self.inner.get(key, collection).await? {
            Some(map) => Ok(Some(self.from_store_value(map)?)),
            None => Ok(None),
        }
    }

    pub async fn put(
        &self,
        key: &str,
        value: &T,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let store_value = self.to_store_value(value)?;
        self.inner.put(key, store_value, collection, ttl).await
    }

    pub async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner.delete(key, collection).await
    }

    pub async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<T>>> {
        let results = self.inner.get_many(keys, collection).await?;
        results
            .into_iter()
            .map(|opt| match opt {
                Some(map) => Ok(Some(self.from_store_value(map)?)),
                None => Ok(None),
            })
            .collect()
    }

    pub async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.inner.put_many(keys, values, collection, ttl).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    struct User {
        name: String,
        age: u32,
    }

    #[tokio::test]
    async fn test_typed_adapter_roundtrip() {
        let mem = MemoryStore::new();
        let adapter = TypedAdapter::<User>::new(Box::new(mem));

        let user = User {
            name: "Alice".to_string(),
            age: 30,
        };

        adapter.put("u1", &user, None, None).await.unwrap();
        let got = adapter.get("u1", None).await.unwrap();
        assert_eq!(got, Some(user));
    }

    #[tokio::test]
    async fn test_typed_adapter_missing() {
        let mem = MemoryStore::new();
        let adapter = TypedAdapter::<User>::new(Box::new(mem));
        let got = adapter.get("missing", None).await.unwrap();
        assert_eq!(got, None);
    }
}
