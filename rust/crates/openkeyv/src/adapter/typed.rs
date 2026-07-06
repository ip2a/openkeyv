use crate::error::{Error, Result};
use crate::protocol::AsyncKeyValue;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::HashMap;

/// Adapter that provides type-safe access on top of an `AsyncKeyValue` store.
///
/// If `T` serializes to a JSON object (i.e. `HashMap<String, Value>`), it is stored directly.
/// Otherwise, it is wrapped in `{"_data": <serialized>}` to maintain the dict interface.
pub struct TypedAdapter<T: Serialize + DeserializeOwned> {
    inner: Box<dyn AsyncKeyValue>,
    needs_wrapping: bool,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> TypedAdapter<T> {
    pub fn new(inner: Box<dyn AsyncKeyValue>) -> Self {
        let needs_wrapping = !Self::serializes_to_object();
        Self {
            inner,
            needs_wrapping,
            _phantom: std::marker::PhantomData,
        }
    }

    fn serializes_to_object() -> bool {
        // A simple heuristic: attempt to serialize a default/empty value and inspect.
        // For real usage this could be improved, but works for typical structs/maps.
        // Instead, we check the type name against known container patterns at compile time
        // via std::any::type_name. This is heuristic but zero-cost.
        let name = std::any::type_name::<T>();
        name.contains("HashMap")
            || name.contains("BTreeMap")
            || name.contains("serde_json :: Value")
            || (!name.contains("String")
                && !name.contains("Vec")
                && !name.contains("i32")
                && !name.contains("i64")
                && !name.contains("f32")
                && !name.contains("f64")
                && !name.contains("bool"))
    }

    fn to_store_value(&self, value: &T) -> Result<HashMap<String, Value>> {
        if self.needs_wrapping {
            let wrapped = serde_json::json!({ "_data": value });
            let obj = wrapped
                .as_object()
                .ok_or_else(|| Error::Serialization("expected object".to_string()))?;
            Ok(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        } else {
            let val =
                serde_json::to_value(value).map_err(|e| Error::Serialization(e.to_string()))?;
            let obj = val
                .as_object()
                .ok_or_else(|| Error::Serialization("expected object".to_string()))?;
            Ok(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_store_value(&self, map: HashMap<String, Value>) -> Result<T> {
        if self.needs_wrapping {
            let val = map
                .get("_data")
                .cloned()
                .unwrap_or_else(|| Value::Object(map.into_iter().collect()));
            serde_json::from_value(val).map_err(|e| Error::Deserialization(e.to_string()))
        } else {
            let val = Value::Object(map.into_iter().collect());
            serde_json::from_value(val).map_err(|e| Error::Deserialization(e.to_string()))
        }
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
        values: &[HashMap<String, Value>],
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
