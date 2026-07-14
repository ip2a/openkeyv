use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;
use std::collections::HashSet;

/// A wrapper that stores all collections within a single backing collection
/// by prefixing keys with the collection name.
pub struct SingleCollectionWrapper<T: AsyncKeyValue> {
    inner: T,
    backing_collection: String,
    separator: String,
}

impl<T: AsyncKeyValue> SingleCollectionWrapper<T> {
    pub fn new(inner: T, backing_collection: impl Into<String>) -> Self {
        Self {
            inner,
            backing_collection: backing_collection.into(),
            separator: "::".to_string(),
        }
    }

    pub fn with_separator(
        inner: T,
        backing_collection: impl Into<String>,
        separator: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            backing_collection: backing_collection.into(),
            separator: separator.into(),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn compound_key(&self, collection: Option<&str>, key: &str) -> String {
        let collection = collection.unwrap_or("default_collection");
        format!("{}{}{}", collection, self.separator, key)
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for SingleCollectionWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let compound = self.compound_key(collection, key);
        self.inner
            .get(&compound, Some(&self.backing_collection))
            .await
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let compound = self.compound_key(collection, key);
        self.inner
            .ttl(&compound, Some(&self.backing_collection))
            .await
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let compound = self.compound_key(collection, key);
        self.inner
            .put(&compound, value, Some(&self.backing_collection), ttl)
            .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let compound = self.compound_key(collection, key);
        self.inner
            .delete(&compound, Some(&self.backing_collection))
            .await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let compounds: Vec<String> = keys
            .iter()
            .map(|k| self.compound_key(collection, k))
            .collect();
        self.inner
            .get_many(&compounds, Some(&self.backing_collection))
            .await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let compounds: Vec<String> = keys
            .iter()
            .map(|k| self.compound_key(collection, k))
            .collect();
        self.inner
            .ttl_many(&compounds, Some(&self.backing_collection))
            .await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let compounds: Vec<String> = keys
            .iter()
            .map(|k| self.compound_key(collection, k))
            .collect();
        self.inner
            .put_many(&compounds, values, Some(&self.backing_collection), ttl)
            .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let compounds: Vec<String> = keys
            .iter()
            .map(|k| self.compound_key(collection, k))
            .collect();
        self.inner
            .delete_many(&compounds, Some(&self.backing_collection))
            .await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for SingleCollectionWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let collection = collection.unwrap_or("default_collection");
        let prefix = format!("{}{}", collection, self.separator);
        let keys = self
            .inner
            .keys(Some(&self.backing_collection), None)
            .await?;
        let mut result: Vec<String> = keys
            .into_iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(str::to_string))
            .collect();
        if let Some(limit) = limit {
            result.truncate(limit);
        }
        Ok(result)
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for SingleCollectionWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let keys = self
            .inner
            .keys(Some(&self.backing_collection), None)
            .await?;
        let mut seen = HashSet::new();
        let mut collections = Vec::new();
        for key in keys {
            if let Some((collection, _)) = key.split_once(&self.separator) {
                if seen.insert(collection.to_string()) {
                    collections.push(collection.to_string());
                }
            }
            if let Some(limit) = limit {
                if collections.len() >= limit {
                    break;
                }
            }
        }
        Ok(collections)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    #[tokio::test]
    async fn test_single_collection() {
        let mem = MemoryStore::new();
        let wrapper = SingleCollectionWrapper::new(mem, "all_data");

        let value = Value::utf8("b");
        wrapper
            .put("k", value.clone(), Some("users"), None)
            .await
            .unwrap();

        let got = wrapper.get("k", Some("users")).await.unwrap();
        assert_eq!(got, Some(value));

        let raw = wrapper.into_inner();
        assert!(
            raw.get("users::k", Some("all_data"))
                .await
                .unwrap()
                .is_some()
        );
    }
}
