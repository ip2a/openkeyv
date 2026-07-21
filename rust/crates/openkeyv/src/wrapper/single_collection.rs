use crate::error::Result;
use crate::protocol::{
    AsyncCompareAndSwap, AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue,
    CompareAndDeleteResult, CompareAndSwapResult, Revision, RevisionedValue,
};
use crate::utils::compound::{compound_key, decompound_key};
use crate::value::Value;
use async_trait::async_trait;
use std::collections::HashSet;

/// A wrapper that stores all logical collections within one backing collection
/// using canonical compound identities.
pub struct SingleCollectionWrapper<T: AsyncKeyValue> {
    inner: T,
    backing_collection: String,
}

impl<T: AsyncKeyValue> SingleCollectionWrapper<T> {
    pub fn new(inner: T, backing_collection: impl Into<String>) -> Self {
        Self {
            inner,
            backing_collection: backing_collection.into(),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn compound_key(&self, collection: Option<&str>, key: &str) -> String {
        compound_key(collection.unwrap_or("default_collection"), key)
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
impl<T> AsyncCompareAndSwap for SingleCollectionWrapper<T>
where
    T: AsyncKeyValue + AsyncCompareAndSwap + Send + Sync,
{
    async fn get_with_revision(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<RevisionedValue>> {
        self.inner
            .get_with_revision(
                &self.compound_key(collection, key),
                Some(&self.backing_collection),
            )
            .await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&Revision>,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<CompareAndSwapResult> {
        self.inner
            .compare_and_swap(
                &self.compound_key(collection, key),
                expected,
                value,
                Some(&self.backing_collection),
                ttl,
            )
            .await
    }

    async fn compare_and_delete(
        &self,
        key: &str,
        expected: &Revision,
        collection: Option<&str>,
    ) -> Result<CompareAndDeleteResult> {
        self.inner
            .compare_and_delete(
                &self.compound_key(collection, key),
                expected,
                Some(&self.backing_collection),
            )
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
        let keys = self
            .inner
            .keys(Some(&self.backing_collection), None)
            .await?;
        let mut result = Vec::new();
        for identity in keys {
            let (key_collection, key) = decompound_key(&identity)?;
            if key_collection == collection {
                result.push(key.to_string());
            }
        }
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
        let limit = limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let keys = self
            .inner
            .keys(Some(&self.backing_collection), None)
            .await?;
        let mut seen = HashSet::new();
        let mut collections = Vec::new();
        for identity in keys {
            let (collection, _) = decompound_key(&identity)?;
            if seen.insert(collection.to_string()) {
                collections.push(collection.to_string());
            }
            if collections.len() == limit {
                break;
            }
        }
        Ok(collections)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;
    use crate::utils::compound::compound_key;

    fn assert_capabilities<T: AsyncKeyValue + AsyncCompareAndSwap>() {}

    #[test]
    fn wrapper_preserves_compare_and_swap_capability() {
        assert_capabilities::<SingleCollectionWrapper<MemoryStore>>();
    }

    #[tokio::test]
    async fn single_collection_uses_canonical_identity() {
        let mem = MemoryStore::new();
        let wrapper = SingleCollectionWrapper::new(mem, "all_data");
        let left = Value::utf8("left");
        let right = Value::utf8("right");

        wrapper
            .put("c", left.clone(), Some("a:b"), None)
            .await
            .unwrap();
        wrapper
            .put("b:c", right.clone(), Some("a"), None)
            .await
            .unwrap();

        assert_eq!(wrapper.get("c", Some("a:b")).await.unwrap(), Some(left));
        assert_eq!(wrapper.get("b:c", Some("a")).await.unwrap(), Some(right));
        assert_eq!(wrapper.keys(Some("a:b"), None).await.unwrap(), vec!["c"]);

        let mut collections = wrapper.collections(None).await.unwrap();
        collections.sort();
        assert_eq!(collections, vec!["a", "a:b"]);

        let raw = wrapper.into_inner();
        assert!(
            raw.get(&compound_key("a:b", "c"), Some("all_data"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn single_collection_rejects_malformed_physical_keys() {
        let mem = MemoryStore::new();
        mem.put("01:akey", Value::null(), Some("all_data"), None)
            .await
            .unwrap();
        let wrapper = SingleCollectionWrapper::new(mem, "all_data");

        assert!(wrapper.keys(Some("a"), None).await.is_err());
        assert!(wrapper.collections(None).await.is_err());
    }
}
