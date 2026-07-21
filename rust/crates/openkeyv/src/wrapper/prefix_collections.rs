use crate::error::Result;
use crate::protocol::{
    AsyncCompareAndSwap, AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue,
    CompareAndDeleteResult, CompareAndSwapResult, Revision, RevisionedValue,
};
use crate::value::Value;
use async_trait::async_trait;

/// A wrapper that prefixes all collection names before delegating to the underlying store.
pub struct PrefixCollectionsWrapper<T: AsyncKeyValue> {
    inner: T,
    prefix: String,
}

impl<T: AsyncKeyValue> PrefixCollectionsWrapper<T> {
    pub fn new(inner: T, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn prefixed_collection(&self, collection: Option<&str>) -> String {
        match collection {
            Some(c) => format!("{}{}", self.prefix, c),
            None => format!("{}default_collection", self.prefix),
        }
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for PrefixCollectionsWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        self.inner
            .get(key, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        self.inner
            .ttl(key, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.inner
            .put(key, value, Some(&self.prefixed_collection(collection)), ttl)
            .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.inner
            .delete(key, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        self.inner
            .get_many(keys, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        self.inner
            .ttl_many(keys, Some(&self.prefixed_collection(collection)))
            .await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.inner
            .put_many(
                keys,
                values,
                Some(&self.prefixed_collection(collection)),
                ttl,
            )
            .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.inner
            .delete_many(keys, Some(&self.prefixed_collection(collection)))
            .await
    }
}

#[async_trait]
impl<T> AsyncCompareAndSwap for PrefixCollectionsWrapper<T>
where
    T: AsyncKeyValue + AsyncCompareAndSwap + Send + Sync,
{
    async fn get_with_revision(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<RevisionedValue>> {
        self.inner
            .get_with_revision(key, Some(&self.prefixed_collection(collection)))
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
                key,
                expected,
                value,
                Some(&self.prefixed_collection(collection)),
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
            .compare_and_delete(key, expected, Some(&self.prefixed_collection(collection)))
            .await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for PrefixCollectionsWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner
            .keys(Some(&self.prefixed_collection(collection)), limit)
            .await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for PrefixCollectionsWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let collections = self.inner.collections(None).await?;
        Ok(collections
            .into_iter()
            .filter_map(|collection| collection.strip_prefix(&self.prefix).map(str::to_string))
            .take(limit.unwrap_or(usize::MAX))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    fn assert_capabilities<T: AsyncKeyValue + AsyncCompareAndSwap>() {}

    #[test]
    fn wrapper_preserves_compare_and_swap_capability() {
        assert_capabilities::<PrefixCollectionsWrapper<MemoryStore>>();
    }

    #[tokio::test]
    async fn test_prefix_collections() {
        let mem = MemoryStore::new();
        let wrapper = PrefixCollectionsWrapper::new(mem, "tenant_");

        let value = Value::utf8("b");
        wrapper
            .put("k", value.clone(), Some("users"), None)
            .await
            .unwrap();

        let got = wrapper.get("k", Some("users")).await.unwrap();
        assert_eq!(got, Some(value));

        let raw = wrapper.into_inner();
        assert!(raw.get("k", Some("tenant_users")).await.unwrap().is_some());
        assert_eq!(raw.collections(None).await.unwrap().len(), 1);
    }
}
