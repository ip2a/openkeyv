use crate::error::Result;
use crate::protocol::{
    AsyncCompareAndSwap, AsyncCull, AsyncDestroyCollection, AsyncDestroyStore,
    AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue, CompareAndDeleteResult,
    CompareAndSwapResult, Revision, RevisionedValue,
};
use crate::value::Value;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Tracks operation counts for a wrapped store.
#[derive(Debug, Default)]
pub struct Statistics {
    pub gets: AtomicUsize,
    pub puts: AtomicUsize,
    pub deletes: AtomicUsize,
    pub get_many: AtomicUsize,
    pub put_many: AtomicUsize,
    pub delete_many: AtomicUsize,
    pub hits: AtomicUsize,
    pub misses: AtomicUsize,
}

impl Statistics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> StatisticsSnapshot {
        StatisticsSnapshot {
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            get_many: self.get_many.load(Ordering::Relaxed),
            put_many: self.put_many.load(Ordering::Relaxed),
            delete_many: self.delete_many.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatisticsSnapshot {
    pub gets: usize,
    pub puts: usize,
    pub deletes: usize,
    pub get_many: usize,
    pub put_many: usize,
    pub delete_many: usize,
    pub hits: usize,
    pub misses: usize,
}

/// A wrapper that tracks operation statistics.
pub struct StatisticsWrapper<T: AsyncKeyValue> {
    inner: T,
    stats: Statistics,
}

impl<T: AsyncKeyValue> StatisticsWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            stats: Statistics::new(),
        }
    }

    pub fn stats(&self) -> &Statistics {
        &self.stats
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: AsyncKeyValue> AsyncKeyValue for StatisticsWrapper<T> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get(key, collection).await?;
        if result.is_some() {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        self.inner.ttl(key, collection).await
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put(key, value, collection, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        self.stats.get_many.fetch_add(1, Ordering::Relaxed);
        let results = self.inner.get_many(keys, collection).await?;
        for r in &results {
            if r.is_some() {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
            } else {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        self.inner.ttl_many(keys, collection).await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.stats.put_many.fetch_add(1, Ordering::Relaxed);
        self.inner.put_many(keys, values, collection, ttl).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.stats.delete_many.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateKeys for StatisticsWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.keys(collection, limit).await
    }
}

#[async_trait]
impl<T> AsyncEnumerateCollections for StatisticsWrapper<T>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        self.inner.collections(limit).await
    }
}

#[async_trait]
impl<T> AsyncCompareAndSwap for StatisticsWrapper<T>
where
    T: AsyncKeyValue + AsyncCompareAndSwap + Send + Sync,
{
    async fn get_with_revision(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<RevisionedValue>> {
        self.inner.get_with_revision(key, collection).await
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
            .compare_and_swap(key, expected, value, collection, ttl)
            .await
    }

    async fn compare_and_delete(
        &self,
        key: &str,
        expected: &Revision,
        collection: Option<&str>,
    ) -> Result<CompareAndDeleteResult> {
        self.inner
            .compare_and_delete(key, expected, collection)
            .await
    }
}

#[async_trait]
impl<T> AsyncCull for StatisticsWrapper<T>
where
    T: AsyncKeyValue + AsyncCull + Send + Sync,
{
    async fn cull(&self) -> Result<()> {
        self.inner.cull().await
    }
}

#[async_trait]
impl<T> AsyncDestroyCollection for StatisticsWrapper<T>
where
    T: AsyncKeyValue + AsyncDestroyCollection + Send + Sync,
{
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        self.inner.destroy_collection(collection).await
    }
}

#[async_trait]
impl<T> AsyncDestroyStore for StatisticsWrapper<T>
where
    T: AsyncKeyValue + AsyncDestroyStore + Send + Sync,
{
    async fn destroy(&self) -> Result<bool> {
        self.inner.destroy().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    fn assert_capabilities<
        T: AsyncKeyValue
            + AsyncCompareAndSwap
            + AsyncCull
            + AsyncDestroyCollection
            + AsyncDestroyStore,
    >() {
    }

    #[test]
    fn transparent_wrapper_preserves_store_capabilities() {
        assert_capabilities::<StatisticsWrapper<MemoryStore>>();
    }

    #[tokio::test]
    async fn test_statistics() {
        let mem = MemoryStore::new();
        let wrapper = StatisticsWrapper::new(mem);

        let value = Value::utf8("b");

        wrapper.put("k", value.clone(), None, None).await.unwrap();
        let _ = wrapper.get("k", None).await.unwrap();
        let _ = wrapper.get("missing", None).await.unwrap();
        wrapper.delete("k", None).await.unwrap();

        let snap = wrapper.stats().snapshot();
        assert_eq!(snap.puts, 1);
        assert_eq!(snap.gets, 2);
        assert_eq!(snap.deletes, 1);
        assert_eq!(snap.hits, 1);
        assert_eq!(snap.misses, 1);
    }
}
