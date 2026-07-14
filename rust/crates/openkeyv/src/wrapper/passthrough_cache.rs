use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;
use std::collections::HashSet;

/// A two-tier cache wrapper.
///
/// Reads from the cache store first, then reads the primary store on a cache miss
/// and populates the cache with the result.
pub struct PassthroughCacheWrapper<C: AsyncKeyValue, P: AsyncKeyValue> {
    cache: C,
    primary: P,
    cache_ttl: Option<f64>,
}

impl<C: AsyncKeyValue, P: AsyncKeyValue> PassthroughCacheWrapper<C, P> {
    pub fn new(cache: C, primary: P) -> Self {
        Self {
            cache,
            primary,
            cache_ttl: None,
        }
    }

    pub fn with_cache_ttl(cache: C, primary: P, cache_ttl: f64) -> Self {
        Self {
            cache,
            primary,
            cache_ttl: Some(cache_ttl),
        }
    }
}

#[async_trait]
impl<C: AsyncKeyValue, P: AsyncKeyValue> AsyncKeyValue for PassthroughCacheWrapper<C, P> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        match self.cache.get(key, collection).await? {
            Some(value) => Ok(Some(value)),
            None => {
                let value = self.primary.get(key, collection).await?;
                if let Some(ref v) = value {
                    self.cache
                        .put(key, v.clone(), collection, self.cache_ttl)
                        .await?;
                }
                Ok(value)
            }
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        match self.cache.ttl(key, collection).await? {
            Some((value, ttl)) => Ok(Some((value, ttl))),
            None => {
                let result = self.primary.ttl(key, collection).await?;
                if let Some((ref v, _)) = result {
                    self.cache
                        .put(key, v.clone(), collection, self.cache_ttl)
                        .await?;
                }
                Ok(result)
            }
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.cache.delete(key, collection).await?;
        self.primary.put(key, value, collection, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.cache.delete(key, collection).await?;
        self.primary.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        // Simple implementation: delegate to primary, could be optimized
        let results = self.primary.get_many(keys, collection).await?;
        for (key, result) in keys.iter().zip(results.iter()) {
            if let Some(v) = result {
                self.cache
                    .put(key, v.clone(), collection, self.cache_ttl)
                    .await?;
            }
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        self.primary.ttl_many(keys, collection).await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        for key in keys {
            self.cache.delete(key, collection).await?;
        }
        self.primary.put_many(keys, values, collection, ttl).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        for key in keys {
            self.cache.delete(key, collection).await?;
        }
        self.primary.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<C, P> AsyncEnumerateKeys for PassthroughCacheWrapper<C, P>
where
    C: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
    P: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cache = self.cache.keys(collection, None).await?;
        let primary = self.primary.keys(collection, None).await?;
        let mut seen = HashSet::new();
        let mut merged = Vec::new();

        for value in cache.into_iter().chain(primary) {
            if seen.insert(value.clone()) {
                merged.push(value);
                if limit.is_some_and(|limit| merged.len() >= limit) {
                    break;
                }
            }
        }

        Ok(merged)
    }
}

#[async_trait]
impl<C, P> AsyncEnumerateCollections for PassthroughCacheWrapper<C, P>
where
    C: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
    P: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let cache = self.cache.collections(None).await?;
        let primary = self.primary.collections(None).await?;
        let mut seen = HashSet::new();
        let mut merged = Vec::new();

        for value in cache.into_iter().chain(primary) {
            if seen.insert(value.clone()) {
                merged.push(value);
                if limit.is_some_and(|limit| merged.len() >= limit) {
                    break;
                }
            }
        }

        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;
    use crate::wrapper::readonly::ReadOnlyWrapper;

    #[tokio::test]
    async fn test_passthrough_cache() {
        let cache = MemoryStore::new();
        let primary = MemoryStore::new();

        let value = Value::utf8("b");
        primary.put("k", value.clone(), None, None).await.unwrap();

        let wrapper = PassthroughCacheWrapper::new(cache, primary);

        // First read populates cache
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value.clone()));

        // Should still be available even if primary is gone
        // (in this test both are in memory, but verifies structure)
        let got2 = wrapper.get("k", None).await.unwrap();
        assert_eq!(got2, Some(value));
    }

    #[tokio::test]
    async fn test_passthrough_cache_propagates_cache_write_errors() {
        let cache = ReadOnlyWrapper::new(MemoryStore::new());
        let primary = MemoryStore::new();
        primary
            .put("k", Value::utf8("v"), None, None)
            .await
            .unwrap();
        let wrapper = PassthroughCacheWrapper::new(cache, primary);

        assert!(wrapper.get("k", None).await.is_err());
        assert!(
            wrapper
                .put("k", Value::utf8("next"), None, None)
                .await
                .is_err()
        );
    }
}
