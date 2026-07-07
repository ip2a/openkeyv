use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;
use std::collections::HashSet;
/// A wrapper that falls back to a secondary store when the primary fails.
pub struct FallbackWrapper<T: AsyncKeyValue, F: AsyncKeyValue> {
    primary: T,
    fallback: F,
}

impl<T: AsyncKeyValue, F: AsyncKeyValue> FallbackWrapper<T, F> {
    pub fn new(primary: T, fallback: F) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl<T: AsyncKeyValue, F: AsyncKeyValue> AsyncKeyValue for FallbackWrapper<T, F> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        match self.primary.get(key, collection).await {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => self.fallback.get(key, collection).await,
            Err(_) => self.fallback.get(key, collection).await,
        }
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        match self.primary.ttl(key, collection).await {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => self.fallback.ttl(key, collection).await,
            Err(_) => self.fallback.ttl(key, collection).await,
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.primary.put(key, value, collection, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.primary.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        match self.primary.get_many(keys, collection).await {
            Ok(results) => Ok(results),
            Err(_) => self.fallback.get_many(keys, collection).await,
        }
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        match self.primary.ttl_many(keys, collection).await {
            Ok(results) => Ok(results),
            Err(_) => self.fallback.ttl_many(keys, collection).await,
        }
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.primary.put_many(keys, values, collection, ttl).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.primary.delete_many(keys, collection).await
    }
}

#[async_trait]
impl<T, F> AsyncEnumerateKeys for FallbackWrapper<T, F>
where
    T: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
    F: AsyncKeyValue + AsyncEnumerateKeys + Send + Sync,
{
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let primary = self.primary.keys(collection, None).await;
        let fallback = self.fallback.keys(collection, None).await;
        merge_string_results(primary, fallback, limit)
    }
}

#[async_trait]
impl<T, F> AsyncEnumerateCollections for FallbackWrapper<T, F>
where
    T: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
    F: AsyncKeyValue + AsyncEnumerateCollections + Send + Sync,
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let primary = self.primary.collections(None).await;
        let fallback = self.fallback.collections(None).await;
        merge_string_results(primary, fallback, limit)
    }
}

fn merge_string_results(
    primary: Result<Vec<String>>,
    fallback: Result<Vec<String>>,
    limit: Option<usize>,
) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut merged = Vec::new();

    for list in [primary, fallback] {
        match list {
            Ok(values) => {
                for value in values {
                    if seen.insert(value.clone()) {
                        merged.push(value);
                        if let Some(limit) = limit {
                            if merged.len() >= limit {
                                return Ok(merged);
                            }
                        }
                    }
                }
            }
            Err(err) if merged.is_empty() => return Err(err),
            Err(_) => {}
        }
    }

    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    #[tokio::test]
    async fn test_fallback_to_secondary() {
        let primary = MemoryStore::new();
        let fallback = MemoryStore::new();

        let value = Value::utf8("b");
        fallback.put("k", value.clone(), None, None).await.unwrap();

        let wrapper = FallbackWrapper::new(primary, fallback);
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_fallback_prefers_primary() {
        let primary = MemoryStore::new();
        let fallback = MemoryStore::new();

        let pval = Value::utf8("primary");
        primary.put("k", pval.clone(), None, None).await.unwrap();

        let fval = Value::utf8("fallback");
        fallback.put("k", fval, None, None).await.unwrap();

        let wrapper = FallbackWrapper::new(primary, fallback);
        let got = wrapper.get("k", None).await.unwrap();
        assert_eq!(got, Some(pval));
    }
}
