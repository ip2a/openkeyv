use crate::error::Result;
use crate::protocol::{AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue};
use crate::value::Value;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};

pub trait RoutedStore:
    AsyncKeyValue + AsyncEnumerateKeys + AsyncEnumerateCollections + Send + Sync
{
}

impl<T> RoutedStore for T where
    T: AsyncKeyValue + AsyncEnumerateKeys + AsyncEnumerateCollections + Send + Sync
{
}

/// Routes operations to different backing stores based on a routing function.
pub struct RoutingWrapper<F: Fn(Option<&str>, &str) -> usize + Send + Sync> {
    stores: Vec<Box<dyn RoutedStore>>,
    router: F,
}

impl<F: Fn(Option<&str>, &str) -> usize + Send + Sync> RoutingWrapper<F> {
    pub fn new(stores: Vec<Box<dyn RoutedStore>>, router: F) -> Self {
        Self { stores, router }
    }

    fn route(&self, collection: Option<&str>, key: &str) -> Result<&dyn RoutedStore> {
        let idx = (self.router)(collection, key);
        self.stores.get(idx).map(|b| b.as_ref()).ok_or_else(|| {
            crate::error::Error::InvalidOperation(format!("route index {} out of bounds", idx))
        })
    }
}

#[async_trait]
impl<F: Fn(Option<&str>, &str) -> usize + Send + Sync> AsyncKeyValue for RoutingWrapper<F> {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        self.route(collection, key)?.get(key, collection).await
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        self.route(collection, key)?.ttl(key, collection).await
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.route(collection, key)?
            .put(key, value, collection, ttl)
            .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.route(collection, key)?.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        // Route each key individually; if all route to same store, use bulk
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let first_idx = (self.router)(collection, &keys[0]);
        let same_store = keys
            .iter()
            .all(|k| (self.router)(collection, k) == first_idx);
        if same_store {
            self.route(collection, &keys[0])?
                .get_many(keys, collection)
                .await
        } else {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                results.push(self.get(key, collection).await?);
            }
            Ok(results)
        }
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let first_idx = (self.router)(collection, &keys[0]);
        let same_store = keys
            .iter()
            .all(|k| (self.router)(collection, k) == first_idx);
        if same_store {
            self.route(collection, &keys[0])?
                .ttl_many(keys, collection)
                .await
        } else {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                results.push(self.ttl(key, collection).await?);
            }
            Ok(results)
        }
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let first_idx = (self.router)(collection, &keys[0]);
        let same_store = keys
            .iter()
            .all(|k| (self.router)(collection, k) == first_idx);
        if same_store {
            self.route(collection, &keys[0])?
                .put_many(keys, values, collection, ttl)
                .await
        } else {
            for (key, value) in keys.iter().zip(values.iter()) {
                self.put(key, value.clone(), collection, ttl).await?;
            }
            Ok(())
        }
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let first_idx = (self.router)(collection, &keys[0]);
        let same_store = keys
            .iter()
            .all(|k| (self.router)(collection, k) == first_idx);
        if same_store {
            self.route(collection, &keys[0])?
                .delete_many(keys, collection)
                .await
        } else {
            let mut count = 0;
            for key in keys {
                if self.delete(key, collection).await? {
                    count += 1;
                }
            }
            Ok(count)
        }
    }
}

#[async_trait]
impl<F: Fn(Option<&str>, &str) -> usize + Send + Sync> AsyncEnumerateKeys for RoutingWrapper<F> {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let mut seen = HashSet::new();
        let mut merged = Vec::new();
        for store in &self.stores {
            for key in store.keys(collection, None).await? {
                if seen.insert(key.clone()) {
                    merged.push(key);
                    if let Some(limit) = limit {
                        if merged.len() >= limit {
                            return Ok(merged);
                        }
                    }
                }
            }
        }
        Ok(merged)
    }
}

#[async_trait]
impl<F: Fn(Option<&str>, &str) -> usize + Send + Sync> AsyncEnumerateCollections
    for RoutingWrapper<F>
{
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let mut seen = HashSet::new();
        let mut merged = Vec::new();
        for store in &self.stores {
            for collection in store.collections(None).await? {
                if seen.insert(collection.clone()) {
                    merged.push(collection);
                    if let Some(limit) = limit {
                        if merged.len() >= limit {
                            return Ok(merged);
                        }
                    }
                }
            }
        }
        Ok(merged)
    }
}

/// Convenience wrapper that routes by collection name to a specific store.
pub struct CollectionRoutingWrapper {
    stores: HashMap<String, Box<dyn RoutedStore>>,
}

impl CollectionRoutingWrapper {
    pub fn new(routes: HashMap<String, Box<dyn RoutedStore>>) -> Self {
        Self { stores: routes }
    }

    fn resolve(&self, collection: Option<&str>) -> Result<&dyn RoutedStore> {
        let collection = collection.ok_or_else(|| {
            crate::error::Error::InvalidOperation(
                "collection routing requires an explicit collection".to_string(),
            )
        })?;

        self.stores
            .get(collection)
            .map(|store| store.as_ref())
            .ok_or_else(|| {
                crate::error::Error::InvalidOperation(format!(
                    "no store configured for collection {collection}"
                ))
            })
    }
}

#[async_trait]
impl AsyncKeyValue for CollectionRoutingWrapper {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        self.resolve(collection)?.get(key, collection).await
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        self.resolve(collection)?.ttl(key, collection).await
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.resolve(collection)?
            .put(key, value, collection, ttl)
            .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        self.resolve(collection)?.delete(key, collection).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        self.resolve(collection)?.get_many(keys, collection).await
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        self.resolve(collection)?.ttl_many(keys, collection).await
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        self.resolve(collection)?
            .put_many(keys, values, collection, ttl)
            .await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        self.resolve(collection)?
            .delete_many(keys, collection)
            .await
    }
}

#[async_trait]
impl AsyncEnumerateKeys for CollectionRoutingWrapper {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        self.resolve(collection)?.keys(collection, limit).await
    }
}

#[async_trait]
impl AsyncEnumerateCollections for CollectionRoutingWrapper {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let mut seen = HashSet::new();
        let mut merged = Vec::new();

        for (collection_name, store) in &self.stores {
            if seen.insert(collection_name.clone()) {
                merged.push(collection_name.clone());
                if let Some(limit) = limit {
                    if merged.len() >= limit {
                        return Ok(merged);
                    }
                }
            }
            for collection in store.collections(None).await? {
                if seen.insert(collection.clone()) {
                    merged.push(collection);
                    if let Some(limit) = limit {
                        if merged.len() >= limit {
                            return Ok(merged);
                        }
                    }
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

    #[tokio::test]
    async fn test_collection_routing() {
        let users_store = MemoryStore::new();
        let orders_store = MemoryStore::new();

        let mut routes = HashMap::new();
        routes.insert(
            "users".to_string(),
            Box::new(users_store) as Box<dyn RoutedStore>,
        );
        routes.insert(
            "orders".to_string(),
            Box::new(orders_store) as Box<dyn RoutedStore>,
        );

        let router = CollectionRoutingWrapper::new(routes);

        let value = Value::utf8("b");
        router
            .put("k", value.clone(), Some("users"), None)
            .await
            .unwrap();

        let got = router.get("k", Some("users")).await.unwrap();
        assert_eq!(got, Some(value));

        let missing = router.get("k", Some("orders")).await.unwrap();
        assert_eq!(missing, None);

        assert!(router.get("k", Some("unknown")).await.is_err());
        assert!(router.get("k", None).await.is_err());
    }

    #[tokio::test]
    async fn routing_wrapper_rejects_invalid_batch_route() {
        let router = RoutingWrapper::new(
            vec![Box::new(MemoryStore::new()) as Box<dyn RoutedStore>],
            |_collection, _key| 1,
        );
        let keys = vec!["key".to_string()];

        assert!(router.get_many(&keys, None).await.is_err());
        assert!(router.ttl_many(&keys, None).await.is_err());
        assert!(
            router
                .put_many(&keys, &[Value::null()], None, None)
                .await
                .is_err()
        );
        assert!(router.delete_many(&keys, None).await.is_err());
    }
}
