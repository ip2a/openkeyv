use std::sync::Arc;

use crate::migration::{AsyncKeyspaceMigration, MigrationOptions, MigrationReport};
use crate::protocol::{
    AsyncChangeFeed, AsyncCompareAndSwap, AsyncEnumerateCollections, AsyncEnumerateKeys, BaseStore,
};
use crate::utils::compound::Subspace;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreCapabilities {
    pub enumerate_keys: bool,
    pub enumerate_collections: bool,
    pub compare_and_swap: bool,
    pub change_feed: bool,
}

#[derive(Clone)]
pub struct StoreHandle {
    pub base: Arc<dyn BaseStore>,
    pub capabilities: StoreCapabilities,
    pub enumerate_keys: Option<Arc<dyn AsyncEnumerateKeys>>,
    pub enumerate_collections: Option<Arc<dyn AsyncEnumerateCollections>>,
    pub compare_and_swap: Option<Arc<dyn AsyncCompareAndSwap>>,
    pub change_feed: Option<Arc<dyn AsyncChangeFeed>>,
    pub keyspace_migration: Option<Arc<dyn AsyncKeyspaceMigration>>,
}

impl StoreHandle {
    fn missing(name: &str) -> crate::Error {
        crate::Error::InvalidOperation(format!("Store does not provide {name}"))
    }
}

#[async_trait::async_trait]
impl crate::AsyncKeyValue for StoreHandle {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> crate::Result<Option<crate::Value>> {
        self.base.get(key, collection).await
    }
    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> crate::Result<Option<(crate::Value, Option<f64>)>> {
        self.base.ttl(key, collection).await
    }
    async fn put(
        &self,
        key: &str,
        value: crate::Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> crate::Result<()> {
        self.base.put(key, value, collection, ttl).await
    }
    async fn delete(&self, key: &str, collection: Option<&str>) -> crate::Result<bool> {
        self.base.delete(key, collection).await
    }
    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> crate::Result<Vec<Option<crate::Value>>> {
        self.base.get_many(keys, collection).await
    }
    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> crate::Result<Vec<Option<(crate::Value, Option<f64>)>>> {
        self.base.ttl_many(keys, collection).await
    }
    async fn put_many(
        &self,
        keys: &[String],
        values: &[crate::Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> crate::Result<()> {
        self.base.put_many(keys, values, collection, ttl).await
    }
    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> crate::Result<usize> {
        self.base.delete_many(keys, collection).await
    }
}

#[async_trait::async_trait]
impl crate::AsyncEnumerateKeys for StoreHandle {
    async fn keys(
        &self,
        collection: Option<&str>,
        limit: Option<usize>,
    ) -> crate::Result<Vec<String>> {
        self.enumerate_keys
            .as_ref()
            .ok_or_else(|| Self::missing("key enumeration"))?
            .keys(collection, limit)
            .await
    }
}

#[async_trait::async_trait]
impl crate::AsyncEnumerateCollections for StoreHandle {
    async fn collections(&self, limit: Option<usize>) -> crate::Result<Vec<String>> {
        self.enumerate_collections
            .as_ref()
            .ok_or_else(|| Self::missing("collection enumeration"))?
            .collections(limit)
            .await
    }
}

#[async_trait::async_trait]
impl crate::AsyncCompareAndSwap for StoreHandle {
    async fn get_with_revision(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> crate::Result<Option<crate::RevisionedValue>> {
        self.compare_and_swap
            .as_ref()
            .ok_or_else(|| Self::missing("compare-and-swap"))?
            .get_with_revision(key, collection)
            .await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&crate::Revision>,
        value: crate::Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> crate::Result<crate::CompareAndSwapResult> {
        self.compare_and_swap
            .as_ref()
            .ok_or_else(|| Self::missing("compare-and-swap"))?
            .compare_and_swap(key, expected, value, collection, ttl)
            .await
    }
    async fn compare_and_delete(
        &self,
        key: &str,
        expected: &crate::Revision,
        collection: Option<&str>,
    ) -> crate::Result<crate::CompareAndDeleteResult> {
        self.compare_and_swap
            .as_ref()
            .ok_or_else(|| Self::missing("compare-and-swap"))?
            .compare_and_delete(key, expected, collection)
            .await
    }
}

#[async_trait::async_trait]
impl crate::AsyncChangeFeed for StoreHandle {
    async fn subscribe(
        &self,
        request: crate::ChangeFeedRequest,
    ) -> crate::Result<crate::ChangeSubscription> {
        self.change_feed
            .as_ref()
            .ok_or_else(|| Self::missing("change feed"))?
            .subscribe(request)
            .await
    }
}

impl StoreHandle {
    pub fn basic<T>(store: Arc<T>) -> Self
    where
        T: BaseStore + 'static,
    {
        Self {
            base: store,
            capabilities: StoreCapabilities::default(),
            enumerate_keys: None,
            enumerate_collections: None,
            compare_and_swap: None,
            change_feed: None,
            keyspace_migration: None,
        }
    }

    pub fn with_capabilities<T>(
        store: Arc<T>,
        enumerate_keys: Option<Arc<dyn AsyncEnumerateKeys>>,
        enumerate_collections: Option<Arc<dyn AsyncEnumerateCollections>>,
        compare_and_swap: Option<Arc<dyn AsyncCompareAndSwap>>,
        change_feed: Option<Arc<dyn AsyncChangeFeed>>,
    ) -> Self
    where
        T: BaseStore + 'static,
    {
        Self {
            base: store,
            capabilities: StoreCapabilities {
                enumerate_keys: enumerate_keys.is_some(),
                enumerate_collections: enumerate_collections.is_some(),
                compare_and_swap: compare_and_swap.is_some(),
                change_feed: change_feed.is_some(),
            },
            enumerate_keys,
            enumerate_collections,
            compare_and_swap,
            change_feed,
            keyspace_migration: None,
        }
    }

    pub fn with_keyspace_migration<T>(mut self, migration: Arc<T>) -> Self
    where
        T: AsyncKeyspaceMigration + 'static,
    {
        self.keyspace_migration = Some(migration);
        self
    }
}

#[async_trait::async_trait]
impl AsyncKeyspaceMigration for StoreHandle {
    async fn migrate_into_keyspace(
        &self,
        keyspace: &Subspace,
        options: &MigrationOptions,
    ) -> crate::Result<MigrationReport> {
        self.keyspace_migration
            .as_ref()
            .ok_or_else(|| Self::missing("keyspace migration"))?
            .migrate_into_keyspace(keyspace, options)
            .await
    }
}
