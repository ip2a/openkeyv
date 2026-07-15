use super::client::MemoryClient;
use super::config::{MemoryConfig, SeedData};
use super::error::{Error, Result};
use crate::change::{ChangeFeedRequest, ChangeOperation, ChangeSubscription};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncChangeFeed, AsyncCompareAndSwap, AsyncCull, AsyncDestroyCollection, AsyncDestroyStore,
    AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue, CompareAndDeleteResult,
    CompareAndSwapResult, Revision, RevisionedValue,
};
use crate::value::Value;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;

const DEFAULT_PAGE_SIZE: usize = 10_000;
const PAGE_LIMIT: usize = 10_000;

/// A fixed-size in-memory key-value store using time-aware LRU cache per collection.
#[derive(Clone)]
pub struct MemoryStore {
    client: Arc<MemoryClient>,
    config: MemoryConfig,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::with_options(None, None, None)
    }

    pub fn with_options(
        max_entries_per_collection: Option<usize>,
        default_collection: Option<String>,
        seed: Option<SeedData>,
    ) -> Self {
        Self::with_config(MemoryConfig::new(
            max_entries_per_collection,
            default_collection,
            seed,
        ))
    }

    pub fn with_config(config: MemoryConfig) -> Self {
        Self {
            client: Arc::new(MemoryClient::new()),
            config,
        }
    }

    async fn setup(&self) -> Result<()> {
        let mut complete = self.client.setup_complete().write().await;
        if *complete {
            return Ok(());
        }

        if let Some(seed) = &self.config.seed {
            for (collection, items) in seed {
                let col = self
                    .client
                    .collections()
                    .entry(collection.clone())
                    .or_default();
                for (key, value) in items {
                    let entry = ManagedEntry::new(value.clone());
                    col.insert(key.clone(), entry);
                }
            }
        }

        *complete = true;
        Ok(())
    }

    async fn setup_collection(&self, collection: &str) -> Result<()> {
        self.setup().await?;
        self.client
            .collections()
            .entry(collection.to_string())
            .or_default();
        Ok(())
    }

    fn revision_key(collection: &str, key: &str) -> String {
        format!("{collection}\0{key}")
    }

    fn current_revision(&self, collection: &str, key: &str) -> Option<Revision> {
        self.client
            .revisions()
            .get(&Self::revision_key(collection, key))
            .map(|entry| *entry)
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn get_collection(
        &self,
        name: &str,
    ) -> Result<dashmap::mapref::one::Ref<'_, String, DashMap<String, ManagedEntry>>> {
        self.client
            .collections()
            .get(name)
            .ok_or_else(|| Error::InvalidOperation(format!("collection '{}' not found", name)))
    }

    fn maybe_cull_collection(&self, col: &DashMap<String, ManagedEntry>) {
        if let Some(max) = self.config.max_entries_per_collection {
            if col.len() > max {
                col.retain(|_k, v| !v.is_expired());
                while col.len() > max {
                    if let Some(k) = col.iter().next().map(|e| e.key().clone()) {
                        col.remove(&k);
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncKeyValue for MemoryStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        match col.get(key) {
            Some(entry) if !entry.is_expired() => Ok(Some(entry.value.clone())),
            _ => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        match col.get(key) {
            Some(entry) if !entry.is_expired() => {
                let ttl = entry.ttl();
                Ok(Some((entry.value.clone(), ttl)))
            }
            _ => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };

        let _mutation = self.client.mutation_lock().lock().await;
        if let Some(col) = self.client.collections().get_mut(cname) {
            self.maybe_cull_collection(&col);
            col.insert(key.to_string(), entry);
        }
        let revision = self
            .client
            .record_change(cname, key, ChangeOperation::Put)
            .await;
        self.client
            .revisions()
            .insert(Self::revision_key(cname, key), revision);
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let _mutation = self.client.mutation_lock().lock().await;
        let col = self.get_collection(cname)?;
        let deleted = col.remove(key).is_some();
        if deleted {
            let revision = self
                .client
                .record_change(cname, key, ChangeOperation::Delete)
                .await;
            self.client
                .revisions()
                .insert(Self::revision_key(cname, key), revision);
        }
        Ok(deleted)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        let results: Vec<_> = keys
            .iter()
            .map(|k| {
                col.get(k)
                    .filter(|e| !e.is_expired())
                    .map(|e| e.value.clone())
            })
            .collect();
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        let results: Vec<_> = keys
            .iter()
            .map(|k| {
                col.get(k).filter(|e| !e.is_expired()).map(|e| {
                    let ttl = e.ttl();
                    (e.value.clone(), ttl)
                })
            })
            .collect();
        Ok(results)
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        if keys.len() != values.len() {
            return Err(Error::BatchSizeMismatch {
                keys: keys.len(),
                values: values.len(),
            });
        }
        if let Some(seconds) = ttl {
            ManagedEntry::validate_ttl(seconds)?;
        }
        let cname = self.collection_name(collection).to_string();
        for (key, value) in keys.iter().zip(values.iter()) {
            self.put(key, value.clone(), Some(&cname), ttl).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection).to_string();
        let mut deleted = 0;
        for key in keys {
            if self.delete(key, Some(&cname)).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

#[async_trait]
impl AsyncCompareAndSwap for MemoryStore {
    async fn get_with_revision(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<RevisionedValue>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;
        let _mutation = self.client.mutation_lock().lock().await;
        let entry = self
            .get_collection(cname)?
            .get(key)
            .map(|entry| entry.clone())
            .filter(|entry| !entry.is_expired());
        let Some(entry) = entry else { return Ok(None) };
        let revision = self
            .current_revision(cname, key)
            .ok_or(Error::CorruptedData)?;
        let ttl = entry.ttl();
        Ok(Some(RevisionedValue {
            value: entry.value,
            revision,
            ttl,
        }))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&Revision>,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<CompareAndSwapResult> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        let _mutation = self.client.mutation_lock().lock().await;
        let current = self
            .get_collection(cname)?
            .get(key)
            .map(|entry| entry.clone())
            .filter(|entry| !entry.is_expired());
        let current_revision = current
            .as_ref()
            .and_then(|_| self.current_revision(cname, key));
        if current_revision.as_ref() != expected {
            return Ok(CompareAndSwapResult::Conflict {
                current: current
                    .zip(current_revision)
                    .map(|(entry, revision)| RevisionedValue {
                        ttl: entry.ttl(),
                        value: entry.value,
                        revision,
                    }),
            });
        }
        self.get_collection(cname)?.insert(key.to_string(), entry);
        let revision = self
            .client
            .record_change(cname, key, ChangeOperation::Put)
            .await;
        self.client
            .revisions()
            .insert(Self::revision_key(cname, key), revision);
        Ok(CompareAndSwapResult::Applied { revision })
    }

    async fn compare_and_delete(
        &self,
        key: &str,
        expected: &Revision,
        collection: Option<&str>,
    ) -> Result<CompareAndDeleteResult> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;
        let _mutation = self.client.mutation_lock().lock().await;
        let current = self
            .get_collection(cname)?
            .get(key)
            .map(|entry| entry.clone())
            .filter(|entry| !entry.is_expired());
        let current_revision = current
            .as_ref()
            .and_then(|_| self.current_revision(cname, key));
        if current_revision.as_ref() != Some(expected) {
            return Ok(CompareAndDeleteResult::Conflict {
                current: current
                    .zip(current_revision)
                    .map(|(entry, revision)| RevisionedValue {
                        ttl: entry.ttl(),
                        value: entry.value,
                        revision,
                    }),
            });
        }
        self.get_collection(cname)?.remove(key);
        let revision = self
            .client
            .record_change(cname, key, ChangeOperation::Delete)
            .await;
        self.client
            .revisions()
            .insert(Self::revision_key(cname, key), revision);
        Ok(CompareAndDeleteResult::Deleted)
    }
}

#[async_trait]
impl AsyncChangeFeed for MemoryStore {
    async fn subscribe(&self, request: ChangeFeedRequest) -> Result<ChangeSubscription> {
        let stream = self.client.subscribe(request.start, request.filter).await?;
        Ok(ChangeSubscription::new(stream))
    }
}

#[async_trait]
impl AsyncCull for MemoryStore {
    async fn cull(&self) -> Result<()> {
        for entry in self.client.collections().iter() {
            let col = entry.value();
            col.retain(|_k, v| !v.is_expired());
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for MemoryStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        Ok(col.iter().take(limit).map(|e| e.key().clone()).collect())
    }
}

#[async_trait]
impl AsyncEnumerateCollections for MemoryStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        self.setup().await?;
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        Ok(self
            .client
            .collections()
            .iter()
            .take(limit)
            .map(|e| e.key().clone())
            .collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for MemoryStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        self.setup().await?;
        Ok(self.client.collections().remove(collection).is_some())
    }
}

#[async_trait]
impl AsyncDestroyStore for MemoryStore {
    async fn destroy(&self) -> Result<bool> {
        self.client.collections().clear();
        let mut complete = self.client.setup_complete().write().await;
        *complete = false;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_store_basic() {
        let store = MemoryStore::new();
        let value = Value::utf8("test");

        store.put("key1", value.clone(), None, None).await.unwrap();
        let result = store.get("key1", None).await.unwrap();
        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_memory_store_preserves_missing_ttl() {
        let store = MemoryStore::new();
        let value = Value::utf8("persistent");
        let keys = vec!["key1".to_string(), "missing".to_string()];

        store.put("key1", value.clone(), None, None).await.unwrap();

        assert_eq!(
            store.ttl("key1", None).await.unwrap(),
            Some((value.clone(), None))
        );
        assert_eq!(
            store.ttl_many(&keys, None).await.unwrap(),
            vec![Some((value, None)), None]
        );
    }

    #[tokio::test]
    async fn test_memory_store_ttl() {
        let store = MemoryStore::new();
        let value = Value::null();

        store
            .put("key1", value.clone(), None, Some(0.01))
            .await
            .unwrap();
        assert!(store.get("key1", None).await.unwrap().is_some());

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert!(store.get("key1", None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memory_store_delete() {
        let store = MemoryStore::new();
        let value = Value::null();

        store.put("key1", value, None, None).await.unwrap();
        assert!(store.delete("key1", None).await.unwrap());
        assert!(!store.delete("key1", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_store_collections() {
        let store = MemoryStore::new();
        let value = Value::null();

        store
            .put("k", value.clone(), Some("c1"), None)
            .await
            .unwrap();
        store.put("k", value, Some("c2"), None).await.unwrap();

        let cols = store.collections(None).await.unwrap();
        assert!(cols.contains(&"c1".to_string()));
        assert!(cols.contains(&"c2".to_string()));
    }

    #[tokio::test]
    async fn test_memory_store_bulk() {
        let store = MemoryStore::new();
        let v1 = Value::integer(1);
        let v2 = Value::integer(2);

        store
            .put_many(
                &["k1".to_string(), "k2".to_string()],
                &[v1.clone(), v2.clone()],
                None,
                None,
            )
            .await
            .unwrap();

        let results = store
            .get_many(&["k1".to_string(), "k2".to_string()], None)
            .await
            .unwrap();
        assert_eq!(results, vec![Some(v1), Some(v2)]);
    }

    #[tokio::test]
    async fn test_change_feed_delivers_live_mutations_across_clones() {
        let store = MemoryStore::new();
        let writer = store.clone();
        let mut changes = store.subscribe(ChangeFeedRequest::default()).await.unwrap();

        writer
            .put("service-1", Value::utf8("ready"), Some("events"), None)
            .await
            .unwrap();
        writer.delete("service-1", Some("events")).await.unwrap();

        let put = tokio::time::timeout(std::time::Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let delete = tokio::time::timeout(std::time::Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(put.revision, 1);
        assert_eq!(put.collection, "events");
        assert_eq!(put.key, "service-1");
        assert_eq!(put.operation, ChangeOperation::Put);
        assert_eq!(delete.revision, 2);
        assert_eq!(delete.operation, ChangeOperation::Delete);
    }

    #[tokio::test]
    async fn test_change_feed_replays_and_resumes_after_cursor() {
        let store = MemoryStore::new();
        store
            .put("event-1", Value::integer(1), Some("events"), None)
            .await
            .unwrap();
        store
            .put("event-2", Value::integer(2), Some("events"), None)
            .await
            .unwrap();

        let mut replay = store
            .subscribe(ChangeFeedRequest {
                start: crate::change::ChangeStart::Beginning,
                filter: crate::change::ChangeFilter::collection("events"),
            })
            .await
            .unwrap();
        let first = replay.recv().await.unwrap().unwrap();
        let second = replay.recv().await.unwrap().unwrap();
        assert_eq!(first.key, "event-1");
        assert_eq!(second.key, "event-2");

        let mut resumed = store
            .subscribe(ChangeFeedRequest {
                start: crate::change::ChangeStart::After(first.cursor),
                filter: crate::change::ChangeFilter::collection("events"),
            })
            .await
            .unwrap();
        assert_eq!(resumed.recv().await.unwrap().unwrap().key, "event-2");

        store
            .put("event-3", Value::integer(3), Some("events"), None)
            .await
            .unwrap();
        assert_eq!(resumed.recv().await.unwrap().unwrap().key, "event-3");
    }

    #[tokio::test]
    async fn test_change_feed_filters_collections_and_operations() {
        let store = MemoryStore::new();
        let mut changes = store
            .subscribe(ChangeFeedRequest {
                start: crate::change::ChangeStart::Latest,
                filter: crate::change::ChangeFilter {
                    collections: vec!["events".to_string()],
                    operations: vec![ChangeOperation::Put],
                },
            })
            .await
            .unwrap();

        store
            .put("ignored", Value::null(), Some("state"), None)
            .await
            .unwrap();
        store
            .put("deleted", Value::null(), Some("events"), None)
            .await
            .unwrap();
        store.delete("deleted", Some("events")).await.unwrap();
        store
            .put("matched", Value::null(), Some("events"), None)
            .await
            .unwrap();

        let first = changes.recv().await.unwrap().unwrap();
        let second = changes.recv().await.unwrap().unwrap();
        assert_eq!(first.key, "deleted");
        assert_eq!(second.key, "matched");
        assert_eq!(second.operation, ChangeOperation::Put);
    }

    #[tokio::test]
    async fn test_change_feed_does_not_report_missing_delete() {
        let store = MemoryStore::new();
        let mut changes = store.subscribe(ChangeFeedRequest::default()).await.unwrap();

        assert!(!store.delete("missing", Some("events")).await.unwrap());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), changes.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_change_feed_rejects_expired_cursor() {
        let store = MemoryStore::new();
        for revision in 0..=super::super::client::CHANGE_RETENTION {
            store
                .put(
                    &format!("event-{revision}"),
                    Value::integer(revision as i64),
                    Some("events"),
                    None,
                )
                .await
                .unwrap();
        }

        let result = store
            .subscribe(ChangeFeedRequest {
                start: crate::change::ChangeStart::After(crate::change::ChangeCursor::new("0")),
                filter: crate::change::ChangeFilter::default(),
            })
            .await;
        assert!(matches!(
            result,
            Err(crate::error::Error::ChangeCursorExpired { .. })
        ));
    }

    #[tokio::test]
    async fn test_memory_store_destroy() {
        let store = MemoryStore::new();
        let value = Value::null();
        store.put("k", value, None, None).await.unwrap();

        assert!(store.destroy().await.unwrap());
        assert_eq!(store.get("k", None).await.unwrap(), None);
    }
}
