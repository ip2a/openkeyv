use super::client::{MemoryClient, RevisionedEntry, RevisionedEntrySnapshot};
use super::config::{MemoryConfig, SeedData};
use super::error::{Error, Result};
use crate::change::{ChangeFeedRequest, ChangeOperation, ChangeStream};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncChangeFeed, AsyncCompareAndSwap, AsyncCull, AsyncDestroyCollection, AsyncDestroyStore,
    AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue, CompareAndDeleteResult,
    CompareAndSwapResult, Revision, RevisionedValue,
};
use crate::value::Value;
use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::sync::Arc;

const DEFAULT_PAGE_SIZE: usize = 10_000;
const PAGE_LIMIT: usize = 10_000;

/// An in-memory key-value store with an optional per-collection entry limit.
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
                    let revision = Revision::fresh()?;
                    col.insert(key.clone(), RevisionedEntry { entry, revision });
                }
                let _ = self.enforce_capacity(&col);
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

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn get_collection(
        &self,
        name: &str,
    ) -> Result<dashmap::mapref::one::Ref<'_, String, DashMap<String, RevisionedEntry>>> {
        self.client
            .collections()
            .get(name)
            .ok_or_else(|| Error::InvalidOperation(format!("collection '{}' not found", name)))
    }

    fn enforce_capacity(&self, col: &DashMap<String, RevisionedEntry>) -> Vec<String> {
        let Some(max) = self.config.max_entries_per_collection else {
            return Vec::new();
        };

        let mut deleted = Vec::new();
        col.retain(|key, value| {
            let keep = !value.entry.is_expired();
            if !keep {
                deleted.push(key.clone());
            }
            keep
        });
        while col.len() > max {
            let Some(key) = col.iter().map(|entry| entry.key().clone()).min() else {
                break;
            };
            if col.remove(&key).is_some() {
                deleted.push(key);
            }
        }
        deleted
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

fn snapshot_to_revisioned_value(snapshot: RevisionedEntrySnapshot) -> RevisionedValue {
    RevisionedValue {
        value: snapshot.value,
        revision: snapshot.revision,
        ttl: snapshot.ttl,
    }
}

#[async_trait]
impl AsyncKeyValue for MemoryStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let col = self.get_collection(cname)?;
        match col.get(key) {
            Some(rev) if !rev.entry.is_expired() => Ok(Some(rev.entry.value.clone())),
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
            Some(rev) if !rev.entry.is_expired() => {
                let ttl = rev.entry.ttl();
                Ok(Some((rev.entry.value.clone(), ttl)))
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
        let revision = Revision::fresh()?;

        let _mutation = self.client.mutation_lock().lock().await;
        let deleted = if let Some(col) = self.client.collections().get_mut(cname) {
            col.insert(key.to_string(), RevisionedEntry { entry, revision });
            self.enforce_capacity(&col)
        } else {
            Vec::new()
        };
        self.client
            .record_change(cname, key, ChangeOperation::Put)
            .await;
        for deleted_key in deleted {
            self.client
                .record_change(cname, &deleted_key, ChangeOperation::Delete)
                .await;
        }
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let _mutation = self.client.mutation_lock().lock().await;
        let col = self.get_collection(cname)?;
        let deleted = col.remove(key).is_some();
        if deleted {
            self.client
                .record_change(cname, key, ChangeOperation::Delete)
                .await;
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
                    .filter(|rev| !rev.entry.is_expired())
                    .map(|rev| rev.entry.value.clone())
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
                col.get(k).filter(|rev| !rev.entry.is_expired()).map(|rev| {
                    let ttl = rev.entry.ttl();
                    (rev.entry.value.clone(), ttl)
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

        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let entries = values
            .iter()
            .map(|value| match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => Ok(ManagedEntry::new(value.clone())),
            })
            .collect::<Result<Vec<_>>>()?;
        let revisions = (0..entries.len())
            .map(|_| Revision::fresh())
            .collect::<Result<Vec<_>>>()?;

        let _mutation = self.client.mutation_lock().lock().await;
        let deleted = if let Some(col) = self.client.collections().get_mut(cname) {
            for ((key, entry), revision) in keys.iter().zip(entries).zip(revisions) {
                col.insert(key.clone(), RevisionedEntry { entry, revision });
            }
            self.enforce_capacity(&col)
        } else {
            Vec::new()
        };
        for key in keys {
            self.client
                .record_change(cname, key, ChangeOperation::Put)
                .await;
        }
        for deleted_key in deleted {
            self.client
                .record_change(cname, &deleted_key, ChangeOperation::Delete)
                .await;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let _mutation = self.client.mutation_lock().lock().await;
        let col = self.get_collection(cname)?;
        let mut deleted = Vec::new();
        for key in keys {
            if col.remove(key).is_some() {
                deleted.push(key);
            }
        }
        for key in &deleted {
            self.client
                .record_change(cname, key, ChangeOperation::Delete)
                .await;
        }
        Ok(deleted.len())
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

        let col = self.get_collection(cname)?;
        Ok(match col.get(key) {
            Some(rev) if !rev.entry.is_expired() => {
                Some(snapshot_to_revisioned_value(rev.snapshot()))
            }
            _ => None,
        })
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&Revision>,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<CompareAndSwapResult> {
        if let Some(seconds) = ttl {
            ManagedEntry::validate_ttl(seconds)?;
        }

        let cname = self.collection_name(collection);
        self.setup_collection(cname).await?;

        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        // Generate the new revision before taking any mutation lock so a
        // randomness failure cannot leave a partial write behind.
        let new_revision = Revision::fresh()?;

        let _mutation = self.client.mutation_lock().lock().await;
        let col = self.get_collection(cname)?;
        let mut entry_guard = match col.entry(key.to_string()) {
            Entry::Occupied(occupied) => occupied,
            Entry::Vacant(vacant) => match expected {
                None => {
                    vacant.insert(RevisionedEntry {
                        entry,
                        revision: new_revision,
                    });
                    self.client
                        .record_change(cname, key, ChangeOperation::Put)
                        .await;
                    return Ok(CompareAndSwapResult::Applied {
                        revision: new_revision,
                    });
                }
                Some(_) => {
                    return Ok(CompareAndSwapResult::Conflict { current: None });
                }
            },
        };

        let current = entry_guard.get();
        if current.entry.is_expired() {
            // Expired entries are treated exactly as absent.
            match expected {
                None => {
                    entry_guard.insert(RevisionedEntry {
                        entry,
                        revision: new_revision,
                    });
                    self.client
                        .record_change(cname, key, ChangeOperation::Put)
                        .await;
                    return Ok(CompareAndSwapResult::Applied {
                        revision: new_revision,
                    });
                }
                Some(_) => {
                    // Remove the expired occupied entry within the same atomic
                    // operation before reporting absence.
                    entry_guard.remove();
                    return Ok(CompareAndSwapResult::Conflict { current: None });
                }
            }
        }

        match expected {
            None => Ok(CompareAndSwapResult::Conflict {
                current: Some(snapshot_to_revisioned_value(current.snapshot())),
            }),
            Some(expected_revision) if expected_revision == &current.revision => {
                entry_guard.insert(RevisionedEntry {
                    entry,
                    revision: new_revision,
                });
                self.client
                    .record_change(cname, key, ChangeOperation::Put)
                    .await;
                Ok(CompareAndSwapResult::Applied {
                    revision: new_revision,
                })
            }
            Some(_) => Ok(CompareAndSwapResult::Conflict {
                current: Some(snapshot_to_revisioned_value(current.snapshot())),
            }),
        }
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
        let col = self.get_collection(cname)?;
        let entry_guard = match col.entry(key.to_string()) {
            Entry::Occupied(occupied) => occupied,
            Entry::Vacant(_) => {
                return Ok(CompareAndDeleteResult::Conflict { current: None });
            }
        };

        let current = entry_guard.get();
        if current.entry.is_expired() {
            // Expired occupied entry is removed within the same atomic operation
            // and treated as absent.
            entry_guard.remove();
            return Ok(CompareAndDeleteResult::Conflict { current: None });
        }

        if expected == &current.revision {
            entry_guard.remove();
            self.client
                .record_change(cname, key, ChangeOperation::Delete)
                .await;
            Ok(CompareAndDeleteResult::Deleted)
        } else {
            Ok(CompareAndDeleteResult::Conflict {
                current: Some(snapshot_to_revisioned_value(current.snapshot())),
            })
        }
    }
}

#[async_trait]
impl AsyncChangeFeed for MemoryStore {
    async fn subscribe(&self, request: ChangeFeedRequest) -> Result<Box<dyn ChangeStream + Send>> {
        let stream = self.client.subscribe(request.start, request.filter).await?;
        Ok(Box::new(stream))
    }
}

#[async_trait]
impl AsyncCull for MemoryStore {
    async fn cull(&self) -> Result<()> {
        let _mutation = self.client.mutation_lock().lock().await;
        let mut deleted = Vec::new();
        for entry in self.client.collections().iter() {
            let collection = entry.key().clone();
            let col = entry.value();
            col.retain(|key, value| {
                let keep = !value.entry.is_expired();
                if !keep {
                    deleted.push((collection.clone(), key.clone()));
                }
                keep
            });
        }
        for (collection, key) in deleted {
            self.client
                .record_change(&collection, &key, ChangeOperation::Delete)
                .await;
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
        let _mutation = self.client.mutation_lock().lock().await;
        Ok(self.client.collections().remove(collection).is_some())
    }
}

#[async_trait]
impl AsyncDestroyStore for MemoryStore {
    async fn destroy(&self) -> Result<bool> {
        let _mutation = self.client.mutation_lock().lock().await;
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
    async fn test_memory_store_enforces_collection_capacity_after_single_and_batch_puts() {
        let store = MemoryStore::with_options(Some(2), None, None);

        store.put("a", Value::utf8("a"), None, None).await.unwrap();
        store.put("b", Value::utf8("b"), None, None).await.unwrap();
        store.put("c", Value::utf8("c"), None, None).await.unwrap();

        assert_eq!(store.get("a", None).await.unwrap(), None);
        assert!(store.get("b", None).await.unwrap().is_some());
        assert!(store.get("c", None).await.unwrap().is_some());

        store
            .put_many(
                &["d".to_string(), "e".to_string(), "f".to_string()],
                &[Value::utf8("d"), Value::utf8("e"), Value::utf8("f")],
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(store.get("b", None).await.unwrap(), None);
        assert_eq!(store.get("c", None).await.unwrap(), None);
        assert_eq!(store.get("d", None).await.unwrap(), None);
        assert!(store.get("e", None).await.unwrap().is_some());
        assert!(store.get("f", None).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_capacity_eviction_emits_delete_change() {
        let store = MemoryStore::with_options(Some(1), None, None);
        let mut changes = store.subscribe(ChangeFeedRequest::default()).await.unwrap();

        store.put("a", Value::utf8("a"), None, None).await.unwrap();
        changes.recv().await.unwrap().unwrap();
        store.put("b", Value::utf8("b"), None, None).await.unwrap();

        let put = changes.recv().await.unwrap().unwrap();
        let delete = changes.recv().await.unwrap().unwrap();
        assert_eq!(put.key, "b");
        assert_eq!(put.operation, ChangeOperation::Put);
        assert_eq!(delete.key, "a");
        assert_eq!(delete.operation, ChangeOperation::Delete);
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
    async fn test_cull_emits_delete_change_for_expired_entries() {
        let store = MemoryStore::new();
        let mut changes = store.subscribe(ChangeFeedRequest::default()).await.unwrap();

        store
            .put("expired", Value::utf8("value"), Some("events"), Some(0.01))
            .await
            .unwrap();
        changes.recv().await.unwrap().unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        store.cull().await.unwrap();

        let delete = tokio::time::timeout(std::time::Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(delete.collection, "events");
        assert_eq!(delete.key, "expired");
        assert_eq!(delete.operation, ChangeOperation::Delete);
        assert_eq!(store.get("expired", Some("events")).await.unwrap(), None);
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

    // ----- 4B atomic CAS / revision tests -----

    fn conflict_current(result: CompareAndSwapResult) -> Option<RevisionedValue> {
        match result {
            CompareAndSwapResult::Conflict { current } => current,
            CompareAndSwapResult::Applied { .. } => None,
        }
    }

    #[tokio::test]
    async fn test_get_with_revision_missing() {
        let store = MemoryStore::new();
        assert_eq!(
            store.get_with_revision("missing", None).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_cas_create_if_absent_success() {
        let store = MemoryStore::new();
        let result = store
            .compare_and_swap("k", None, Value::utf8("v"), None, None)
            .await
            .unwrap();
        let revision = match result {
            CompareAndSwapResult::Applied { revision } => revision,
            _ => panic!("expected applied"),
        };
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();
        assert_eq!(observed.value, Value::utf8("v"));
        assert_eq!(observed.revision, revision);
        assert_eq!(observed.ttl, None);
    }

    #[tokio::test]
    async fn test_cas_create_if_absent_existing_conflict() {
        let store = MemoryStore::new();
        store.put("k", Value::utf8("v"), None, None).await.unwrap();
        let current = store
            .compare_and_swap("k", None, Value::utf8("other"), None, None)
            .await
            .unwrap();
        match current {
            CompareAndSwapResult::Conflict { current: Some(rev) } => {
                assert_eq!(rev.value, Value::utf8("v"));
                assert_eq!(rev.ttl, None);
            }
            _ => panic!("expected conflict with current"),
        }
    }

    #[tokio::test]
    async fn test_cas_exact_revision_update() {
        let store = MemoryStore::new();
        store.put("k", Value::integer(1), None, None).await.unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();

        let result = store
            .compare_and_swap("k", Some(&observed.revision), Value::integer(2), None, None)
            .await
            .unwrap();
        let new_revision = match result {
            CompareAndSwapResult::Applied { revision } => revision,
            _ => panic!("expected applied"),
        };
        assert_ne!(new_revision, observed.revision);

        let after = store.get_with_revision("k", None).await.unwrap().unwrap();
        assert_eq!(after.value, Value::integer(2));
        assert_eq!(after.revision, new_revision);
    }

    #[tokio::test]
    async fn test_cas_stale_revision_conflict() {
        let store = MemoryStore::new();
        store.put("k", Value::integer(1), None, None).await.unwrap();
        let first = store.get_with_revision("k", None).await.unwrap().unwrap();
        store.put("k", Value::integer(2), None, None).await.unwrap();

        let result = store
            .compare_and_swap("k", Some(&first.revision), Value::integer(3), None, None)
            .await
            .unwrap();
        match result {
            CompareAndSwapResult::Conflict { current: Some(rev) } => {
                assert_eq!(rev.value, Value::integer(2))
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_cas_same_value_changes_revision() {
        let store = MemoryStore::new();
        store
            .put("k", Value::utf8("same"), None, None)
            .await
            .unwrap();
        let before = store.get_with_revision("k", None).await.unwrap().unwrap();

        let result = store
            .compare_and_swap("k", Some(&before.revision), Value::utf8("same"), None, None)
            .await
            .unwrap();
        match result {
            CompareAndSwapResult::Applied { revision } => assert_ne!(revision, before.revision),
            _ => panic!("expected applied"),
        }
    }

    #[tokio::test]
    async fn test_cas_ttl_validation_before_mutation() {
        let store = MemoryStore::new();
        store.put("k", Value::utf8("v"), None, None).await.unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();

        let err = store
            .compare_and_swap(
                "k",
                Some(&observed.revision),
                Value::utf8("v"),
                None,
                Some(-1.0),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidTtl(_)));
        // Entry must be unchanged after the failed attempt.
        let after = store.get_with_revision("k", None).await.unwrap().unwrap();
        assert_eq!(after.revision, observed.revision);
    }

    #[tokio::test]
    async fn test_cas_new_ttl_replaces_old_ttl() {
        let store = MemoryStore::new();
        store
            .put("k", Value::utf8("v"), None, Some(100.0))
            .await
            .unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();
        assert!(observed.ttl.unwrap() > 0.0);

        let result = store
            .compare_and_swap("k", Some(&observed.revision), Value::utf8("v2"), None, None)
            .await
            .unwrap();
        assert!(matches!(result, CompareAndSwapResult::Applied { .. }));

        let after = store.get_with_revision("k", None).await.unwrap().unwrap();
        assert_eq!(after.value, Value::utf8("v2"));
        assert_eq!(after.ttl, None);
    }

    #[tokio::test]
    async fn test_cas_conflict_does_not_refresh_ttl() {
        let store = MemoryStore::new();
        store
            .put("k", Value::utf8("v"), None, Some(100.0))
            .await
            .unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();
        let ttl_before = observed.ttl.unwrap();

        store
            .compare_and_swap(
                "k",
                Some(&Revision::from_bytes([0; 16])),
                Value::utf8("x"),
                None,
                None,
            )
            .await
            .unwrap();
        let after = store.get_with_revision("k", None).await.unwrap().unwrap();
        // TTL should be effectively unchanged (within floating jitter).
        assert!((after.ttl.unwrap() - ttl_before).abs() < 1.0);
    }

    #[tokio::test]
    async fn test_cas_expired_treated_as_absent() {
        let store = MemoryStore::new();
        store
            .put("k", Value::utf8("v"), None, Some(0.01))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // create-if-absent should succeed against the expired entry.
        let result = store
            .compare_and_swap("k", None, Value::utf8("rebuilt"), None, None)
            .await
            .unwrap();
        assert!(matches!(result, CompareAndSwapResult::Applied { .. }));

        // expected-revision against the expired entry must report absence.
        store
            .put("exp", Value::utf8("v"), None, Some(0.01))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let result = store
            .compare_and_swap(
                "exp",
                Some(&Revision::from_bytes([1; 16])),
                Value::utf8("x"),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(conflict_current(result), None);
    }

    #[tokio::test]
    async fn test_compare_and_delete_success() {
        let store = MemoryStore::new();
        store.put("k", Value::utf8("v"), None, None).await.unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();

        let result = store
            .compare_and_delete("k", &observed.revision, None)
            .await
            .unwrap();
        assert_eq!(result, CompareAndDeleteResult::Deleted);
        assert_eq!(store.get("k", None).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_compare_and_delete_stale_conflict() {
        let store = MemoryStore::new();
        store.put("k", Value::utf8("v"), None, None).await.unwrap();
        let first = store.get_with_revision("k", None).await.unwrap().unwrap();
        store.put("k", Value::utf8("v2"), None, None).await.unwrap();

        let result = store
            .compare_and_delete("k", &first.revision, None)
            .await
            .unwrap();
        match result {
            CompareAndDeleteResult::Conflict { current: Some(rev) } => {
                assert_eq!(rev.value, Value::utf8("v2"))
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_compare_and_delete_missing_conflict() {
        let store = MemoryStore::new();
        let result = store
            .compare_and_delete("missing", &Revision::from_bytes([1; 16]), None)
            .await
            .unwrap();
        assert_eq!(result, CompareAndDeleteResult::Conflict { current: None });
    }

    #[tokio::test]
    async fn test_compare_and_delete_expired_treated_as_absent() {
        let store = MemoryStore::new();
        store
            .put("k", Value::utf8("v"), None, Some(0.01))
            .await
            .unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let result = store
            .compare_and_delete("k", &observed.revision, None)
            .await
            .unwrap();
        assert_eq!(result, CompareAndDeleteResult::Conflict { current: None });
        // The expired occupied entry must have been removed by the atomic operation.
        assert_eq!(store.get("k", None).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_delete_recreate_rejects_stale_revision() {
        let store = MemoryStore::new();
        store.put("k", Value::utf8("v1"), None, None).await.unwrap();
        let first = store.get_with_revision("k", None).await.unwrap().unwrap();
        store.delete("k", None).await.unwrap();
        store.put("k", Value::utf8("v2"), None, None).await.unwrap();

        // The stale pre-delete revision must not match the recreated entry.
        let result = store
            .compare_and_swap("k", Some(&first.revision), Value::utf8("v3"), None, None)
            .await
            .unwrap();
        match result {
            CompareAndSwapResult::Conflict { current: Some(rev) } => {
                assert_eq!(rev.value, Value::utf8("v2"))
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_ordinary_put_invalidates_observed_revision() {
        let store = MemoryStore::new();
        store.put("k", Value::utf8("v1"), None, None).await.unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();
        store.put("k", Value::utf8("v2"), None, None).await.unwrap();

        let result = store
            .compare_and_swap("k", Some(&observed.revision), Value::utf8("v3"), None, None)
            .await
            .unwrap();
        assert!(matches!(result, CompareAndSwapResult::Conflict { .. }));
    }

    #[tokio::test]
    async fn test_ordinary_put_many_invalidates_observed_revision() {
        let store = MemoryStore::new();
        store.put("k", Value::utf8("v1"), None, None).await.unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();
        store
            .put_many(&["k".to_string()], &[Value::utf8("v2")], None, None)
            .await
            .unwrap();

        let result = store
            .compare_and_swap("k", Some(&observed.revision), Value::utf8("v3"), None, None)
            .await
            .unwrap();
        assert!(matches!(result, CompareAndSwapResult::Conflict { .. }));
    }

    #[tokio::test]
    async fn test_concurrent_cas_exactly_one_success() {
        let store = Arc::new(MemoryStore::new());
        store
            .put("k", Value::utf8("seed"), None, None)
            .await
            .unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();

        let mut handles = Vec::new();
        for i in 0..8u8 {
            let store = store.clone();
            let revision = observed.revision;
            handles.push(tokio::spawn(async move {
                store
                    .compare_and_swap("k", Some(&revision), Value::integer(i as i64), None, None)
                    .await
                    .unwrap()
            }));
        }
        let mut applied = 0usize;
        for handle in handles {
            if matches!(handle.await.unwrap(), CompareAndSwapResult::Applied { .. }) {
                applied += 1;
            }
        }
        assert_eq!(applied, 1);
    }

    #[tokio::test]
    async fn test_concurrent_create_if_absent_exactly_one_success() {
        let store = Arc::new(MemoryStore::new());
        let mut handles = Vec::new();
        for i in 0..8u8 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                store
                    .compare_and_swap("k", None, Value::integer(i as i64), None, None)
                    .await
                    .unwrap()
            }));
        }
        let mut applied = 0usize;
        for handle in handles {
            if matches!(handle.await.unwrap(), CompareAndSwapResult::Applied { .. }) {
                applied += 1;
            }
        }
        assert_eq!(applied, 1);
    }

    #[tokio::test]
    async fn test_change_feed_only_records_successful_mutations() {
        let store = MemoryStore::new();
        let mut changes = store.subscribe(ChangeFeedRequest::default()).await.unwrap();

        store.put("k", Value::utf8("v"), None, None).await.unwrap();
        let observed = store.get_with_revision("k", None).await.unwrap().unwrap();

        // A conflicting CAS must not emit a change event.
        store
            .compare_and_swap(
                "k",
                Some(&Revision::from_bytes([9; 16])),
                Value::utf8("x"),
                None,
                None,
            )
            .await
            .unwrap();

        let first = tokio::time::timeout(std::time::Duration::from_millis(100), changes.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.key, "k");
        assert_eq!(first.operation, ChangeOperation::Put);

        // No second event should arrive for the conflict.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), changes.recv())
                .await
                .is_err()
        );

        // A successful CAS emits exactly one event.
        store
            .compare_and_swap("k", Some(&observed.revision), Value::utf8("v2"), None, None)
            .await
            .unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_millis(100), changes.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second.key, "k");
        assert_eq!(second.operation, ChangeOperation::Put);
    }
}
