use crate::change::{ChangeFeedRequest, ChangeOperation, ChangeSubscription, StoreChange};
use crate::error::Result;
use crate::protocol::{
    AsyncChangeFeed, AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::utils::compound::Subspace;

/// Backend capability for rewriting legacy flat identities into a keyspace.
#[async_trait::async_trait]
pub trait AsyncKeyspaceMigration: Send + Sync {
    async fn migrate_into_keyspace(
        &self,
        keyspace: &Subspace,
        options: &MigrationOptions,
    ) -> Result<MigrationReport>;
}

/// Rewrite legacy flat identities into `keyspace` in place.
#[cfg(any(feature = "redis", feature = "valkey"))]
pub async fn migrate_into_keyspace<S>(
    store: &S,
    keyspace: &Subspace,
    options: &MigrationOptions,
) -> Result<MigrationReport>
where
    S: AsyncKeyspaceMigration + ?Sized,
{
    store.migrate_into_keyspace(keyspace, options).await
}

/// Options for copying the current contents of one store into another.
#[derive(Clone, Debug)]
pub struct MigrationOptions {
    /// Empty means all collections.
    pub collections: Vec<String>,
    /// Optional `(source, target)` collection prefix mapping. Collections outside
    /// the source prefix are skipped.
    pub collection_prefix: Option<(String, String)>,
    pub batch_size: usize,
    pub preserve_ttl: bool,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            collections: Vec::new(),
            collection_prefix: None,
            batch_size: 1_000,
            preserve_ttl: true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub scanned: u64,
    pub copied: u64,
    pub skipped_expired: u64,
    pub replayed: u64,
}

impl MigrationReport {
    fn merge(&mut self, other: Self) {
        self.scanned += other.scanned;
        self.copied += other.copied;
        self.skipped_expired += other.skipped_expired;
        self.replayed += other.replayed;
    }
}

pub(crate) fn selected(options: &MigrationOptions, collection: &str) -> bool {
    options.collections.is_empty()
        || options
            .collections
            .iter()
            .any(|candidate| candidate == collection)
}

pub(crate) fn target_collection(options: &MigrationOptions, collection: &str) -> Option<String> {
    match &options.collection_prefix {
        Some((source, target)) => collection
            .strip_prefix(source)
            .map(|suffix| format!("{target}{suffix}")),
        None => Some(collection.to_string()),
    }
}

fn batch_size(options: &MigrationOptions) -> usize {
    options.batch_size.max(1)
}

/// Copy the current contents of selected collections, preserving each entry's TTL.
///
/// This is a snapshot primitive, not a cutover operation. Callers that need live
/// migration should subscribe to the source ChangeFeed before calling this function,
/// then apply the changes received after the copy completes.
pub async fn copy_snapshot<S, T>(
    source: &S,
    target: &T,
    options: &MigrationOptions,
) -> Result<MigrationReport>
where
    S: AsyncKeyValue + AsyncEnumerateCollections + AsyncEnumerateKeys + Send + Sync,
    T: AsyncKeyValue + Send + Sync,
{
    let mut report = MigrationReport::default();
    for collection in source.collections(None).await? {
        if !selected(options, &collection) {
            continue;
        }
        let Some(target_collection) = target_collection(options, &collection) else {
            continue;
        };
        let keys = source.keys(Some(&collection), None).await?;
        for batch in keys.chunks(batch_size(options)) {
            let batch_keys = batch.to_vec();
            report.scanned += batch_keys.len() as u64;
            if options.preserve_ttl {
                for (key, item) in batch_keys
                    .iter()
                    .zip(source.ttl_many(&batch_keys, Some(&collection)).await?)
                {
                    let Some((value, ttl)) = item else {
                        report.skipped_expired += 1;
                        continue;
                    };
                    target
                        .put(key, value, Some(&target_collection), ttl)
                        .await?;
                    report.copied += 1;
                }
            } else {
                let values = source.get_many(&batch_keys, Some(&collection)).await?;
                let mut keys_to_copy = Vec::with_capacity(batch_keys.len());
                let mut values_to_copy = Vec::with_capacity(batch_keys.len());
                for (key, value) in batch_keys.iter().zip(values) {
                    let Some(value) = value else {
                        report.skipped_expired += 1;
                        continue;
                    };
                    keys_to_copy.push(key.clone());
                    values_to_copy.push(value);
                }
                if !keys_to_copy.is_empty() {
                    target
                        .put_many(
                            &keys_to_copy,
                            &values_to_copy,
                            Some(&target_collection),
                            None,
                        )
                        .await?;
                    report.copied += keys_to_copy.len() as u64;
                }
            }
        }
    }
    Ok(report)
}

/// Open a live feed before copying the snapshot, so changes committed during the
/// copy remain available for replay after this function returns.
pub async fn copy_snapshot_with_feed<S, T>(
    source: &S,
    target: &T,
    options: &MigrationOptions,
) -> Result<(MigrationReport, ChangeSubscription)>
where
    S: AsyncChangeFeed
        + AsyncKeyValue
        + AsyncEnumerateCollections
        + AsyncEnumerateKeys
        + Send
        + Sync,
    T: AsyncKeyValue + Send + Sync,
{
    let subscription = source.subscribe(ChangeFeedRequest::default()).await?;
    let report = copy_snapshot(source, target, options).await?;
    Ok((report, subscription))
}

/// Apply one source change to the target using the source's current value and TTL.
pub async fn apply_change<S, T>(
    source: &S,
    target: &T,
    change: &StoreChange,
    options: &MigrationOptions,
) -> Result<MigrationReport>
where
    S: AsyncKeyValue + Send + Sync,
    T: AsyncKeyValue + Send + Sync,
{
    let mut report = MigrationReport {
        replayed: 1,
        ..MigrationReport::default()
    };
    if !selected(options, &change.collection) {
        return Ok(MigrationReport::default());
    }
    let Some(target_collection) = target_collection(options, &change.collection) else {
        return Ok(MigrationReport::default());
    };
    match change.operation {
        ChangeOperation::Delete => {
            target.delete(&change.key, Some(&target_collection)).await?;
        }
        ChangeOperation::Put => {
            let value = if options.preserve_ttl {
                source.ttl(&change.key, Some(&change.collection)).await?
            } else {
                source
                    .get(&change.key, Some(&change.collection))
                    .await?
                    .map(|value| (value, None))
            };
            match value {
                Some((value, ttl)) => {
                    target
                        .put(&change.key, value, Some(&target_collection), ttl)
                        .await?;
                }
                None => {
                    target.delete(&change.key, Some(&target_collection)).await?;
                    report.skipped_expired = 1;
                }
            }
        }
    }
    Ok(report)
}

/// Merge a replay report into an existing migration report.
pub fn merge_report(report: &mut MigrationReport, replay: MigrationReport) {
    report.merge(replay);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::{ChangeCursor, StoreChange};
    use crate::protocol::AsyncKeyValue;
    use crate::store::memory::MemoryStore;
    use crate::value::Value;
    use chrono::Utc;

    #[tokio::test]
    async fn snapshot_copy_preserves_values_and_ttl() {
        let source = MemoryStore::new();
        let target = MemoryStore::new();
        source
            .put("permanent", Value::utf8("value"), Some("services"), None)
            .await
            .unwrap();
        source
            .put(
                "temporary",
                Value::utf8("ttl"),
                Some("services"),
                Some(60.0),
            )
            .await
            .unwrap();

        let report = copy_snapshot(&source, &target, &MigrationOptions::default())
            .await
            .unwrap();

        assert_eq!(report.scanned, 2);
        assert_eq!(report.copied, 2);
        assert_eq!(
            target.get("permanent", Some("services")).await.unwrap(),
            Some(Value::utf8("value"))
        );
        assert!(
            target
                .ttl("temporary", Some("services"))
                .await
                .unwrap()
                .unwrap()
                .1
                .is_some()
        );
    }

    #[tokio::test]
    async fn snapshot_copy_normalizes_zero_batch_size() {
        let source = MemoryStore::new();
        let target = MemoryStore::new();
        source
            .put("key", Value::utf8("value"), Some("services"), None)
            .await
            .unwrap();

        let options = MigrationOptions {
            batch_size: 0,
            ..MigrationOptions::default()
        };
        let report = copy_snapshot(&source, &target, &options).await.unwrap();

        assert_eq!(report.copied, 1);
        assert_eq!(
            target.get("key", Some("services")).await.unwrap(),
            Some(Value::utf8("value"))
        );
    }

    #[tokio::test]
    async fn snapshot_copy_without_ttl_uses_batch_put() {
        let source = MemoryStore::new();
        let target = MemoryStore::new();
        source
            .put_many(
                &["one".to_string(), "two".to_string()],
                &[Value::utf8("1"), Value::utf8("2")],
                Some("services"),
                Some(60.0),
            )
            .await
            .unwrap();

        let options = MigrationOptions {
            preserve_ttl: false,
            batch_size: 2,
            ..MigrationOptions::default()
        };
        let report = copy_snapshot(&source, &target, &options).await.unwrap();

        assert_eq!(report.copied, 2);
        assert_eq!(
            target.get("one", Some("services")).await.unwrap(),
            Some(Value::utf8("1"))
        );
        assert_eq!(
            target.ttl("one", Some("services")).await.unwrap(),
            Some((Value::utf8("1"), None))
        );
    }

    #[tokio::test]
    async fn apply_change_replays_put_and_delete() {
        let source = MemoryStore::new();
        let target = MemoryStore::new();
        source
            .put("key", Value::utf8("new"), Some("services"), None)
            .await
            .unwrap();
        let put = StoreChange {
            cursor: ChangeCursor::new("1-0"),
            revision: 1,
            collection: "services".to_string(),
            key: "key".to_string(),
            operation: ChangeOperation::Put,
            occurred_at: Utc::now(),
        };
        apply_change(&source, &target, &put, &MigrationOptions::default())
            .await
            .unwrap();
        assert_eq!(
            target.get("key", Some("services")).await.unwrap(),
            Some(Value::utf8("new"))
        );

        let delete = StoreChange {
            operation: ChangeOperation::Delete,
            ..put
        };
        apply_change(&source, &target, &delete, &MigrationOptions::default())
            .await
            .unwrap();
        assert_eq!(target.get("key", Some("services")).await.unwrap(), None);
    }

    #[tokio::test]
    async fn snapshot_feed_captures_changes_after_subscription() {
        let source = MemoryStore::new();
        let target = MemoryStore::new();
        let (report, mut feed) =
            copy_snapshot_with_feed(&source, &target, &MigrationOptions::default())
                .await
                .unwrap();
        assert_eq!(report.copied, 0);

        source
            .put("live", Value::utf8("value"), Some("services"), None)
            .await
            .unwrap();
        let change = feed.recv().await.unwrap().unwrap();
        apply_change(&source, &target, &change, &MigrationOptions::default())
            .await
            .unwrap();

        assert_eq!(
            target.get("live", Some("services")).await.unwrap(),
            Some(Value::utf8("value"))
        );
    }

    #[tokio::test]
    async fn snapshot_copy_maps_collection_prefix() {
        let source = MemoryStore::new();
        let target = MemoryStore::new();
        source
            .put("key", Value::utf8("value"), Some("old:services"), None)
            .await
            .unwrap();
        source
            .put(
                "ignored",
                Value::utf8("value"),
                Some("other:services"),
                None,
            )
            .await
            .unwrap();
        let options = MigrationOptions {
            collection_prefix: Some(("old:".to_string(), "new:".to_string())),
            ..MigrationOptions::default()
        };

        let report = copy_snapshot(&source, &target, &options).await.unwrap();

        assert_eq!(report.copied, 1);
        assert_eq!(
            target.get("key", Some("new:services")).await.unwrap(),
            Some(Value::utf8("value"))
        );
        assert_eq!(
            target.get("ignored", Some("other:services")).await.unwrap(),
            None
        );
    }
}
