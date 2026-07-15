use super::client::RedisClient;
use super::config::RedisConfig;
use super::error::{Error, Result, map_redis_err};
use crate::change::{
    ChangeFeedRequest, ChangeFilter, ChangeOperation, ChangeStart, ChangeStream,
    ChangeSubscription, StoreChange,
};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncChangeFeed, AsyncCull, AsyncDestroyCollection, AsyncDestroyStore,
    AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::utils::compound::{collection_prefix, compound_key, decompound_key};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::streams::{StreamId, StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Script};

const SCAN_COUNT: usize = 1_000;
const CHANGE_RETENTION: usize = 10_000;
const CHANGE_STREAM_KEY: &str = "__openkeyv_changefeed_stream";
const CHANGE_REVISION_KEY: &str = "__openkeyv_changefeed_revision";

const PUT_CHANGE_SCRIPT: &str = r#"
local revision = redis.call('INCR', KEYS[2])
local id = tostring(revision) .. '-0'
if ARGV[2] == '0' then
    redis.call('SET', KEYS[1], ARGV[1])
else
    redis.call('PSETEX', KEYS[1], ARGV[2], ARGV[1])
end
redis.call('XADD', KEYS[3], 'MAXLEN', '=', ARGV[6], id,
    'revision', tostring(revision),
    'collection', ARGV[3],
    'key', ARGV[4],
    'operation', 'put',
    'occurred_at', ARGV[5])
return id
"#;

const DELETE_CHANGE_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then
    return false
end
redis.call('DEL', KEYS[1])
local revision = redis.call('INCR', KEYS[2])
local id = tostring(revision) .. '-0'
redis.call('XADD', KEYS[3], 'MAXLEN', '=', ARGV[3], id,
    'revision', tostring(revision),
    'collection', ARGV[1],
    'key', ARGV[2],
    'operation', 'delete',
    'occurred_at', ARGV[4])
return id
"#;

fn is_internal_key(key: &str) -> bool {
    key == CHANGE_STREAM_KEY || key == CHANGE_REVISION_KEY
}

fn cursor_revision(cursor: &str) -> Result<u64> {
    let (revision, sequence) = cursor
        .split_once('-')
        .ok_or_else(|| crate::error::Error::InvalidChangeCursor(cursor.to_string()))?;
    if sequence != "0" {
        return Err(crate::error::Error::InvalidChangeCursor(cursor.to_string()));
    }
    revision
        .parse::<u64>()
        .map_err(|_| crate::error::Error::InvalidChangeCursor(cursor.to_string()))
}

fn stream_change(entry: &StreamId) -> Result<StoreChange> {
    let revision = entry
        .get::<u64>("revision")
        .ok_or(crate::error::Error::CorruptedData)?;
    let collection = entry
        .get::<String>("collection")
        .ok_or(crate::error::Error::CorruptedData)?;
    let key = entry
        .get::<String>("key")
        .ok_or(crate::error::Error::CorruptedData)?;
    let operation = match entry.get::<String>("operation").as_deref() {
        Some("put") => ChangeOperation::Put,
        Some("delete") => ChangeOperation::Delete,
        _ => return Err(crate::error::Error::CorruptedData),
    };
    let occurred_at = entry
        .get::<String>("occurred_at")
        .and_then(|value| value.parse::<i64>().ok())
        .and_then(chrono::DateTime::from_timestamp_millis)
        .ok_or(crate::error::Error::CorruptedData)?;

    Ok(StoreChange {
        cursor: crate::change::ChangeCursor::new(entry.id.clone()),
        revision,
        collection,
        key,
        operation,
        occurred_at,
    })
}

struct RedisChangeStream {
    connection: redis::aio::MultiplexedConnection,
    cursor: String,
    last_revision: Option<u64>,
    filter: ChangeFilter,
}

#[async_trait]
impl ChangeStream for RedisChangeStream {
    async fn recv(&mut self) -> Result<Option<StoreChange>> {
        loop {
            let options = StreamReadOptions::default().count(128).block(1_000);
            let reply: StreamReadReply = self
                .connection
                .xread_options(&[CHANGE_STREAM_KEY], &[self.cursor.as_str()], &options)
                .await
                .map_err(map_redis_err)?;

            for stream in reply.keys {
                for entry in stream.ids {
                    let previous_cursor = self.cursor.clone();
                    let change = stream_change(&entry)?;
                    if let Some(last_revision) = self.last_revision {
                        if change.revision > last_revision.saturating_add(1) {
                            return Err(crate::error::Error::ChangeCursorExpired {
                                requested: previous_cursor,
                                oldest: entry.id.clone(),
                            });
                        }
                    }
                    self.cursor = entry.id.clone();
                    self.last_revision = Some(change.revision);
                    if self.filter.matches(&change) {
                        return Ok(Some(change));
                    }
                }
            }
        }
    }
}

async fn latest_cursor(connection: &mut redis::aio::MultiplexedConnection) -> Result<String> {
    let reply: StreamRangeReply = connection
        .xrevrange_count(CHANGE_STREAM_KEY, "+", "-", 1)
        .await
        .map_err(map_redis_err)?;
    Ok(reply
        .ids
        .first()
        .map(|entry| entry.id.clone())
        .unwrap_or_else(|| "0-0".to_string()))
}

async fn validate_after_cursor(
    connection: &mut redis::aio::MultiplexedConnection,
    cursor: &str,
) -> Result<()> {
    let requested = cursor_revision(cursor)?;
    let last: StreamRangeReply = connection
        .xrevrange_count(CHANGE_STREAM_KEY, "+", "-", 1)
        .await
        .map_err(map_redis_err)?;
    let Some(last_entry) = last.ids.first() else {
        return if requested == 0 {
            Ok(())
        } else {
            Err(crate::error::Error::InvalidChangeCursor(cursor.to_string()))
        };
    };
    let last_revision = cursor_revision(&last_entry.id)?;
    if requested > last_revision {
        return Err(crate::error::Error::InvalidChangeCursor(cursor.to_string()));
    }

    let first: StreamRangeReply = connection
        .xrange_count(CHANGE_STREAM_KEY, "-", "+", 1)
        .await
        .map_err(map_redis_err)?;
    if let Some(first_entry) = first.ids.first() {
        let first_revision = cursor_revision(&first_entry.id)?;
        if first_revision > requested.saturating_add(1) {
            return Err(crate::error::Error::ChangeCursorExpired {
                requested: cursor.to_string(),
                oldest: first_entry.id.clone(),
            });
        }
    }
    Ok(())
}

fn collection_scan_pattern(collection: &str) -> String {
    let prefix = collection_prefix(collection);
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for character in prefix.chars() {
        if matches!(character, '*' | '?' | '[' | ']' | '\\') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('*');
    pattern
}

/// Redis-backed key-value store.
///
/// Each collection is represented by a key prefix in Redis.
/// Values are stored as `OKVE1`-encoded `ManagedEntry` bytes.
pub struct RedisStore {
    client: RedisClient,
    config: RedisConfig,
}

impl RedisStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(map_redis_err)?;
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(map_redis_err)?;
        Ok(Self {
            client: RedisClient::with_client(conn, client),
            config: RedisConfig::default(),
        })
    }

    pub async fn from_client(client: redis::Client) -> Result<Self> {
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(map_redis_err)?;
        Ok(Self {
            client: RedisClient::with_client(conn, client),
            config: RedisConfig::default(),
        })
    }

    pub fn with_config(conn: redis::aio::MultiplexedConnection, config: RedisConfig) -> Self {
        Self {
            client: RedisClient::new(conn),
            config,
        }
    }

    fn connection(&self) -> redis::aio::MultiplexedConnection {
        self.client.connection()
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }
}

#[async_trait]
impl AsyncChangeFeed for RedisStore {
    async fn subscribe(&self, request: ChangeFeedRequest) -> Result<ChangeSubscription> {
        let mut connection = self.client.subscription_connection().await?;
        let start = request.start;
        let filter = request.filter;
        let cursor = match &start {
            ChangeStart::Beginning => "0-0".to_string(),
            ChangeStart::Latest => latest_cursor(&mut connection).await?,
            ChangeStart::After(cursor) => {
                validate_after_cursor(&mut connection, cursor.as_str()).await?;
                cursor.to_string()
            }
        };
        let last_revision = match &start {
            ChangeStart::Beginning => None,
            ChangeStart::Latest => Some(cursor_revision(&cursor)?),
            ChangeStart::After(cursor) => Some(cursor_revision(cursor.as_str())?),
        };
        Ok(ChangeSubscription::new(RedisChangeStream {
            connection,
            cursor,
            last_revision,
            filter,
        }))
    }
}

#[async_trait]
impl AsyncKeyValue for RedisStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let mut conn = self.connection();
        let res: Option<Vec<u8>> = conn.get(&ck).await.map_err(map_redis_err)?;
        match res {
            Some(bytes) => {
                let entry = ManagedEntry::decode(Bytes::from(bytes))?;
                if entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry.value))
                }
            }
            None => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let mut conn = self.connection();
        let res: Option<Vec<u8>> = conn.get(&ck).await.map_err(map_redis_err)?;
        match res {
            Some(bytes) => {
                let entry = ManagedEntry::decode(Bytes::from(bytes))?;
                if entry.is_expired() {
                    Ok(None)
                } else {
                    let ttl = entry.ttl();
                    Ok(Some((entry.value, ttl)))
                }
            }
            None => Ok(None),
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
        let ck = compound_key(cname, key);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        let milliseconds = ttl
            .map(|seconds| ((seconds * 1000.0).ceil() as u64).to_string())
            .unwrap_or_else(|| "0".to_string());
        let occurred_at = chrono::Utc::now().timestamp_millis().to_string();
        let mut conn = self.connection();
        let _: String = Script::new(PUT_CHANGE_SCRIPT)
            .key(ck)
            .key(CHANGE_REVISION_KEY)
            .key(CHANGE_STREAM_KEY)
            .arg(entry.encode())
            .arg(milliseconds)
            .arg(cname)
            .arg(key)
            .arg(occurred_at)
            .arg(CHANGE_RETENTION)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let occurred_at = chrono::Utc::now().timestamp_millis().to_string();
        let mut conn = self.connection();
        let result: Option<String> = Script::new(DELETE_CHANGE_SCRIPT)
            .key(ck)
            .key(CHANGE_REVISION_KEY)
            .key(CHANGE_STREAM_KEY)
            .arg(cname)
            .arg(key)
            .arg(CHANGE_RETENTION)
            .arg(occurred_at)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(result.is_some())
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        let cks: Vec<String> = keys.iter().map(|k| compound_key(cname, k)).collect();
        let mut conn = self.connection();
        let res: Vec<Option<Vec<u8>>> = conn.mget(&cks).await.map_err(map_redis_err)?;
        res.into_iter()
            .map(|opt| match opt {
                Some(bytes) => {
                    let entry = ManagedEntry::decode(Bytes::from(bytes))?;
                    if entry.is_expired() {
                        Ok(None)
                    } else {
                        Ok(Some(entry.value))
                    }
                }
                None => Ok(None),
            })
            .collect()
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let cname = self.collection_name(collection);
        let cks: Vec<String> = keys.iter().map(|k| compound_key(cname, k)).collect();
        let mut conn = self.connection();
        let res: Vec<Option<Vec<u8>>> = conn.mget(&cks).await.map_err(map_redis_err)?;
        res.into_iter()
            .map(|opt| match opt {
                Some(bytes) => {
                    let entry = ManagedEntry::decode(Bytes::from(bytes))?;
                    if entry.is_expired() {
                        Ok(None)
                    } else {
                        let ttl = entry.ttl();
                        Ok(Some((entry.value, ttl)))
                    }
                }
                None => Ok(None),
            })
            .collect()
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
        for (key, value) in keys.iter().zip(values.iter()) {
            self.put(key, value.clone(), Some(cname), ttl).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let mut count = 0;
        for key in keys {
            if self.delete(key, Some(cname)).await? {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for RedisStore {
    async fn cull(&self) -> Result<()> {
        // Redis handles TTL natively; no manual culling needed.
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for RedisStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let cname = self.collection_name(collection);
        let pattern = collection_scan_pattern(cname);
        let mut conn = self.connection();
        let mut cursor = 0_u64;
        let mut keys = std::collections::HashSet::new();

        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;

            for identity in batch {
                let (key_collection, key) = decompound_key(&identity)?;
                if key_collection != cname {
                    return Err(Error::InvalidKey(format!(
                        "Redis SCAN returned an identity outside collection {cname:?}"
                    )));
                }
                keys.insert(key.to_string());
                if keys.len() == limit {
                    return Ok(keys.into_iter().collect());
                }
            }

            cursor = next_cursor;
            if cursor == 0 {
                return Ok(keys.into_iter().collect());
            }
        }
    }
}

#[async_trait]
impl AsyncEnumerateCollections for RedisStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.connection();
        let mut cursor = 0_u64;
        let mut collections = std::collections::HashSet::new();

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("*")
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;

            for identity in keys {
                if is_internal_key(&identity) {
                    continue;
                }
                let (collection, _) = decompound_key(&identity)?;
                collections.insert(collection.to_string());
                if collections.len() == limit {
                    return Ok(collections.into_iter().collect());
                }
            }

            cursor = next_cursor;
            if cursor == 0 {
                return Ok(collections.into_iter().collect());
            }
        }
    }
}

#[async_trait]
impl AsyncDestroyCollection for RedisStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let pattern = collection_scan_pattern(collection);
        let mut conn = self.connection();
        let mut cursor = 0_u64;
        let mut destroyed = false;

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;

            let mut matching = Vec::with_capacity(keys.len());
            for identity in keys {
                let (key_collection, _) = decompound_key(&identity)?;
                if key_collection != collection {
                    return Err(Error::InvalidKey(format!(
                        "Redis SCAN returned an identity outside collection {collection:?}"
                    )));
                }
                matching.push(identity);
            }
            if !matching.is_empty() {
                let _: () = conn.del(&matching).await.map_err(map_redis_err)?;
                destroyed = true;
            }

            cursor = next_cursor;
            if cursor == 0 {
                return Ok(destroyed);
            }
        }
    }
}

#[async_trait]
impl AsyncDestroyStore for RedisStore {
    async fn destroy(&self) -> Result<bool> {
        let mut conn = self.connection();
        let _: () = redis::cmd("FLUSHDB")
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_pattern_escapes_collection_glob_characters() {
        assert_eq!(collection_scan_pattern("*?[\\]"), r"5:\*\?\[\\\]*");
    }

    #[test]
    fn redis_change_cursors_require_stream_ids_with_zero_sequence() {
        assert_eq!(cursor_revision("42-0").unwrap(), 42);
        assert!(matches!(
            cursor_revision("42"),
            Err(Error::InvalidChangeCursor(_))
        ));
        assert!(matches!(
            cursor_revision("42-1"),
            Err(Error::InvalidChangeCursor(_))
        ));
        assert!(matches!(
            cursor_revision("not-a-cursor"),
            Err(Error::InvalidChangeCursor(_))
        ));
    }

    #[test]
    fn redis_changefeed_keys_are_not_collections() {
        assert!(is_internal_key(CHANGE_STREAM_KEY));
        assert!(is_internal_key(CHANGE_REVISION_KEY));
        assert!(!is_internal_key("5:events:key"));
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_REDIS_URL"]
    async fn test_redis_change_feed_delivers_and_resumes_across_instances() {
        let url = std::env::var("OPENKEYV_REDIS_URL").unwrap();
        let writer = RedisStore::new(&url).await.unwrap();
        let reader = RedisStore::new(&url).await.unwrap();
        let collection = format!(
            "openkeyv_changefeed_test_{}_{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );

        let mut live = reader
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::Latest,
                filter: ChangeFilter::collection(&collection),
            })
            .await
            .unwrap();

        writer
            .put("event-1", Value::integer(1), Some(&collection), None)
            .await
            .unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), live.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.collection, collection);
        assert_eq!(first.key, "event-1");
        assert_eq!(first.operation, ChangeOperation::Put);
        assert_eq!(
            reader.get("event-1", Some(&collection)).await.unwrap(),
            Some(Value::integer(1))
        );

        writer
            .put("event-2", Value::integer(2), Some(&collection), None)
            .await
            .unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(2), live.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second.key, "event-2");
        assert!(second.revision > first.revision);

        let mut resumed = reader
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::After(first.cursor),
                filter: ChangeFilter::collection(&collection),
            })
            .await
            .unwrap();
        let replayed = tokio::time::timeout(std::time::Duration::from_secs(2), resumed.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replayed.cursor, second.cursor);

        assert!(writer.delete("event-2", Some(&collection)).await.unwrap());
        let deleted = tokio::time::timeout(std::time::Duration::from_secs(2), resumed.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(deleted.key, "event-2");
        assert_eq!(deleted.operation, ChangeOperation::Delete);

        assert!(writer.destroy_collection(&collection).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_REDIS_URL"]
    async fn test_redis_store_uses_binary_entries_and_native_ttl() {
        let url = std::env::var("OPENKEYV_REDIS_URL").unwrap();
        let store = RedisStore::new(&url).await.unwrap();
        let collection = format!("openkeyv_binary_test_{}", std::process::id());
        let _ = store.destroy_collection(&collection).await.unwrap();

        let value = Value::binary(Bytes::from_static(&[0, 255, 1]));
        store
            .put("single", value.clone(), Some(&collection), Some(10.5))
            .await
            .unwrap();
        assert_eq!(
            store.get("single", Some(&collection)).await.unwrap(),
            Some(value)
        );

        let key = compound_key(&collection, "single");
        let mut conn = store.connection();
        let bytes: Vec<u8> = conn.get(&key).await.unwrap();
        assert!(bytes.starts_with(b"OKVE1"));
        let ttl: i64 = redis::cmd("PTTL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(ttl > 10_000 && ttl <= 10_500);

        let keys = vec!["one".to_string(), "two".to_string()];
        let values = vec![Value::integer(1), Value::integer(2)];
        store
            .put_many(&keys, &values, Some(&collection), Some(10.5))
            .await
            .unwrap();
        assert_eq!(
            store.get_many(&keys, Some(&collection)).await.unwrap(),
            vec![Some(values[0].clone()), Some(values[1].clone())]
        );
        for key in keys {
            let key = compound_key(&collection, &key);
            let bytes: Vec<u8> = conn.get(&key).await.unwrap();
            assert!(bytes.starts_with(b"OKVE1"));
            let ttl: i64 = redis::cmd("PTTL")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert!(ttl > 10_000 && ttl <= 10_500);
        }

        store
            .put_many(&[], &[], Some(&collection), Some(10.5))
            .await
            .unwrap();

        assert!(store.destroy_collection(&collection).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_REDIS_URL"]
    async fn test_redis_store_scans_and_deletes_multiple_pages() {
        let url = std::env::var("OPENKEYV_REDIS_URL").unwrap();
        let store = RedisStore::new(&url).await.unwrap();
        let collection = format!("openkeyv_scan_test_{}", std::process::id());
        let _ = store.destroy_collection(&collection).await.unwrap();

        let keys: Vec<String> = (0..1_205).map(|index| format!("key-{index:04}")).collect();
        let values = vec![Value::null(); keys.len()];
        store
            .put_many(&keys, &values, Some(&collection), None)
            .await
            .unwrap();

        let scanned = store.keys(Some(&collection), None).await.unwrap();
        assert_eq!(scanned.len(), keys.len());
        let scanned: std::collections::HashSet<_> = scanned.into_iter().collect();
        assert!(keys.iter().all(|key| scanned.contains(key)));
        assert_eq!(
            store.keys(Some(&collection), Some(17)).await.unwrap().len(),
            17
        );
        assert!(store.collections(None).await.unwrap().contains(&collection));

        assert!(store.destroy_collection(&collection).await.unwrap());
        assert!(
            store
                .keys(Some(&collection), None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!store.destroy_collection(&collection).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_REDIS_URL"]
    async fn test_redis_identity_is_collision_free_and_glob_safe() {
        let url = std::env::var("OPENKEYV_REDIS_URL").unwrap();
        let store = RedisStore::new(&url).await.unwrap();
        let base = format!("openkeyv_identity_*?[\\]_{}", std::process::id());
        let left_collection = format!("{base}:b");
        let right_collection = base;
        let _ = store.destroy_collection(&left_collection).await.unwrap();
        let _ = store.destroy_collection(&right_collection).await.unwrap();

        let left = Value::utf8("left");
        let right = Value::utf8("right");
        store
            .put("c", left.clone(), Some(&left_collection), None)
            .await
            .unwrap();
        store
            .put("b:c", right.clone(), Some(&right_collection), None)
            .await
            .unwrap();

        assert_eq!(
            store.get("c", Some(&left_collection)).await.unwrap(),
            Some(left)
        );
        assert_eq!(
            store.get("b:c", Some(&right_collection)).await.unwrap(),
            Some(right.clone())
        );
        assert_eq!(
            store.keys(Some(&left_collection), None).await.unwrap(),
            vec!["c"]
        );
        assert_eq!(
            store.keys(Some(&right_collection), None).await.unwrap(),
            vec!["b:c"]
        );
        let collections: std::collections::HashSet<_> =
            store.collections(None).await.unwrap().into_iter().collect();
        assert!(collections.contains(&left_collection));
        assert!(collections.contains(&right_collection));

        assert!(store.destroy_collection(&left_collection).await.unwrap());
        assert_eq!(
            store.get("b:c", Some(&right_collection)).await.unwrap(),
            Some(right)
        );
        assert!(store.destroy_collection(&right_collection).await.unwrap());

        let malformed = format!("01:openkeyv-{}", std::process::id());
        let mut conn = store.connection();
        let _: () = conn.set(&malformed, b"invalid").await.unwrap();
        assert!(matches!(
            store.collections(None).await,
            Err(Error::InvalidKey(_))
        ));
        let _: () = conn.del(&malformed).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_REDIS_URL"]
    async fn test_redis_store_rejects_json_entry_payload() {
        let url = std::env::var("OPENKEYV_REDIS_URL").unwrap();
        let store = RedisStore::new(&url).await.unwrap();
        let collection = format!("openkeyv_json_test_{}", std::process::id());
        let _ = store.destroy_collection(&collection).await.unwrap();

        let key = compound_key(&collection, "json-entry");
        let mut conn = store.connection();
        let _: () = conn
            .set(key, br#"{"value":null}"#.as_slice())
            .await
            .unwrap();

        let error = store
            .get("json-entry", Some(&collection))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid OpenKeyV entry magic"));

        assert!(store.destroy_collection(&collection).await.unwrap());
    }
}
