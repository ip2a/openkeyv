use super::client::ValkeyClient;
use super::config::ValkeyConfig;
use super::error::{Error, Result, map_valkey_err};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::utils::compound::{collection_prefix, compound_key, decompound_key};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::AsyncCommands;

const SCAN_COUNT: usize = 1_000;

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

/// Valkey-backed key-value store.
///
/// Each collection is represented by a key prefix in Valkey.
/// Values are stored as `OKVE1`-encoded `ManagedEntry` bytes.
pub struct ValkeyStore {
    client: ValkeyClient,
    config: ValkeyConfig,
}

impl ValkeyStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(map_valkey_err)?;
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(map_valkey_err)?;
        Ok(Self::with_config(conn, ValkeyConfig::default()))
    }

    pub async fn from_client(client: redis::Client) -> Result<Self> {
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(map_valkey_err)?;
        Ok(Self::with_config(conn, ValkeyConfig::default()))
    }

    pub fn with_config(conn: redis::aio::MultiplexedConnection, config: ValkeyConfig) -> Self {
        Self {
            client: ValkeyClient::new(conn),
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
impl AsyncKeyValue for ValkeyStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let mut conn = self.connection();
        let res: Option<Vec<u8>> = conn.get(&ck).await.map_err(map_valkey_err)?;
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
        let res: Option<Vec<u8>> = conn.get(&ck).await.map_err(map_valkey_err)?;
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
        let mut conn = self.connection();
        match ttl {
            Some(seconds) => {
                let milliseconds = (seconds * 1000.0) as u64;
                let _: () = conn
                    .pset_ex(ck, entry.encode(), milliseconds)
                    .await
                    .map_err(map_valkey_err)?;
            }
            None => {
                let _: () = conn.set(ck, entry.encode()).await.map_err(map_valkey_err)?;
            }
        }
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let mut conn = self.connection();
        let res: i64 = conn.del(&ck).await.map_err(map_valkey_err)?;
        Ok(res > 0)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        let cks: Vec<String> = keys.iter().map(|k| compound_key(cname, k)).collect();
        let mut conn = self.connection();
        let res: Vec<Option<Vec<u8>>> = conn.mget(&cks).await.map_err(map_valkey_err)?;
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
        let res: Vec<Option<Vec<u8>>> = conn.mget(&cks).await.map_err(map_valkey_err)?;
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
        let mut conn = self.connection();
        let mut pipe = redis::pipe();

        if let Some(seconds) = ttl {
            let milliseconds = (seconds * 1000.0) as u64;
            for (key, value) in keys.iter().zip(values.iter()) {
                let ck = compound_key(cname, key);
                let entry = ManagedEntry::with_ttl(value.clone(), seconds)?;
                pipe.pset_ex(ck, entry.encode(), milliseconds).ignore();
            }
        } else {
            for (key, value) in keys.iter().zip(values.iter()) {
                let ck = compound_key(cname, key);
                let entry = ManagedEntry::new(value.clone());
                pipe.set(ck, entry.encode()).ignore();
            }
        }
        let _: () = pipe.query_async(&mut conn).await.map_err(map_valkey_err)?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let cks: Vec<String> = keys.iter().map(|k| compound_key(cname, k)).collect();
        let mut conn = self.connection();
        let res: i64 = conn.del(&cks).await.map_err(map_valkey_err)?;
        Ok(res as usize)
    }
}

#[async_trait]
impl AsyncCull for ValkeyStore {
    async fn cull(&self) -> Result<()> {
        // Valkey handles TTL natively; no manual culling needed.
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for ValkeyStore {
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
                .map_err(map_valkey_err)?;

            for identity in batch {
                let (key_collection, key) = decompound_key(&identity)?;
                if key_collection != cname {
                    return Err(Error::InvalidKey(format!(
                        "Valkey SCAN returned an identity outside collection {cname:?}"
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
impl AsyncEnumerateCollections for ValkeyStore {
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
                .map_err(map_valkey_err)?;

            for identity in keys {
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
impl AsyncDestroyCollection for ValkeyStore {
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
                .map_err(map_valkey_err)?;

            let mut matching = Vec::with_capacity(keys.len());
            for identity in keys {
                let (key_collection, _) = decompound_key(&identity)?;
                if key_collection != collection {
                    return Err(Error::InvalidKey(format!(
                        "Valkey SCAN returned an identity outside collection {collection:?}"
                    )));
                }
                matching.push(identity);
            }
            if !matching.is_empty() {
                let _: () = conn.del(&matching).await.map_err(map_valkey_err)?;
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
impl AsyncDestroyStore for ValkeyStore {
    async fn destroy(&self) -> Result<bool> {
        let mut conn = self.connection();
        let _: () = redis::cmd("FLUSHDB")
            .query_async(&mut conn)
            .await
            .map_err(map_valkey_err)?;
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

    #[tokio::test]
    #[ignore = "requires OPENKEYV_VALKEY_URL"]
    async fn test_valkey_store_uses_binary_entries_and_native_ttl() {
        let url = std::env::var("OPENKEYV_VALKEY_URL").unwrap();
        let store = ValkeyStore::new(&url).await.unwrap();
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
    #[ignore = "requires OPENKEYV_VALKEY_URL"]
    async fn test_valkey_store_scans_and_deletes_multiple_pages() {
        let url = std::env::var("OPENKEYV_VALKEY_URL").unwrap();
        let store = ValkeyStore::new(&url).await.unwrap();
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
    #[ignore = "requires OPENKEYV_VALKEY_URL"]
    async fn test_valkey_identity_is_collision_free_and_glob_safe() {
        let url = std::env::var("OPENKEYV_VALKEY_URL").unwrap();
        let store = ValkeyStore::new(&url).await.unwrap();
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
    #[ignore = "requires OPENKEYV_VALKEY_URL"]
    async fn test_valkey_store_rejects_json_entry_payload() {
        let url = std::env::var("OPENKEYV_VALKEY_URL").unwrap();
        let store = ValkeyStore::new(&url).await.unwrap();
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
