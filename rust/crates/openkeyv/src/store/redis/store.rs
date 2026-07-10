use super::client::RedisClient;
use super::config::RedisConfig;
use super::error::{Error, Result, map_redis_err};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::AsyncCommands;

const COLLECTION_SEPARATOR: &str = ":";

fn compound_key(collection: &str, key: &str) -> String {
    format!("{}{}{}", collection, COLLECTION_SEPARATOR, key)
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
        Ok(Self::with_config(conn, RedisConfig::default()))
    }

    pub async fn from_client(client: redis::Client) -> Result<Self> {
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(map_redis_err)?;
        Ok(Self::with_config(conn, RedisConfig::default()))
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

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
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
                    let ttl = entry.ttl().unwrap_or(0.0);
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
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let mut conn = self.connection();
        match ttl {
            Some(seconds) => {
                let milliseconds = (seconds * 1000.0) as u64;
                let _: () = conn
                    .pset_ex(ck, entry.encode(), milliseconds)
                    .await
                    .map_err(map_redis_err)?;
            }
            None => {
                let _: () = conn.set(ck, entry.encode()).await.map_err(map_redis_err)?;
            }
        }
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let mut conn = self.connection();
        let res: i64 = conn.del(&ck).await.map_err(map_redis_err)?;
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
    ) -> Result<Vec<Option<(Value, f64)>>> {
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
                        let ttl = entry.ttl().unwrap_or(0.0);
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
        let cname = self.collection_name(collection);
        let mut conn = self.connection();
        let mut pipe = redis::pipe();

        if let Some(seconds) = ttl {
            let milliseconds = (seconds * 1000.0) as u64;
            for (key, value) in keys.iter().zip(values.iter()) {
                let ck = compound_key(cname, key);
                let entry = ManagedEntry::with_ttl(value.clone(), seconds);
                pipe.pset_ex(ck, entry.encode(), milliseconds).ignore();
            }
        } else {
            for (key, value) in keys.iter().zip(values.iter()) {
                let ck = compound_key(cname, key);
                let entry = ManagedEntry::new(value.clone());
                pipe.set(ck, entry.encode()).ignore();
            }
        }
        let _: () = pipe.query_async(&mut conn).await.map_err(map_redis_err)?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let cks: Vec<String> = keys.iter().map(|k| compound_key(cname, k)).collect();
        let mut conn = self.connection();
        let res: i64 = conn.del(&cks).await.map_err(map_redis_err)?;
        Ok(res as usize)
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
        let cname = self.collection_name(collection);
        let prefix = format!("{}{}", cname, COLLECTION_SEPARATOR);
        let mut conn = self.connection();
        let pattern = format!("{}*", prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let limit = limit.unwrap_or(10_000).min(10_000);
        Ok(keys
            .into_iter()
            .filter_map(|k| k.strip_prefix(&prefix).map(|s| s.to_string()))
            .take(limit)
            .collect())
    }
}

#[async_trait]
impl AsyncEnumerateCollections for RedisStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let mut conn = self.connection();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("*")
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let mut collections = std::collections::HashSet::new();
        for key in keys {
            if let Some(pos) = key.find(COLLECTION_SEPARATOR) {
                collections.insert(key[..pos].to_string());
            }
        }
        let limit = limit.unwrap_or(10_000).min(10_000);
        Ok(collections.into_iter().take(limit).collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for RedisStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let prefix = format!("{}{}*", collection, COLLECTION_SEPARATOR);
        let mut conn = self.connection();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&prefix)
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        if keys.is_empty() {
            return Ok(false);
        }
        let _: () = conn.del(&keys).await.map_err(map_redis_err)?;
        Ok(true)
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
