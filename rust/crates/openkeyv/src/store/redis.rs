use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use redis::{AsyncCommands, RedisError};

const DEFAULT_COLLECTION: &str = "default_collection";
const COLLECTION_SEPARATOR: &str = ":";

fn compound_key(collection: &str, key: &str) -> String {
    format!("{}{}{}", collection, COLLECTION_SEPARATOR, key)
}

fn map_redis_err(e: RedisError) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}

/// Redis-backed key-value store.
///
/// Each collection is represented by a key prefix in Redis.
/// Values are stored as JSON-serialized `ManagedEntry` strings.
pub struct RedisStore {
    conn: redis::aio::MultiplexedConnection,
    default_collection: String,
}

impl RedisStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(map_redis_err)?;
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(map_redis_err)?;
        Ok(Self {
            conn,
            default_collection: DEFAULT_COLLECTION.to_string(),
        })
    }

    pub async fn from_client(client: redis::Client) -> Result<Self> {
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(map_redis_err)?;
        Ok(Self {
            conn,
            default_collection: DEFAULT_COLLECTION.to_string(),
        })
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }
}

#[async_trait]
impl AsyncKeyValue for RedisStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let mut conn = self.conn.clone();
        let res: Option<String> = conn.get(&ck).await.map_err(map_redis_err)?;
        match res {
            Some(json) => {
                let entry: ManagedEntry = serde_json::from_str(&json)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
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
        let mut conn = self.conn.clone();
        let res: Option<String> = conn.get(&ck).await.map_err(map_redis_err)?;
        match res {
            Some(json) => {
                let entry: ManagedEntry = serde_json::from_str(&json)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
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
        let json =
            serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;
        let mut conn = self.conn.clone();
        match ttl {
            Some(seconds) => {
                let secs = seconds as u64;
                let _: () = conn.set_ex(ck, json, secs).await.map_err(map_redis_err)?;
            }
            None => {
                let _: () = conn.set(ck, json).await.map_err(map_redis_err)?;
            }
        }
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let ck = compound_key(cname, key);
        let mut conn = self.conn.clone();
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
        let mut conn = self.conn.clone();
        let res: Vec<Option<String>> = conn.mget(&cks).await.map_err(map_redis_err)?;
        res.into_iter()
            .map(|opt| match opt {
                Some(json) => {
                    let entry: ManagedEntry = serde_json::from_str(&json)
                        .map_err(|e| Error::Deserialization(e.to_string()))?;
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
        let mut conn = self.conn.clone();
        let res: Vec<Option<String>> = conn.mget(&cks).await.map_err(map_redis_err)?;
        res.into_iter()
            .map(|opt| match opt {
                Some(json) => {
                    let entry: ManagedEntry = serde_json::from_str(&json)
                        .map_err(|e| Error::Deserialization(e.to_string()))?;
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
        let mut conn = self.conn.clone();

        if let Some(seconds) = ttl {
            let secs = seconds as u64;
            for (key, value) in keys.iter().zip(values.iter()) {
                let ck = compound_key(cname, key);
                let entry = ManagedEntry::with_ttl(value.clone(), seconds);
                let json = serde_json::to_string(&entry)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                let _: () = conn.set_ex(ck, json, secs).await.map_err(map_redis_err)?;
            }
        } else {
            let mut pipe = redis::pipe();
            for (key, value) in keys.iter().zip(values.iter()) {
                let ck = compound_key(cname, key);
                let entry = ManagedEntry::new(value.clone());
                let json = serde_json::to_string(&entry)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                pipe.set(ck, json).ignore();
            }
            let _: () = pipe.query_async(&mut conn).await.map_err(map_redis_err)?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let cks: Vec<String> = keys.iter().map(|k| compound_key(cname, k)).collect();
        let mut conn = self.conn.clone();
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
        let mut conn = self.conn.clone();
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
        let mut conn = self.conn.clone();
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
        let mut conn = self.conn.clone();
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
        let mut conn = self.conn.clone();
        let _: () = redis::cmd("FLUSHDB")
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(true)
    }
}
