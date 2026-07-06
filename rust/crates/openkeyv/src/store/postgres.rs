use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::Row;
use std::collections::HashMap;

const DEFAULT_COLLECTION: &str = "default_collection";

fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::StoreSetup {
            message: "table name cannot be empty".to_string(),
        });
    }
    if name.len() > 63 {
        return Err(Error::StoreSetup {
            message: format!("table name too long (>63): {name}"),
        });
    }
    if name.chars().next().unwrap().is_ascii_digit() {
        return Err(Error::StoreSetup {
            message: format!("table name must not start with a digit: {name}"),
        });
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(Error::StoreSetup {
            message: format!("table name must be alphanumeric (with underscores): {name}"),
        });
    }
    Ok(())
}

/// PostgreSQL-backed key-value store.
///
/// Uses a single table with columns for collection, key, JSONB value, and TTL metadata.
pub struct PostgresStore {
    pool: sqlx::PgPool,
    table_name: String,
    default_collection: String,
}

impl PostgresStore {
    pub async fn new(url: &str, table_name: Option<&str>) -> Result<Self> {
        let pool = sqlx::PgPool::connect(url)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to connect to postgres: {e}"),
            })?;
        Self::from_pool(pool, table_name).await
    }

    pub async fn from_pool(pool: sqlx::PgPool, table_name: Option<&str>) -> Result<Self> {
        let table_name = table_name.unwrap_or("kv_store").to_string();
        validate_table_name(&table_name)?;
        let store = Self {
            pool,
            table_name,
            default_collection: DEFAULT_COLLECTION.to_string(),
        };
        store.ensure_table().await?;
        Ok(store)
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    async fn ensure_table(&self) -> Result<()> {
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                collection TEXT NOT NULL,\
                key TEXT NOT NULL,\
                value JSONB NOT NULL,\
                ttl DOUBLE PRECISION,\
                created_at TIMESTAMPTZ,\
                expires_at TIMESTAMPTZ,\
                PRIMARY KEY (collection, key)\
            )",
            self.table_name
        );
        sqlx::query(&create_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create table: {e}"),
            })?;

        let mut index_name = format!("idx_{}_expires_at", self.table_name);
        if index_name.len() > 63 {
            let hash = blake3::hash(self.table_name.as_bytes()).to_hex();
            index_name = format!("idx_{}_exp", &hash[..16]);
        }
        let index_sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}(expires_at) WHERE expires_at IS NOT NULL",
            index_name, self.table_name
        );
        sqlx::query(&index_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create index: {e}"),
            })?;
        Ok(())
    }

    async fn get_entry(&self, key: &str, collection: &str) -> Result<Option<ManagedEntry>> {
        let sql = format!(
            "SELECT value, created_at, expires_at FROM {} WHERE collection = $1 AND key = $2",
            self.table_name
        );
        let row = sqlx::query(&sql)
            .bind(collection)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to get: {e}"),
            })?;
        match row {
            Some(row) => {
                let value: sqlx::types::Json<HashMap<String, Value>> = row
                    .try_get("value")
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                let created_at: Option<chrono::DateTime<chrono::Utc>> =
                    row.try_get("created_at")
                        .map_err(|e| Error::Deserialization(e.to_string()))?;
                let expires_at: Option<chrono::DateTime<chrono::Utc>> =
                    row.try_get("expires_at")
                        .map_err(|e| Error::Deserialization(e.to_string()))?;
                let entry = ManagedEntry {
                    value: value.0,
                    created_at,
                    expires_at,
                };
                if entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry))
                }
            }
            None => Ok(None),
        }
    }

    async fn put_entry(
        &self,
        key: &str,
        collection: &str,
        entry: &ManagedEntry,
        ttl: Option<f64>,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} (collection, key, value, ttl, created_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (collection, key) \
             DO UPDATE SET value = EXCLUDED.value, ttl = EXCLUDED.ttl, expires_at = EXCLUDED.expires_at",
            self.table_name
        );
        sqlx::query(&sql)
            .bind(collection)
            .bind(key)
            .bind(sqlx::types::Json(&entry.value))
            .bind(ttl)
            .bind(entry.created_at)
            .bind(entry.expires_at)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to put: {e}"),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for PostgresStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        Ok(self.get_entry(key, cname).await?.map(|e| e.value))
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let cname = self.collection_name(collection);
        match self.get_entry(key, cname).await? {
            Some(entry) => {
                let ttl = entry.ttl().unwrap_or(0.0);
                Ok(Some((entry.value, ttl)))
            }
            None => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        self.put_entry(key, cname, &entry, ttl).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let sql = format!(
            "DELETE FROM {} WHERE collection = $1 AND key = $2",
            self.table_name
        );
        let res = sqlx::query(&sql)
            .bind(cname)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to delete: {e}"),
            })?;
        Ok(res.rows_affected() > 0)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let cname = self.collection_name(collection);
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let sql = format!(
            "SELECT key, value, created_at, expires_at FROM {} WHERE collection = $1 AND key = ANY($2)",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(cname)
            .bind(keys)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to get_many: {e}"),
            })?;
        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            let key: String = row
                .try_get("key")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let value: sqlx::types::Json<HashMap<String, Value>> = row
                .try_get("value")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let created_at: Option<chrono::DateTime<chrono::Utc>> = row
                .try_get("created_at")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let expires_at: Option<chrono::DateTime<chrono::Utc>> = row
                .try_get("expires_at")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let entry = ManagedEntry {
                value: value.0,
                created_at,
                expires_at,
            };
            if !entry.is_expired() {
                map.insert(key, entry.value);
            }
        }
        Ok(keys.iter().map(|k| map.get(k).cloned()).collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let cname = self.collection_name(collection);
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let sql = format!(
            "SELECT key, value, created_at, expires_at FROM {} WHERE collection = $1 AND key = ANY($2)",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(cname)
            .bind(keys)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to ttl_many: {e}"),
            })?;
        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            let key: String = row
                .try_get("key")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let value: sqlx::types::Json<HashMap<String, Value>> = row
                .try_get("value")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let created_at: Option<chrono::DateTime<chrono::Utc>> = row
                .try_get("created_at")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let expires_at: Option<chrono::DateTime<chrono::Utc>> = row
                .try_get("expires_at")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let entry = ManagedEntry {
                value: value.0,
                created_at,
                expires_at,
            };
            if !entry.is_expired() {
                let ttl = entry.ttl().unwrap_or(0.0);
                map.insert(key, (entry.value, ttl));
            }
        }
        Ok(keys.iter().map(|k| map.get(k).cloned()).collect())
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[HashMap<String, Value>],
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
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            self.put_entry(key, cname, &entry, ttl).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        if keys.is_empty() {
            return Ok(0);
        }
        let sql = format!(
            "DELETE FROM {} WHERE collection = $1 AND key = ANY($2)",
            self.table_name
        );
        let res = sqlx::query(&sql)
            .bind(cname)
            .bind(keys)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to delete_many: {e}"),
            })?;
        Ok(res.rows_affected() as usize)
    }
}

#[async_trait]
impl AsyncCull for PostgresStore {
    async fn cull(&self) -> Result<()> {
        let sql = format!(
            "DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at <= NOW()",
            self.table_name
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to cull: {e}"),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for PostgresStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let limit = limit.unwrap_or(10_000).min(10_000) as i64;
        let sql = format!(
            "SELECT key FROM {} WHERE collection = $1 LIMIT $2",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(cname)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to list keys: {e}"),
            })?;
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let key: String = row
                .try_get("key")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            keys.push(key);
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for PostgresStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000) as i64;
        let sql = format!(
            "SELECT DISTINCT collection FROM {} ORDER BY collection LIMIT $1",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to list collections: {e}"),
            })?;
        let mut collections = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row
                .try_get("collection")
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            collections.push(name);
        }
        Ok(collections)
    }
}

#[async_trait]
impl AsyncDestroyCollection for PostgresStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let sql = format!("DELETE FROM {} WHERE collection = $1", self.table_name);
        let res = sqlx::query(&sql)
            .bind(collection)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to destroy collection: {e}"),
            })?;
        Ok(res.rows_affected() > 0)
    }
}

#[async_trait]
impl AsyncDestroyStore for PostgresStore {
    async fn destroy(&self) -> Result<bool> {
        let sql = format!("DELETE FROM {}", self.table_name);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to destroy store: {e}"),
            })?;
        Ok(true)
    }
}
