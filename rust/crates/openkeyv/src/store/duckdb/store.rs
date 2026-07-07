use super::client::DuckDBClient;
use super::config::DuckDBConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;

/// DuckDB-backed key-value store.
///
/// Uses an in-process DuckDB connection (in-memory or file-backed).
/// Values are stored as JSON strings with TTL metadata.
pub struct DuckDBStore {
    client: DuckDBClient,
    config: DuckDBConfig,
}

impl DuckDBStore {
    pub async fn new(path: Option<&str>, table_name: Option<&str>) -> Result<Self> {
        let conn = match path {
            None | Some(":memory:") => duckdb::Connection::open_in_memory(),
            Some(p) => duckdb::Connection::open(p),
        }
        .map_err(|e| Error::StoreConnection {
            message: format!("failed to open duckdb: {e}"),
        })?;
        Self::from_conn(conn, table_name).await
    }

    pub async fn from_conn(conn: duckdb::Connection, table_name: Option<&str>) -> Result<Self> {
        let store = Self::with_config(conn, DuckDBConfig::new(table_name)?);
        store.ensure_table().await?;
        Ok(store)
    }

    pub fn with_config(conn: duckdb::Connection, config: DuckDBConfig) -> Self {
        Self {
            client: DuckDBClient::new(conn),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn conn(&self) -> &tokio::sync::Mutex<duckdb::Connection> {
        self.client.conn()
    }

    async fn ensure_table(&self) -> Result<()> {
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                collection TEXT NOT NULL,\
                key TEXT NOT NULL,\
                value TEXT NOT NULL,\
                created_at TIMESTAMPTZ,\
                expires_at TIMESTAMPTZ,\
                PRIMARY KEY (collection, key)\
            )",
            self.config.table_name
        );
        let guard = self.conn().lock().await;
        guard
            .execute(&create_sql, [])
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create table: {e}"),
            })?;

        let idx_collection = format!("idx_{}_collection", self.config.table_name);
        let idx_expires = format!("idx_{}_expires_at", self.config.table_name);
        guard
            .execute(
                &format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {}(collection)",
                    idx_collection, self.config.table_name
                ),
                [],
            )
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create collection index: {e}"),
            })?;
        guard
            .execute(
                &format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {}(expires_at)",
                    idx_expires, self.config.table_name
                ),
                [],
            )
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create expires index: {e}"),
            })?;
        Ok(())
    }

    async fn get_entry(&self, key: &str, collection: &str) -> Result<Option<ManagedEntry>> {
        let sql = format!(
            "SELECT value, created_at, expires_at FROM {} WHERE collection = ?1 AND key = ?2",
            self.config.table_name
        );
        let guard = self.conn().lock().await;
        let mut stmt = guard.prepare(&sql).map_err(|e| Error::StoreConnection {
            message: format!("failed to prepare get: {e}"),
        })?;
        let mut rows =
            stmt.query(duckdb::params![collection, key])
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to query get: {e}"),
                })?;
        if let Some(row) = rows.next().map_err(|e| Error::StoreConnection {
            message: format!("failed to read row: {e}"),
        })? {
            let value_str: String = row
                .get(0)
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let value: Value = serde_json::from_str(&value_str)
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let created_at: Option<chrono::DateTime<chrono::Utc>> = row
                .get(1)
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let expires_at: Option<chrono::DateTime<chrono::Utc>> = row
                .get(2)
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            let entry = ManagedEntry {
                value,
                created_at,
                expires_at,
            };
            if entry.is_expired() {
                Ok(None)
            } else {
                Ok(Some(entry))
            }
        } else {
            Ok(None)
        }
    }

    async fn put_entry(&self, key: &str, collection: &str, entry: &ManagedEntry) -> Result<()> {
        let sql = format!(
            "INSERT OR REPLACE INTO {} (collection, key, value, created_at, expires_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            self.config.table_name
        );
        let value_str =
            serde_json::to_string(&entry.value).map_err(|e| Error::Serialization(e.to_string()))?;
        let guard = self.conn().lock().await;
        guard
            .execute(
                &sql,
                duckdb::params![
                    collection,
                    key,
                    value_str,
                    entry.created_at,
                    entry.expires_at
                ],
            )
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to put: {e}"),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for DuckDBStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        Ok(self.get_entry(key, cname).await?.map(|e| e.value))
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
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
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        self.put_entry(key, cname, &entry).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let sql = format!(
            "DELETE FROM {} WHERE collection = ?1 AND key = ?2",
            self.config.table_name
        );
        let guard = self.conn().lock().await;
        let affected = guard
            .execute(&sql, duckdb::params![cname, key])
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to delete: {e}"),
            })?;
        Ok(affected > 0)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key, Some(cname)).await?);
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        let cname = self.collection_name(collection);
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.ttl(key, Some(cname)).await?);
        }
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
        let cname = self.collection_name(collection);
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            self.put_entry(key, cname, &entry).await?;
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
impl AsyncCull for DuckDBStore {
    async fn cull(&self) -> Result<()> {
        let sql = format!(
            "DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at <= now()",
            self.config.table_name
        );
        let guard = self.conn().lock().await;
        guard
            .execute(&sql, [])
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to cull: {e}"),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for DuckDBStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let limit = limit.unwrap_or(10_000).min(10_000);
        let sql = format!(
            "SELECT key FROM {} WHERE collection = ?1 LIMIT ?2",
            self.config.table_name
        );
        let guard = self.conn().lock().await;
        let mut stmt = guard.prepare(&sql).map_err(|e| Error::StoreConnection {
            message: format!("failed to prepare keys: {e}"),
        })?;
        let mut rows = stmt
            .query(duckdb::params![cname, limit as i64])
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to query keys: {e}"),
            })?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().map_err(|e| Error::StoreConnection {
            message: format!("failed to read row: {e}"),
        })? {
            let key: String = row
                .get(0)
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            keys.push(key);
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for DuckDBStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000);
        let sql = format!(
            "SELECT DISTINCT collection FROM {} ORDER BY collection LIMIT ?1",
            self.config.table_name
        );
        let guard = self.conn().lock().await;
        let mut stmt = guard.prepare(&sql).map_err(|e| Error::StoreConnection {
            message: format!("failed to prepare collections: {e}"),
        })?;
        let mut rows =
            stmt.query(duckdb::params![limit as i64])
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to query collections: {e}"),
                })?;
        let mut collections = Vec::new();
        while let Some(row) = rows.next().map_err(|e| Error::StoreConnection {
            message: format!("failed to read row: {e}"),
        })? {
            let name: String = row
                .get(0)
                .map_err(|e| Error::Deserialization(e.to_string()))?;
            collections.push(name);
        }
        Ok(collections)
    }
}

#[async_trait]
impl AsyncDestroyCollection for DuckDBStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let sql = format!(
            "DELETE FROM {} WHERE collection = ?1",
            self.config.table_name
        );
        let guard = self.conn().lock().await;
        let affected = guard
            .execute(&sql, duckdb::params![collection])
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to destroy collection: {e}"),
            })?;
        Ok(affected > 0)
    }
}

#[async_trait]
impl AsyncDestroyStore for DuckDBStore {
    async fn destroy(&self) -> Result<bool> {
        let sql = format!("DROP TABLE IF EXISTS {}", self.config.table_name);
        let guard = self.conn().lock().await;
        guard
            .execute(&sql, [])
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to destroy store: {e}"),
            })?;
        Ok(true)
    }
}
