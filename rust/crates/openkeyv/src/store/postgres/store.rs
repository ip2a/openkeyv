use super::client::PostgresClient;
use super::config::PostgresConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

const DEFAULT_PAGE_SIZE: usize = 10_000;
const PAGE_LIMIT: usize = 10_000;

struct StoredRow {
    collection: String,
    key: String,
    raw_entry: Bytes,
    expires_at: Option<DateTime<Utc>>,
}

/// PostgreSQL-backed key-value store.
///
/// Each row stores a collection, key, complete binary `OKVE1` entry, and an
/// optional indexed expiration timestamp mirrored from the entry metadata.
pub struct PostgresStore {
    client: PostgresClient,
    config: PostgresConfig,
}

impl PostgresStore {
    pub async fn new(url: &str, table_name: Option<&str>) -> Result<Self> {
        let pool = sqlx::PgPool::connect(url)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to connect to Postgres: {error}"),
            })?;
        Self::from_pool(pool, table_name).await
    }

    pub async fn from_pool(pool: sqlx::PgPool, table_name: Option<&str>) -> Result<Self> {
        let store = Self::with_config(pool, PostgresConfig::new(table_name)?);
        store.ensure_table().await?;
        Ok(store)
    }

    pub fn with_config(pool: sqlx::PgPool, config: PostgresConfig) -> Self {
        Self {
            client: PostgresClient::new(pool),
            config,
        }
    }

    fn validate_text_identity(kind: &str, identity: &str) -> Result<()> {
        if identity.contains('\0') {
            return Err(Error::InvalidKey(format!(
                "Postgres {kind} cannot contain NUL"
            )));
        }
        Ok(())
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> Result<&'a str> {
        let collection = collection.unwrap_or(&self.config.default_collection);
        Self::validate_text_identity("collection", collection)?;
        Ok(collection)
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.client.pool()
    }

    fn expires_index_name(&self) -> String {
        let plain = format!("idx_{}_expires_at", self.config.table_name);
        if plain.len() <= 63 {
            plain
        } else {
            let hash = blake3::hash(self.config.table_name.as_bytes()).to_hex();
            format!("idx_{}_expires_at", &hash[..16])
        }
    }

    async fn ensure_table(&self) -> Result<()> {
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!("failed to start Postgres setup transaction: {error}"),
            })?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                collection TEXT NOT NULL,\
                key TEXT NOT NULL,\
                entry BYTEA NOT NULL,\
                expires_at TIMESTAMPTZ,\
                PRIMARY KEY (collection, key)\
            )",
            self.config.table_name
        ))
        .execute(&mut *transaction)
        .await
        .map_err(|error| Error::StoreSetup {
            message: format!(
                "failed to create Postgres table {}: {error}",
                self.config.table_name
            ),
        })?;

        sqlx::query(&format!(
            "LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
            self.config.table_name
        ))
        .execute(&mut *transaction)
        .await
        .map_err(|error| Error::StoreSetup {
            message: format!(
                "failed to lock Postgres table {} for validation: {error}",
                self.config.table_name
            ),
        })?;

        let columns = sqlx::query(
            "SELECT ordinal_position, column_name, data_type, udt_name, is_nullable, \
                    column_default, is_identity, is_generated \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name = $1 \
             ORDER BY ordinal_position",
        )
        .bind(&self.config.table_name)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| Error::StoreSetup {
            message: format!(
                "failed to inspect Postgres table {}: {error}",
                self.config.table_name
            ),
        })?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get::<i32, _>("ordinal_position")?,
                row.try_get::<String, _>("column_name")?,
                row.try_get::<String, _>("data_type")?,
                row.try_get::<String, _>("udt_name")?,
                row.try_get::<String, _>("is_nullable")?,
                row.try_get::<Option<String>, _>("column_default")?,
                row.try_get::<String, _>("is_identity")?,
                row.try_get::<String, _>("is_generated")?,
            ))
        })
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
        .map_err(|error| Error::StoreSetup {
            message: format!(
                "invalid Postgres column metadata for {}: {error}",
                self.config.table_name
            ),
        })?;

        let expected_columns = vec![
            (
                1,
                "collection".to_string(),
                "text".to_string(),
                "text".to_string(),
                "NO".to_string(),
                None,
                "NO".to_string(),
                "NEVER".to_string(),
            ),
            (
                2,
                "key".to_string(),
                "text".to_string(),
                "text".to_string(),
                "NO".to_string(),
                None,
                "NO".to_string(),
                "NEVER".to_string(),
            ),
            (
                3,
                "entry".to_string(),
                "bytea".to_string(),
                "bytea".to_string(),
                "NO".to_string(),
                None,
                "NO".to_string(),
                "NEVER".to_string(),
            ),
            (
                4,
                "expires_at".to_string(),
                "timestamp with time zone".to_string(),
                "timestamptz".to_string(),
                "YES".to_string(),
                None,
                "NO".to_string(),
                "NEVER".to_string(),
            ),
        ];
        if columns != expected_columns {
            return Err(Error::StoreSetup {
                message: format!(
                    "Postgres table {} does not match the required OpenKeyV schema",
                    self.config.table_name
                ),
            });
        }

        let constraints = sqlx::query(
            "SELECT c.contype::text AS constraint_type, pg_get_constraintdef(c.oid) AS definition \
             FROM pg_constraint c \
             JOIN pg_class t ON t.oid = c.conrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE n.nspname = current_schema() AND t.relname = $1 \
             ORDER BY c.contype, c.conname",
        )
        .bind(&self.config.table_name)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| Error::StoreSetup {
            message: format!(
                "failed to inspect Postgres constraints for {}: {error}",
                self.config.table_name
            ),
        })?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("constraint_type")?,
                row.try_get::<String, _>("definition")?,
            ))
        })
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
        .map_err(|error| Error::StoreSetup {
            message: format!(
                "invalid Postgres constraint metadata for {}: {error}",
                self.config.table_name
            ),
        })?;
        if constraints != [("p".to_string(), "PRIMARY KEY (collection, key)".to_string())] {
            return Err(Error::StoreSetup {
                message: format!(
                    "Postgres table {} does not have the required primary key",
                    self.config.table_name
                ),
            });
        }

        let index_name = self.expires_index_name();
        let index_metadata_sql = "SELECT ic.relname AS index_name, i.indisunique, i.indisprimary, \
                    ARRAY(\
                        SELECT a.attname::text \
                        FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) \
                        JOIN pg_attribute a \
                          ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
                        ORDER BY k.ord\
                    ) AS columns, \
                    pg_get_expr(i.indpred, i.indrelid) AS predicate \
             FROM pg_index i \
             JOIN pg_class t ON t.oid = i.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             JOIN pg_class ic ON ic.oid = i.indexrelid \
             WHERE n.nspname = current_schema() AND t.relname = $1 \
               AND (\
                    ic.relname = $2 \
                    OR (\
                        i.indexprs IS NULL \
                        AND ARRAY(\
                            SELECT a.attname::text \
                            FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) \
                            JOIN pg_attribute a \
                              ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
                            ORDER BY k.ord\
                        ) = ARRAY['expires_at']::text[]\
                    )\
               ) \
             ORDER BY ic.relname";

        let mut indexes = sqlx::query(index_metadata_sql)
            .bind(&self.config.table_name)
            .bind(&index_name)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to inspect Postgres expiration index for {}: {error}",
                    self.config.table_name
                ),
            })?;

        if indexes.is_empty() {
            sqlx::query(&format!(
                "CREATE INDEX {} ON {}(expires_at) WHERE expires_at IS NOT NULL",
                index_name, self.config.table_name
            ))
            .execute(&mut *transaction)
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to create Postgres expiration index for {}: {error}",
                    self.config.table_name
                ),
            })?;
            indexes = sqlx::query(index_metadata_sql)
                .bind(&self.config.table_name)
                .bind(&index_name)
                .fetch_all(&mut *transaction)
                .await
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to verify Postgres expiration index for {}: {error}",
                        self.config.table_name
                    ),
                })?;
        }

        let indexes = indexes
            .into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("index_name")?,
                    row.try_get::<bool, _>("indisunique")?,
                    row.try_get::<bool, _>("indisprimary")?,
                    row.try_get::<Vec<String>, _>("columns")?,
                    row.try_get::<Option<String>, _>("predicate")?,
                ))
            })
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "invalid Postgres index metadata for {}: {error}",
                    self.config.table_name
                ),
            })?;
        if indexes
            != [(
                index_name,
                false,
                false,
                vec!["expires_at".to_string()],
                Some("(expires_at IS NOT NULL)".to_string()),
            )]
        {
            return Err(Error::StoreSetup {
                message: format!(
                    "Postgres table {} has an invalid or duplicate expiration index",
                    self.config.table_name
                ),
            });
        }

        transaction
            .commit()
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!("failed to commit Postgres setup transaction: {error}"),
            })
    }

    fn decode_entry(
        key: &str,
        raw_entry: Bytes,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<ManagedEntry> {
        let entry = ManagedEntry::decode(raw_entry).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode Postgres OKVE1 entry for key {key}: {error}"
            ))
        })?;
        let embedded_expires_at = entry
            .expires_at
            .map(|expires_at| expires_at.timestamp_millis());
        let indexed_expires_at = expires_at.map(|expires_at| expires_at.timestamp_millis());
        if embedded_expires_at != indexed_expires_at {
            return Err(Error::Deserialization(format!(
                "Postgres expires_at does not match OKVE1 metadata for key {key}"
            )));
        }
        Ok(entry)
    }
}

#[async_trait]
impl AsyncKeyValue for PostgresStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let collection = self.collection_name(collection)?;
        Self::validate_text_identity("key", key)?;
        let row = sqlx::query(&format!(
            "SELECT entry, expires_at FROM {} WHERE collection = $1 AND key = $2",
            self.config.table_name
        ))
        .bind(collection)
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to get Postgres key {key}: {error}"),
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let raw_entry = Bytes::from(
            row.try_get::<Vec<u8>, _>("entry")
                .map_err(|error| Error::Deserialization(error.to_string()))?,
        );
        let expires_at = row
            .try_get::<Option<DateTime<Utc>>, _>("expires_at")
            .map_err(|error| Error::Deserialization(error.to_string()))?;
        let entry = Self::decode_entry(key, raw_entry.clone(), expires_at)?;
        if entry.is_expired() {
            sqlx::query(&format!(
                "DELETE FROM {} WHERE collection = $1 AND key = $2 \
                 AND entry = $3 AND expires_at IS NOT DISTINCT FROM $4",
                self.config.table_name
            ))
            .bind(collection)
            .bind(key)
            .bind(raw_entry.as_ref())
            .bind(expires_at)
            .execute(self.pool())
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to conditionally delete expired Postgres key {key}: {error}"
                ),
            })?;
            return Ok(None);
        }
        Ok(Some(entry.value))
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let collection = self.collection_name(collection)?;
        Self::validate_text_identity("key", key)?;
        let row = sqlx::query(&format!(
            "SELECT entry, expires_at FROM {} WHERE collection = $1 AND key = $2",
            self.config.table_name
        ))
        .bind(collection)
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to get Postgres TTL for key {key}: {error}"),
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let raw_entry = Bytes::from(
            row.try_get::<Vec<u8>, _>("entry")
                .map_err(|error| Error::Deserialization(error.to_string()))?,
        );
        let expires_at = row
            .try_get::<Option<DateTime<Utc>>, _>("expires_at")
            .map_err(|error| Error::Deserialization(error.to_string()))?;
        let entry = Self::decode_entry(key, raw_entry.clone(), expires_at)?;
        if entry.is_expired() {
            sqlx::query(&format!(
                "DELETE FROM {} WHERE collection = $1 AND key = $2 \
                 AND entry = $3 AND expires_at IS NOT DISTINCT FROM $4",
                self.config.table_name
            ))
            .bind(collection)
            .bind(key)
            .bind(raw_entry.as_ref())
            .bind(expires_at)
            .execute(self.pool())
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to conditionally delete expired Postgres TTL key {key}: {error}"
                ),
            })?;
            return Ok(None);
        }
        let ttl = entry.ttl();
        Ok(Some((entry.value, ttl)))
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let collection = self.collection_name(collection)?;
        Self::validate_text_identity("key", key)?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        sqlx::query(&format!(
            "INSERT INTO {} (collection, key, entry, expires_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (collection, key) DO UPDATE SET \
             entry = EXCLUDED.entry, expires_at = EXCLUDED.expires_at",
            self.config.table_name
        ))
        .bind(collection)
        .bind(key)
        .bind(entry.encode())
        .bind(entry.expires_at)
        .execute(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to put Postgres key {key}: {error}"),
        })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let collection = self.collection_name(collection)?;
        Self::validate_text_identity("key", key)?;
        let result = sqlx::query(&format!(
            "DELETE FROM {} WHERE collection = $1 AND key = $2",
            self.config.table_name
        ))
        .bind(collection)
        .bind(key)
        .execute(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to delete Postgres key {key}: {error}"),
        })?;
        Ok(result.rows_affected() == 1)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let collection = self.collection_name(collection)?;
        for key in keys {
            Self::validate_text_identity("key", key)?;
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let requested = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let rows = sqlx::query(&format!(
            "SELECT key, entry, expires_at FROM {} \
             WHERE collection = $1 AND key = ANY($2)",
            self.config.table_name
        ))
        .bind(collection)
        .bind(keys)
        .fetch_all(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to get Postgres batch: {error}"),
        })?;

        let mut values = HashMap::with_capacity(rows.len());
        let mut expired = Vec::new();
        for row in rows {
            let key = row
                .try_get::<String, _>("key")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            if !requested.contains(key.as_str()) {
                return Err(Error::Deserialization(format!(
                    "Postgres batch query returned unrequested key {key}"
                )));
            }
            let raw_entry = Bytes::from(
                row.try_get::<Vec<u8>, _>("entry")
                    .map_err(|error| Error::Deserialization(error.to_string()))?,
            );
            let expires_at = row
                .try_get::<Option<DateTime<Utc>>, _>("expires_at")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let entry = Self::decode_entry(&key, raw_entry.clone(), expires_at)?;
            if entry.is_expired() {
                expired.push(StoredRow {
                    collection: collection.to_string(),
                    key,
                    raw_entry,
                    expires_at,
                });
                continue;
            }
            if values.insert(key.clone(), entry.value).is_some() {
                return Err(Error::Deserialization(format!(
                    "Postgres batch query returned duplicate key {key}"
                )));
            }
        }

        if !expired.is_empty() {
            let expired_keys = expired
                .iter()
                .map(|row| row.key.as_str())
                .collect::<Vec<_>>();
            let expired_entries = expired
                .iter()
                .map(|row| row.raw_entry.as_ref())
                .collect::<Vec<_>>();
            let expired_timestamps = expired.iter().map(|row| row.expires_at).collect::<Vec<_>>();
            sqlx::query(&format!(
                "DELETE FROM {0} AS target \
                 USING UNNEST($2::text[], $3::bytea[], $4::timestamptz[]) \
                       AS observed(key, entry, expires_at) \
                 WHERE target.collection = $1 \
                   AND target.key = observed.key \
                   AND target.entry = observed.entry \
                   AND target.expires_at IS NOT DISTINCT FROM observed.expires_at",
                self.config.table_name
            ))
            .bind(collection)
            .bind(&expired_keys)
            .bind(&expired_entries)
            .bind(&expired_timestamps)
            .execute(self.pool())
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to clean expired Postgres batch rows: {error}"),
            })?;
        }

        Ok(keys
            .iter()
            .map(|key| values.get(key.as_str()).cloned())
            .collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let collection = self.collection_name(collection)?;
        for key in keys {
            Self::validate_text_identity("key", key)?;
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let requested = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let rows = sqlx::query(&format!(
            "SELECT key, entry, expires_at FROM {} \
             WHERE collection = $1 AND key = ANY($2)",
            self.config.table_name
        ))
        .bind(collection)
        .bind(keys)
        .fetch_all(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to get Postgres TTL batch: {error}"),
        })?;

        let mut values = HashMap::with_capacity(rows.len());
        let mut expired = Vec::new();
        for row in rows {
            let key = row
                .try_get::<String, _>("key")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            if !requested.contains(key.as_str()) {
                return Err(Error::Deserialization(format!(
                    "Postgres TTL batch returned unrequested key {key}"
                )));
            }
            let raw_entry = Bytes::from(
                row.try_get::<Vec<u8>, _>("entry")
                    .map_err(|error| Error::Deserialization(error.to_string()))?,
            );
            let expires_at = row
                .try_get::<Option<DateTime<Utc>>, _>("expires_at")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let entry = Self::decode_entry(&key, raw_entry.clone(), expires_at)?;
            if entry.is_expired() {
                expired.push(StoredRow {
                    collection: collection.to_string(),
                    key,
                    raw_entry,
                    expires_at,
                });
                continue;
            }
            let ttl = entry.ttl();
            if values.insert(key.clone(), (entry.value, ttl)).is_some() {
                return Err(Error::Deserialization(format!(
                    "Postgres TTL batch returned duplicate key {key}"
                )));
            }
        }

        if !expired.is_empty() {
            let expired_keys = expired
                .iter()
                .map(|row| row.key.as_str())
                .collect::<Vec<_>>();
            let expired_entries = expired
                .iter()
                .map(|row| row.raw_entry.as_ref())
                .collect::<Vec<_>>();
            let expired_timestamps = expired.iter().map(|row| row.expires_at).collect::<Vec<_>>();
            sqlx::query(&format!(
                "DELETE FROM {0} AS target \
                 USING UNNEST($2::text[], $3::bytea[], $4::timestamptz[]) \
                       AS observed(key, entry, expires_at) \
                 WHERE target.collection = $1 \
                   AND target.key = observed.key \
                   AND target.entry = observed.entry \
                   AND target.expires_at IS NOT DISTINCT FROM observed.expires_at",
                self.config.table_name
            ))
            .bind(collection)
            .bind(&expired_keys)
            .bind(&expired_entries)
            .bind(&expired_timestamps)
            .execute(self.pool())
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to clean expired Postgres TTL batch rows: {error}"),
            })?;
        }

        Ok(keys
            .iter()
            .map(|key| values.get(key.as_str()).cloned())
            .collect())
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
        let collection = self.collection_name(collection)?;
        for key in keys {
            Self::validate_text_identity("key", key)?;
        }
        if keys.is_empty() {
            return Ok(());
        }

        let mut last_indices = HashMap::with_capacity(keys.len());
        for (index, key) in keys.iter().enumerate() {
            last_indices.insert(key.as_str(), index);
        }
        let mut final_indices = last_indices.into_values().collect::<Vec<_>>();
        final_indices.sort_unstable();

        let mut final_keys = Vec::with_capacity(final_indices.len());
        let mut entries = Vec::with_capacity(final_indices.len());
        let mut expires_at = Vec::with_capacity(final_indices.len());
        for index in final_indices {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(values[index].clone(), seconds)?,
                None => ManagedEntry::new(values[index].clone()),
            };
            final_keys.push(keys[index].as_str());
            entries.push(entry.encode());
            expires_at.push(entry.expires_at);
        }

        sqlx::query(&format!(
            "INSERT INTO {0} (collection, key, entry, expires_at) \
             SELECT $1, rows.key, rows.entry, rows.expires_at \
             FROM UNNEST($2::text[], $3::bytea[], $4::timestamptz[]) \
                  AS rows(key, entry, expires_at) \
             ON CONFLICT (collection, key) DO UPDATE SET \
             entry = EXCLUDED.entry, expires_at = EXCLUDED.expires_at",
            self.config.table_name
        ))
        .bind(collection)
        .bind(&final_keys)
        .bind(&entries)
        .bind(&expires_at)
        .execute(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to put Postgres batch: {error}"),
        })?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let collection = self.collection_name(collection)?;
        for key in keys {
            Self::validate_text_identity("key", key)?;
        }
        if keys.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query(&format!(
            "DELETE FROM {} WHERE collection = $1 AND key = ANY($2)",
            self.config.table_name
        ))
        .bind(collection)
        .bind(keys)
        .execute(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to delete Postgres batch: {error}"),
        })?;
        Ok(result.rows_affected() as usize)
    }
}

#[async_trait]
impl AsyncCull for PostgresStore {
    async fn cull(&self) -> Result<()> {
        let rows = sqlx::query(&format!(
            "SELECT collection, key, entry, expires_at FROM {} \
             WHERE expires_at IS NOT NULL AND expires_at <= now()",
            self.config.table_name
        ))
        .fetch_all(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to query Postgres cull rows: {error}"),
        })?;

        let mut expired = Vec::with_capacity(rows.len());
        for row in rows {
            let collection = row
                .try_get::<String, _>("collection")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let key = row
                .try_get::<String, _>("key")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let raw_entry = Bytes::from(
                row.try_get::<Vec<u8>, _>("entry")
                    .map_err(|error| Error::Deserialization(error.to_string()))?,
            );
            let expires_at = row
                .try_get::<Option<DateTime<Utc>>, _>("expires_at")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let entry = Self::decode_entry(&key, raw_entry.clone(), expires_at)?;
            if !entry.is_expired() {
                return Err(Error::Deserialization(format!(
                    "Postgres expiration query returned live key {key}"
                )));
            }
            expired.push(StoredRow {
                collection,
                key,
                raw_entry,
                expires_at,
            });
        }
        if expired.is_empty() {
            return Ok(());
        }

        let collections = expired
            .iter()
            .map(|row| row.collection.as_str())
            .collect::<Vec<_>>();
        let keys = expired
            .iter()
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>();
        let entries = expired
            .iter()
            .map(|row| row.raw_entry.as_ref())
            .collect::<Vec<_>>();
        let expires_at = expired.iter().map(|row| row.expires_at).collect::<Vec<_>>();
        sqlx::query(&format!(
            "DELETE FROM {0} AS target \
             USING UNNEST($1::text[], $2::text[], $3::bytea[], $4::timestamptz[]) \
                   AS observed(collection, key, entry, expires_at) \
             WHERE target.collection = observed.collection \
               AND target.key = observed.key \
               AND target.entry = observed.entry \
               AND target.expires_at IS NOT DISTINCT FROM observed.expires_at",
            self.config.table_name
        ))
        .bind(&collections)
        .bind(&keys)
        .bind(&entries)
        .bind(&expires_at)
        .execute(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to conditionally delete Postgres cull rows: {error}"),
        })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for PostgresStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        let collection = self.collection_name(collection)?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(&format!(
            "SELECT key, entry, expires_at FROM {} \
             WHERE collection = $1 \
               AND (expires_at IS NULL OR expires_at > now()) \
             ORDER BY key LIMIT $2",
            self.config.table_name
        ))
        .bind(collection)
        .bind(limit as i64)
        .fetch_all(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to enumerate Postgres keys: {error}"),
        })?;

        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let key = row
                .try_get::<String, _>("key")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let raw_entry = Bytes::from(
                row.try_get::<Vec<u8>, _>("entry")
                    .map_err(|error| Error::Deserialization(error.to_string()))?,
            );
            let expires_at = row
                .try_get::<Option<DateTime<Utc>>, _>("expires_at")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let entry = Self::decode_entry(&key, raw_entry, expires_at)?;
            if entry.is_expired() {
                return Err(Error::Deserialization(format!(
                    "Postgres key enumeration returned expired key {key}"
                )));
            }
            keys.push(key);
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for PostgresStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(&format!(
            "SELECT collection, key, entry, expires_at FROM {} \
             WHERE expires_at IS NULL OR expires_at > now() \
             ORDER BY collection, key",
            self.config.table_name
        ))
        .fetch_all(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to enumerate Postgres collections: {error}"),
        })?;

        let mut collections = Vec::with_capacity(limit);
        let mut seen = HashSet::with_capacity(limit);
        for row in rows {
            let collection = row
                .try_get::<String, _>("collection")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let key = row
                .try_get::<String, _>("key")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let raw_entry = Bytes::from(
                row.try_get::<Vec<u8>, _>("entry")
                    .map_err(|error| Error::Deserialization(error.to_string()))?,
            );
            let expires_at = row
                .try_get::<Option<DateTime<Utc>>, _>("expires_at")
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let entry = Self::decode_entry(&key, raw_entry, expires_at)?;
            if entry.is_expired() {
                return Err(Error::Deserialization(format!(
                    "Postgres collection enumeration returned expired key {key}"
                )));
            }
            if seen.insert(collection.clone()) {
                collections.push(collection);
                if collections.len() == limit {
                    break;
                }
            }
        }
        Ok(collections)
    }
}

#[async_trait]
impl AsyncDestroyCollection for PostgresStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        Self::validate_text_identity("collection", collection)?;
        let result = sqlx::query(&format!(
            "DELETE FROM {} WHERE collection = $1",
            self.config.table_name
        ))
        .bind(collection)
        .execute(self.pool())
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to destroy Postgres collection {collection}: {error}"),
        })?;
        Ok(result.rows_affected() > 0)
    }
}

#[async_trait]
impl AsyncDestroyStore for PostgresStore {
    async fn destroy(&self) -> Result<bool> {
        let mut transaction =
            self.pool()
                .begin()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to start Postgres destroy transaction: {error}"),
                })?;
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (\
                SELECT 1 FROM information_schema.tables \
                WHERE table_schema = current_schema() \
                  AND table_name = $1 \
                  AND table_type = 'BASE TABLE'\
            )",
        )
        .bind(&self.config.table_name)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!(
                "failed to inspect Postgres table {} for destruction: {error}",
                self.config.table_name
            ),
        })?;
        if !exists {
            transaction
                .rollback()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to finish Postgres destruction check for {}: {error}",
                        self.config.table_name
                    ),
                })?;
            return Ok(false);
        }
        sqlx::query(&format!("DROP TABLE {}", self.config.table_name))
            .execute(&mut *transaction)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to destroy Postgres table {}: {error}",
                    self.config.table_name
                ),
            })?;
        transaction
            .commit()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to commit Postgres store destruction: {error}"),
            })?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TABLE: AtomicU64 = AtomicU64::new(0);

    fn integration_url() -> String {
        std::env::var("OPENKEYV_POSTGRES_URL")
            .expect("OPENKEYV_POSTGRES_URL must point to a Postgres database")
    }

    fn table_name(prefix: &str) -> String {
        format!(
            "openkeyv_{}_{}_{}",
            prefix,
            std::process::id(),
            NEXT_TABLE.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn offline_store() -> PostgresStore {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://127.0.0.1:1/openkeyv")
            .unwrap();
        PostgresStore::with_config(pool, PostgresConfig::new(None).unwrap())
    }

    #[test]
    fn postgres_text_identity_rejects_nul_only() {
        assert!(PostgresStore::validate_text_identity("key", "line\n值").is_ok());
        assert!(matches!(
            PostgresStore::validate_text_identity("key", "bad\0key"),
            Err(Error::InvalidKey(message)) if message == "Postgres key cannot contain NUL"
        ));
    }

    fn assert_invalid_key<T>(result: Result<T>) {
        assert!(matches!(result, Err(Error::InvalidKey(_))));
    }

    #[tokio::test]
    async fn postgres_prevalidates_nul_before_service_access() {
        let store = offline_store();

        assert_invalid_key(store.get("bad\0key", Some("entries")).await);
        assert_invalid_key(store.ttl("bad\0key", Some("entries")).await);
        assert_invalid_key(
            store
                .put("bad\0key", Value::utf8("value"), Some("entries"), None)
                .await,
        );
        assert_invalid_key(store.delete("bad\0key", Some("entries")).await);
        assert_invalid_key(
            store
                .get_many(
                    &["valid".to_string(), "bad\0key".to_string()],
                    Some("entries"),
                )
                .await,
        );
        assert_invalid_key(
            store
                .ttl_many(
                    &["valid".to_string(), "bad\0key".to_string()],
                    Some("entries"),
                )
                .await,
        );
        assert_invalid_key(
            store
                .put_many(
                    &["valid".to_string(), "bad\0key".to_string()],
                    &[Value::utf8("first"), Value::utf8("second")],
                    Some("entries"),
                    None,
                )
                .await,
        );
        assert_invalid_key(
            store
                .delete_many(
                    &["valid".to_string(), "bad\0key".to_string()],
                    Some("entries"),
                )
                .await,
        );
        assert_invalid_key(store.keys(Some("entries\0"), Some(0)).await);
        assert_invalid_key(store.destroy_collection("entries\0").await);
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_POSTGRES_URL"]
    async fn postgres_batch_nul_validation_has_no_side_effects() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let table = table_name("nul");
        let store = PostgresStore::from_pool(pool.clone(), Some(&table))
            .await
            .unwrap();
        store
            .put("existing", Value::utf8("before"), Some("entries"), None)
            .await
            .unwrap();

        let put_error = store
            .put_many(
                &["new".to_string(), "bad\0key".to_string()],
                &[Value::utf8("new-value"), Value::utf8("invalid")],
                Some("entries"),
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(put_error, Error::InvalidKey(_)));
        assert_eq!(
            store.get("existing", Some("entries")).await.unwrap(),
            Some(Value::utf8("before"))
        );
        assert_eq!(store.get("new", Some("entries")).await.unwrap(), None);

        let delete_error = store
            .delete_many(
                &["existing".to_string(), "bad\0key".to_string()],
                Some("entries"),
            )
            .await
            .unwrap_err();
        assert!(matches!(delete_error, Error::InvalidKey(_)));
        assert_eq!(
            store.get("existing", Some("entries")).await.unwrap(),
            Some(Value::utf8("before"))
        );

        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_POSTGRES_URL"]
    async fn postgres_uses_strict_bytea_schema_and_native_batches() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let table = table_name("batch");
        let store = PostgresStore::from_pool(pool.clone(), Some(&table))
            .await
            .unwrap();

        let keys = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        let values = vec![
            Value::utf8("first"),
            Value::utf8("second"),
            Value::utf8("last"),
        ];
        store
            .put_many(&keys, &values, Some("entries"), Some(60.0))
            .await
            .unwrap();

        let row = sqlx::query(&format!(
            "SELECT entry, expires_at FROM {table} \
             WHERE collection = 'entries' AND key = 'a'"
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        let entry: Vec<u8> = row.try_get("entry").unwrap();
        let expires_at: Option<DateTime<Utc>> = row.try_get("expires_at").unwrap();
        assert_eq!(&entry[..5], b"OKVE1");
        assert!(expires_at.is_some());

        let requested = vec![
            "b".to_string(),
            "missing".to_string(),
            "a".to_string(),
            "b".to_string(),
        ];
        assert_eq!(
            store.get_many(&requested, Some("entries")).await.unwrap(),
            vec![
                Some(Value::utf8("second")),
                None,
                Some(Value::utf8("last")),
                Some(Value::utf8("second")),
            ]
        );
        assert_eq!(
            store
                .delete_many(
                    &["a".to_string(), "a".to_string(), "missing".to_string()],
                    Some("entries")
                )
                .await
                .unwrap(),
            1
        );

        store
            .put("b", Value::utf8("without-ttl"), Some("entries"), None)
            .await
            .unwrap();
        assert_eq!(
            store.ttl("b", Some("entries")).await.unwrap(),
            Some((Value::utf8("without-ttl"), None))
        );
        assert_eq!(
            store.keys(Some("entries"), None).await.unwrap(),
            vec!["b".to_string()]
        );
        assert_eq!(
            store.collections(None).await.unwrap(),
            vec!["entries".to_string()]
        );

        assert!(store.destroy_collection("entries").await.unwrap());
        assert!(!store.destroy_collection("entries").await.unwrap());
        assert!(store.destroy().await.unwrap());
        assert!(!store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_POSTGRES_URL"]
    async fn postgres_rejects_old_schema_and_conflicting_index() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let old_table = table_name("old");
        sqlx::query(&format!(
            "CREATE TABLE {old_table} (\
                collection TEXT NOT NULL,\
                key TEXT NOT NULL,\
                value JSONB NOT NULL,\
                ttl DOUBLE PRECISION,\
                created_at TIMESTAMPTZ,\
                expires_at TIMESTAMPTZ,\
                PRIMARY KEY (collection, key)\
            )"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(matches!(
            PostgresStore::from_pool(pool.clone(), Some(&old_table)).await,
            Err(Error::StoreSetup { .. })
        ));
        sqlx::query(&format!("DROP TABLE {old_table}"))
            .execute(&pool)
            .await
            .unwrap();

        let index_table = table_name("index");
        sqlx::query(&format!(
            "CREATE TABLE {index_table} (\
                collection TEXT NOT NULL,\
                key TEXT NOT NULL,\
                entry BYTEA NOT NULL,\
                expires_at TIMESTAMPTZ,\
                PRIMARY KEY (collection, key)\
            )"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            "CREATE INDEX wrong_{}_expires ON {index_table}(expires_at) \
             WHERE expires_at IS NOT NULL",
            NEXT_TABLE.fetch_add(1, Ordering::Relaxed)
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(matches!(
            PostgresStore::from_pool(pool.clone(), Some(&index_table)).await,
            Err(Error::StoreSetup { .. })
        ));
        sqlx::query(&format!("DROP TABLE {index_table}"))
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_POSTGRES_URL"]
    async fn postgres_cull_and_corrupt_rows_are_strict() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let table = table_name("strict");
        let store = PostgresStore::from_pool(pool.clone(), Some(&table))
            .await
            .unwrap();

        let expired = ManagedEntry {
            value: Value::utf8("expired"),
            created_at: Some(Utc::now() - TimeDelta::seconds(10)),
            expires_at: Some(Utc::now() - TimeDelta::seconds(5)),
        };
        sqlx::query(&format!(
            "INSERT INTO {table} (collection, key, entry, expires_at) \
             VALUES ($1, $2, $3, $4)"
        ))
        .bind("entries")
        .bind("expired")
        .bind(expired.encode())
        .bind(expired.expires_at)
        .execute(&pool)
        .await
        .unwrap();
        store.cull().await.unwrap();
        assert_eq!(store.get("expired", Some("entries")).await.unwrap(), None);

        sqlx::query(&format!(
            "INSERT INTO {table} (collection, key, entry, expires_at) \
             VALUES ('entries', 'legacy', $1, NULL)"
        ))
        .bind(br#"{"value":null}"#.as_slice())
        .execute(&pool)
        .await
        .unwrap();
        assert!(store.get("legacy", Some("entries")).await.is_err());
        assert!(store.keys(Some("entries"), None).await.is_err());
        assert!(store.collections(None).await.is_err());

        sqlx::query(&format!("DELETE FROM {table} WHERE key = 'legacy'"))
            .execute(&pool)
            .await
            .unwrap();
        let mismatch = ManagedEntry::with_ttl(Value::utf8("value"), 60.0).unwrap();
        sqlx::query(&format!(
            "INSERT INTO {table} (collection, key, entry, expires_at) \
             VALUES ($1, $2, $3, $4)"
        ))
        .bind("entries")
        .bind("mismatch")
        .bind(mismatch.encode())
        .bind(mismatch.expires_at.unwrap() + TimeDelta::milliseconds(1))
        .execute(&pool)
        .await
        .unwrap();
        assert!(store.get("mismatch", Some("entries")).await.is_err());

        assert!(store.destroy().await.unwrap());
    }
}
