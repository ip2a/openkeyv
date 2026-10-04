use super::client::PostgresClient;
use super::config::PostgresConfig;
use super::error::{Error, Result};
use crate::change::{
    ChangeFeedRequest, ChangeFilter, ChangeOperation, ChangeStart, ChangeStream, StoreChange,
};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncChangeFeed, AsyncCull, AsyncDestroyCollection, AsyncDestroyStore,
    AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

const DEFAULT_PAGE_SIZE: usize = 10_000;
const PAGE_LIMIT: usize = 10_000;
const CHANGE_CHANNEL: &str = "openkeyv_changefeed";
const CHANGE_DRAIN_LIMIT: i64 = 128;

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
        Self::new_with_config(url, PostgresConfig::new(table_name)?).await
    }

    pub async fn new_with_config(url: &str, config: PostgresConfig) -> Result<Self> {
        let pool = sqlx::PgPool::connect(url)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to connect to Postgres: {error}"),
            })?;
        Self::from_pool_with_config(pool, config).await
    }

    pub async fn from_pool(pool: sqlx::PgPool, table_name: Option<&str>) -> Result<Self> {
        Self::from_pool_with_config(pool, PostgresConfig::new(table_name)?).await
    }

    pub async fn from_pool_with_config(pool: sqlx::PgPool, config: PostgresConfig) -> Result<Self> {
        let store = Self::with_config(pool, config);
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

    fn changes_table_name(&self) -> String {
        let plain = format!("{}_changes", self.config.table_name);
        if plain.len() <= 63 {
            plain
        } else {
            let hash = blake3::hash(self.config.table_name.as_bytes()).to_hex();
            format!("changes_{}", &hash[..47])
        }
    }

    /// Append change rows for `keys` inside the caller's transaction, trim the
    /// log to the configured retention, and wake subscribers once.
    async fn record_changes(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        collection: &str,
        keys: &[&str],
        operation: &str,
    ) -> Result<()> {
        if self.config.change_retention == 0 || keys.is_empty() {
            return Ok(());
        }
        let changes_table = self.changes_table_name();
        sqlx::query(&format!(
            "INSERT INTO {changes_table} (collection, key, operation, occurred_at) \
             SELECT $1, k, $3, now() FROM UNNEST($2::text[]) AS k",
        ))
        .bind(collection)
        .bind(keys)
        .bind(operation)
        .execute(&mut **transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to record Postgres change rows: {error}"),
        })?;
        // ponytail: per-write trim adds one subquery per write; move to a
        // periodic trim if high write volume makes it measurable.
        sqlx::query(&format!(
            "DELETE FROM {changes_table} WHERE revision <= \
             (SELECT COALESCE(MAX(revision), 0) - $1 FROM {changes_table})",
        ))
        .bind(self.config.change_retention as i64)
        .execute(&mut **transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to trim Postgres change log: {error}"),
        })?;
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(CHANGE_CHANNEL)
            .execute(&mut **transaction)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to notify Postgres change feed: {error}"),
            })?;
        Ok(())
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

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                revision BIGSERIAL PRIMARY KEY,\
                collection TEXT NOT NULL,\
                key TEXT NOT NULL,\
                operation TEXT NOT NULL,\
                occurred_at TIMESTAMPTZ NOT NULL\
            )",
            self.changes_table_name()
        ))
        .execute(&mut *transaction)
        .await
        .map_err(|error| Error::StoreSetup {
            message: format!(
                "failed to create Postgres change table for {}: {error}",
                self.config.table_name
            ),
        })?;

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
        let mut transaction =
            self.pool()
                .begin()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to start Postgres put transaction: {error}"),
                })?;
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
        .execute(&mut *transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to put Postgres key {key}: {error}"),
        })?;
        self.record_changes(&mut transaction, collection, &[key], "put")
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to commit Postgres put for key {key}: {error}"),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let collection = self.collection_name(collection)?;
        Self::validate_text_identity("key", key)?;
        let mut transaction =
            self.pool()
                .begin()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to start Postgres delete transaction: {error}"),
                })?;
        let rows = sqlx::query(&format!(
            "DELETE FROM {} WHERE collection = $1 AND key = $2 RETURNING key",
            self.config.table_name
        ))
        .bind(collection)
        .bind(key)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to delete Postgres key {key}: {error}"),
        })?;
        let deleted = !rows.is_empty();
        if deleted {
            self.record_changes(&mut transaction, collection, &[key], "delete")
                .await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to commit Postgres delete for key {key}: {error}"),
            })?;
        Ok(deleted)
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

        let mut transaction =
            self.pool()
                .begin()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to start Postgres batch put transaction: {error}"),
                })?;
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
        .execute(&mut *transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to put Postgres batch: {error}"),
        })?;
        self.record_changes(&mut transaction, collection, &final_keys, "put")
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to commit Postgres batch put: {error}"),
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
        let mut transaction =
            self.pool()
                .begin()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to start Postgres batch delete transaction: {error}"),
                })?;
        let rows = sqlx::query(&format!(
            "DELETE FROM {} WHERE collection = $1 AND key = ANY($2) RETURNING key",
            self.config.table_name
        ))
        .bind(collection)
        .bind(keys)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to delete Postgres batch: {error}"),
        })?;
        let deleted_keys: Vec<String> = rows
            .iter()
            .map(|row| {
                row.try_get::<String, _>("key")
                    .map_err(|error| Error::Deserialization(error.to_string()))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if !deleted_keys.is_empty() {
            let key_refs: Vec<&str> = deleted_keys.iter().map(String::as_str).collect();
            self.record_changes(&mut transaction, collection, &key_refs, "delete")
                .await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to commit Postgres batch delete: {error}"),
            })?;
        Ok(deleted_keys.len())
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
        let mut transaction =
            self.pool()
                .begin()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to start Postgres collection destroy transaction: {error}"
                    ),
                })?;
        let rows = sqlx::query(&format!(
            "DELETE FROM {} WHERE collection = $1 RETURNING key",
            self.config.table_name
        ))
        .bind(collection)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to destroy Postgres collection {collection}: {error}"),
        })?;
        let destroyed = !rows.is_empty();
        if destroyed {
            let keys: Vec<String> = rows
                .iter()
                .map(|row| {
                    row.try_get::<String, _>("key")
                        .map_err(|error| Error::Deserialization(error.to_string()))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            self.record_changes(&mut transaction, collection, &key_refs, "delete")
                .await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to commit Postgres collection destroy for {collection}: {error}"
                ),
            })?;
        Ok(destroyed)
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
        // The change log dies with the store; it is never notified.
        sqlx::query(&format!("DROP TABLE {}", self.changes_table_name()))
            .execute(&mut *transaction)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to destroy Postgres change table for {}: {error}",
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

async fn max_revision(pool: &sqlx::PgPool, changes_table: &str) -> Result<i64> {
    sqlx::query_scalar(&format!(
        "SELECT COALESCE(MAX(revision), 0) FROM {changes_table}"
    ))
    .fetch_one(pool)
    .await
    .map_err(|error| Error::StoreConnection {
        message: format!("failed to read Postgres change log head: {error}"),
    })
}

async fn min_retained_revision(pool: &sqlx::PgPool, changes_table: &str) -> Result<Option<i64>> {
    sqlx::query_scalar(&format!("SELECT MIN(revision) FROM {changes_table}"))
        .fetch_one(pool)
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to read Postgres change log tail: {error}"),
        })
}

struct PostgresChangeStream {
    listener: sqlx::postgres::PgListener,
    pool: sqlx::PgPool,
    changes_table: String,
    /// Last delivered revision; 0 means nothing has been delivered yet.
    cursor: u64,
    filter: ChangeFilter,
}

#[async_trait]
impl ChangeStream for PostgresChangeStream {
    async fn recv(&mut self) -> Result<Option<StoreChange>> {
        loop {
            let rows = sqlx::query(&format!(
                "SELECT revision, collection, key, operation, occurred_at FROM {0} \
                 WHERE revision > $1 ORDER BY revision LIMIT $2",
                self.changes_table
            ))
            .bind(self.cursor as i64)
            .bind(CHANGE_DRAIN_LIMIT)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to read Postgres change rows: {error}"),
            })?;
            for row in rows {
                let revision = row
                    .try_get::<i64, _>("revision")
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                let collection = row
                    .try_get::<String, _>("collection")
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                let key = row
                    .try_get::<String, _>("key")
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                let operation = match row
                    .try_get::<String, _>("operation")
                    .map_err(|error| Error::Deserialization(error.to_string()))?
                    .as_str()
                {
                    "put" => ChangeOperation::Put,
                    "delete" => ChangeOperation::Delete,
                    _ => return Err(Error::CorruptedData),
                };
                let occurred_at = row
                    .try_get::<DateTime<Utc>, _>("occurred_at")
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                self.cursor = revision as u64;
                let change = StoreChange {
                    cursor: crate::change::ChangeCursor::new(revision.to_string()),
                    revision: revision as u64,
                    collection,
                    key,
                    operation,
                    occurred_at,
                };
                if self.filter.matches(&change) {
                    return Ok(Some(change));
                }
            }

            // BIGSERIAL burns ids on rolled-back transactions, so a bare
            // revision gap is not expiry — only missing retained rows are.
            if self.cursor > 0 {
                if let Some(min) = min_retained_revision(&self.pool, &self.changes_table).await? {
                    if min as u64 > self.cursor + 1 {
                        return Err(Error::ChangeCursorExpired {
                            requested: self.cursor.to_string(),
                            oldest: min.to_string(),
                        });
                    }
                }
            }

            // The listener may silently reconnect and miss notifications, so
            // wake up periodically even without a signal.
            tokio::select! {
                notification = self.listener.recv() => {
                    notification.map_err(|error| Error::StoreConnection {
                        message: format!("Postgres change listener failed: {error}"),
                    })?;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        }
    }
}

#[async_trait]
impl AsyncChangeFeed for PostgresStore {
    async fn subscribe(&self, request: ChangeFeedRequest) -> Result<Box<dyn ChangeStream + Send>> {
        if self.config.change_retention == 0 {
            return Err(Error::InvalidOperation(
                "change feed is disabled (change_retention = 0)".to_string(),
            ));
        }
        let changes_table = self.changes_table_name();
        let mut listener = sqlx::postgres::PgListener::connect_with(self.pool())
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to open Postgres change listener: {error}"),
            })?;
        listener
            .listen(CHANGE_CHANNEL)
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to listen on Postgres change channel: {error}"),
            })?;

        let cursor = match &request.start {
            ChangeStart::Beginning => 0,
            ChangeStart::Latest => max_revision(self.pool(), &changes_table).await? as u64,
            ChangeStart::After(cursor) => {
                let requested = cursor
                    .as_str()
                    .parse::<u64>()
                    .map_err(|_| Error::InvalidChangeCursor(cursor.to_string()))?;
                let max = max_revision(self.pool(), &changes_table).await?;
                if requested > max as u64 {
                    return Err(Error::InvalidChangeCursor(cursor.to_string()));
                }
                if requested > 0 {
                    if let Some(min) = min_retained_revision(self.pool(), &changes_table).await? {
                        if min as u64 > requested + 1 {
                            return Err(Error::ChangeCursorExpired {
                                requested: cursor.to_string(),
                                oldest: min.to_string(),
                            });
                        }
                    }
                }
                requested
            }
        };
        Ok(Box::new(PostgresChangeStream {
            listener,
            pool: self.pool().clone(),
            changes_table,
            cursor,
            filter: request.filter,
        }))
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
    async fn postgres_change_feed_delivers_and_resumes_across_instances() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let table = table_name("changefeed");
        let config = PostgresConfig::new(Some(&table)).unwrap();
        let writer = PostgresStore::from_pool_with_config(pool.clone(), config.clone())
            .await
            .unwrap();
        let reader = PostgresStore::from_pool_with_config(pool.clone(), config)
            .await
            .unwrap();
        let collection = "entries";

        let mut live = reader
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::Latest,
                filter: ChangeFilter::collection(collection),
            })
            .await
            .unwrap();

        writer
            .put("event-1", Value::integer(1), Some(collection), None)
            .await
            .unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), live.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.collection, collection);
        assert_eq!(first.key, "event-1");
        assert_eq!(first.operation, ChangeOperation::Put);
        assert_eq!(
            reader.get("event-1", Some(collection)).await.unwrap(),
            Some(Value::integer(1))
        );

        writer
            .put("event-2", Value::integer(2), Some(collection), None)
            .await
            .unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(5), live.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second.key, "event-2");
        assert!(second.revision > first.revision);

        let mut resumed = reader
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::After(first.cursor),
                filter: ChangeFilter::collection(collection),
            })
            .await
            .unwrap();
        let replayed = tokio::time::timeout(std::time::Duration::from_secs(5), resumed.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replayed.cursor, second.cursor);

        assert!(writer.delete("event-2", Some(collection)).await.unwrap());
        let deleted = tokio::time::timeout(std::time::Duration::from_secs(5), resumed.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(deleted.key, "event-2");
        assert_eq!(deleted.operation, ChangeOperation::Delete);

        assert!(writer.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_POSTGRES_URL"]
    async fn postgres_destroy_collection_emits_delete_changes() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let table = table_name("destroyfeed");
        let store = PostgresStore::from_pool(pool.clone(), Some(&table))
            .await
            .unwrap();
        for key in ["a", "b", "c"] {
            store
                .put(key, Value::null(), Some("entries"), None)
                .await
                .unwrap();
        }
        let mut changes = store
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::Beginning,
                filter: ChangeFilter::collection("entries"),
            })
            .await
            .unwrap();

        for key in ["a", "b", "c"] {
            let change = tokio::time::timeout(std::time::Duration::from_secs(5), changes.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(change.key, key);
            assert_eq!(change.operation, ChangeOperation::Put);
        }

        assert!(store.destroy_collection("entries").await.unwrap());
        let mut deleted = Vec::new();
        for _ in 0..3 {
            let change = tokio::time::timeout(std::time::Duration::from_secs(5), changes.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(change.operation, ChangeOperation::Delete);
            deleted.push(change.key);
        }
        deleted.sort();
        assert_eq!(deleted, vec!["a", "b", "c"]);
        assert!(!store.destroy_collection("entries").await.unwrap());

        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_POSTGRES_URL"]
    async fn postgres_change_feed_reports_trimmed_cursor() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let table = table_name("trimfeed");
        let store = PostgresStore::from_pool_with_config(
            pool.clone(),
            PostgresConfig::new(Some(&table))
                .unwrap()
                .with_change_retention(3),
        )
        .await
        .unwrap();

        for index in 0..5 {
            store
                .put(
                    &format!("k{index}"),
                    Value::integer(index),
                    Some("entries"),
                    None,
                )
                .await
                .unwrap();
        }
        let mut replay = store
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::Beginning,
                filter: ChangeFilter::collection("entries"),
            })
            .await
            .unwrap();
        let first = replay.recv().await.unwrap().unwrap();
        assert_eq!(first.key, "k0");

        // Retention 3 keeps k2..k4, so resuming after k0 must report expiry.
        let result = store
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::After(first.cursor),
                filter: ChangeFilter::default(),
            })
            .await;
        assert!(matches!(result, Err(Error::ChangeCursorExpired { .. })));

        // Resuming from a retained cursor still works.
        let mut live = store
            .subscribe(ChangeFeedRequest {
                start: ChangeStart::Latest,
                filter: ChangeFilter::collection("entries"),
            })
            .await
            .unwrap();
        store
            .put("k5", Value::integer(5), Some("entries"), None)
            .await
            .unwrap();
        let change = tokio::time::timeout(std::time::Duration::from_secs(5), live.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(change.key, "k5");

        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_POSTGRES_URL"]
    async fn postgres_change_retention_zero_disables_recording() {
        let pool = sqlx::PgPool::connect(&integration_url()).await.unwrap();
        let table = table_name("zerofeed");
        let store = PostgresStore::from_pool_with_config(
            pool.clone(),
            PostgresConfig::new(Some(&table))
                .unwrap()
                .with_change_retention(0),
        )
        .await
        .unwrap();

        store
            .put("k", Value::null(), Some("entries"), None)
            .await
            .unwrap();
        store.delete("k", Some("entries")).await.unwrap();

        let recorded: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}_changes"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(recorded, 0);

        let error = store
            .subscribe(ChangeFeedRequest::default())
            .await
            .err()
            .expect("subscribe must fail when change_retention is 0");
        assert!(error.to_string().contains("disabled"));

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
