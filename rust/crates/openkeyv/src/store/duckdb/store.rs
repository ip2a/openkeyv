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
use bytes::Bytes;
use chrono::{DateTime, Utc};
use duckdb::{params, params_from_iter, types::Value as DuckValue};
use std::collections::{HashMap, HashSet};

const DEFAULT_PAGE_SIZE: usize = 10_000;
const PAGE_LIMIT: usize = 10_000;

struct StoredRow {
    collection: String,
    key: String,
    raw_entry: Bytes,
    expires_at: Option<DateTime<Utc>>,
}

/// DuckDB-backed key-value store.
///
/// Each row stores a collection, key, complete binary `OKVE1` entry, and an
/// optional indexed expiration timestamp mirrored from the entry metadata.
pub struct DuckDBStore {
    client: DuckDBClient,
    config: DuckDBConfig,
}

impl DuckDBStore {
    pub async fn new(path: Option<&str>, table_name: Option<&str>) -> Result<Self> {
        let connection = match path {
            None | Some(":memory:") => duckdb::Connection::open_in_memory(),
            Some(path) => duckdb::Connection::open(path),
        }
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to open DuckDB: {error}"),
        })?;
        Self::from_conn(connection, table_name).await
    }

    pub async fn from_conn(
        connection: duckdb::Connection,
        table_name: Option<&str>,
    ) -> Result<Self> {
        let store = Self::with_config(connection, DuckDBConfig::new(table_name)?);
        store.ensure_table().await?;
        Ok(store)
    }

    pub fn with_config(connection: duckdb::Connection, config: DuckDBConfig) -> Self {
        Self {
            client: DuckDBClient::new(connection),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn conn(&self) -> &tokio::sync::Mutex<duckdb::Connection> {
        self.client.conn()
    }

    fn expires_index_name(&self) -> String {
        format!("idx_{}_expires_at", self.config.table_name)
    }

    async fn ensure_table(&self) -> Result<()> {
        let mut connection = self.conn().lock().await;
        let table_exists: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = current_schema() AND table_name = ?1",
                [&self.config.table_name],
                |row| row.get(0),
            )
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to inspect DuckDB table {}: {error}",
                    self.config.table_name
                ),
            })?;

        if table_exists == 0 {
            let transaction = connection
                .transaction()
                .map_err(|error| Error::StoreSetup {
                    message: format!("failed to start DuckDB setup transaction: {error}"),
                })?;
            transaction
                .execute(
                    &format!(
                        "CREATE TABLE {} (\
                            collection VARCHAR NOT NULL,\
                            key VARCHAR NOT NULL,\
                            entry BLOB NOT NULL,\
                            expires_at TIMESTAMPTZ,\
                            PRIMARY KEY (collection, key)\
                        )",
                        self.config.table_name
                    ),
                    [],
                )
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to create DuckDB table {}: {error}",
                        self.config.table_name
                    ),
                })?;
            transaction
                .execute(
                    &format!(
                        "CREATE INDEX {} ON {}(expires_at)",
                        self.expires_index_name(),
                        self.config.table_name
                    ),
                    [],
                )
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to create DuckDB expiration index for {}: {error}",
                        self.config.table_name
                    ),
                })?;
            transaction.commit().map_err(|error| Error::StoreSetup {
                message: format!("failed to commit DuckDB setup transaction: {error}"),
            })?;
        } else if table_exists != 1 {
            return Err(Error::StoreSetup {
                message: format!(
                    "DuckDB schema contains multiple tables named {}",
                    self.config.table_name
                ),
            });
        }

        let table_info_sql = format!("PRAGMA table_info('{}')", self.config.table_name);
        let mut statement =
            connection
                .prepare(&table_info_sql)
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to prepare DuckDB schema inspection for {}: {error}",
                        self.config.table_name
                    ),
                })?;
        let mut rows = statement.query([]).map_err(|error| Error::StoreSetup {
            message: format!(
                "failed to inspect DuckDB schema for {}: {error}",
                self.config.table_name
            ),
        })?;
        let mut columns = Vec::new();
        while let Some(row) = rows.next().map_err(|error| Error::StoreSetup {
            message: format!(
                "failed to read DuckDB schema for {}: {error}",
                self.config.table_name
            ),
        })? {
            columns.push((
                row.get::<_, i64>(0).map_err(|error| Error::StoreSetup {
                    message: format!("invalid DuckDB column ordinal: {error}"),
                })?,
                row.get::<_, String>(1).map_err(|error| Error::StoreSetup {
                    message: format!("invalid DuckDB column name: {error}"),
                })?,
                row.get::<_, String>(2).map_err(|error| Error::StoreSetup {
                    message: format!("invalid DuckDB column type: {error}"),
                })?,
                row.get::<_, bool>(3).map_err(|error| Error::StoreSetup {
                    message: format!("invalid DuckDB nullability metadata: {error}"),
                })?,
                row.get::<_, Option<String>>(4)
                    .map_err(|error| Error::StoreSetup {
                        message: format!("invalid DuckDB default metadata: {error}"),
                    })?,
                row.get::<_, bool>(5).map_err(|error| Error::StoreSetup {
                    message: format!("invalid DuckDB primary-key metadata: {error}"),
                })?,
            ));
        }
        drop(rows);
        drop(statement);

        let expected_columns = vec![
            (
                0,
                "collection".to_string(),
                "VARCHAR".to_string(),
                true,
                None,
                true,
            ),
            (
                1,
                "key".to_string(),
                "VARCHAR".to_string(),
                true,
                None,
                true,
            ),
            (
                2,
                "entry".to_string(),
                "BLOB".to_string(),
                true,
                None,
                false,
            ),
            (
                3,
                "expires_at".to_string(),
                "TIMESTAMP WITH TIME ZONE".to_string(),
                false,
                None,
                false,
            ),
        ];
        if columns != expected_columns {
            return Err(Error::StoreSetup {
                message: format!(
                    "DuckDB table {} does not match the required OpenKeyV schema",
                    self.config.table_name
                ),
            });
        }

        let index_name = self.expires_index_name();
        let index_rows = {
            let mut statement = connection
                .prepare(
                    "SELECT index_name, is_unique, is_primary, expressions \
                     FROM duckdb_indexes() \
                     WHERE schema_name = current_schema() AND table_name = ?1 \
                     AND (index_name = ?2 OR expressions = '[expires_at]')",
                )
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to prepare DuckDB index inspection for {}: {error}",
                        self.config.table_name
                    ),
                })?;
            let mut rows = statement
                .query(params![&self.config.table_name, &index_name])
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to inspect DuckDB indexes for {}: {error}",
                        self.config.table_name
                    ),
                })?;
            let mut indexes = Vec::new();
            while let Some(row) = rows.next().map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to read DuckDB index metadata for {}: {error}",
                    self.config.table_name
                ),
            })? {
                indexes.push((
                    row.get::<_, String>(0).map_err(|error| Error::StoreSetup {
                        message: format!("invalid DuckDB index name: {error}"),
                    })?,
                    row.get::<_, bool>(1).map_err(|error| Error::StoreSetup {
                        message: format!("invalid DuckDB unique-index metadata: {error}"),
                    })?,
                    row.get::<_, bool>(2).map_err(|error| Error::StoreSetup {
                        message: format!("invalid DuckDB primary-index metadata: {error}"),
                    })?,
                    row.get::<_, String>(3).map_err(|error| Error::StoreSetup {
                        message: format!("invalid DuckDB index expression: {error}"),
                    })?,
                ));
            }
            indexes
        };

        if index_rows.is_empty() {
            connection
                .execute(
                    &format!(
                        "CREATE INDEX {} ON {}(expires_at)",
                        index_name, self.config.table_name
                    ),
                    [],
                )
                .map_err(|error| Error::StoreSetup {
                    message: format!(
                        "failed to create DuckDB expiration index for {}: {error}",
                        self.config.table_name
                    ),
                })?;
        } else if index_rows != [(index_name.clone(), false, false, "[expires_at]".to_string())] {
            return Err(Error::StoreSetup {
                message: format!(
                    "DuckDB table {} has an invalid or duplicate expiration index",
                    self.config.table_name
                ),
            });
        }

        Ok(())
    }

    fn decode_entry(
        key: &str,
        raw_entry: Bytes,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<ManagedEntry> {
        let entry = ManagedEntry::decode(raw_entry).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode DuckDB OKVE1 entry for key {key}: {error}"
            ))
        })?;
        let embedded_expires_at = entry
            .expires_at
            .map(|expires_at| expires_at.timestamp_millis());
        let indexed_expires_at = expires_at.map(|expires_at| expires_at.timestamp_millis());
        if embedded_expires_at != indexed_expires_at {
            return Err(Error::Deserialization(format!(
                "DuckDB expires_at does not match OKVE1 metadata for key {key}"
            )));
        }
        Ok(entry)
    }
}

#[async_trait]
impl AsyncKeyValue for DuckDBStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let collection = self.collection_name(collection);
        let sql = format!(
            "SELECT entry, expires_at FROM {} WHERE collection = ?1 AND key = ?2",
            self.config.table_name
        );
        let connection = self.conn().lock().await;
        let stored = {
            let mut statement =
                connection
                    .prepare(&sql)
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to prepare DuckDB get for key {key}: {error}"),
                    })?;
            let mut rows = statement.query(params![collection, key]).map_err(|error| {
                Error::StoreConnection {
                    message: format!("failed to query DuckDB key {key}: {error}"),
                }
            })?;
            match rows.next().map_err(|error| Error::StoreConnection {
                message: format!("failed to read DuckDB key {key}: {error}"),
            })? {
                Some(row) => Some((
                    row.get::<_, Vec<u8>>(0)
                        .map_err(|error| Error::Deserialization(error.to_string()))?,
                    row.get::<_, Option<DateTime<Utc>>>(1)
                        .map_err(|error| Error::Deserialization(error.to_string()))?,
                )),
                None => None,
            }
        };
        let Some((raw_entry, expires_at)) = stored else {
            return Ok(None);
        };
        let raw_entry = Bytes::from(raw_entry);
        let entry = Self::decode_entry(key, raw_entry.clone(), expires_at)?;
        if entry.is_expired() {
            connection
                .execute(
                    &format!(
                        "DELETE FROM {} WHERE collection = ?1 AND key = ?2 \
                         AND entry = ?3 AND expires_at IS NOT DISTINCT FROM ?4",
                        self.config.table_name
                    ),
                    params![collection, key, raw_entry.as_ref(), expires_at],
                )
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to conditionally delete expired DuckDB key {key}: {error}"
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
        let collection = self.collection_name(collection);
        let sql = format!(
            "SELECT entry, expires_at FROM {} WHERE collection = ?1 AND key = ?2",
            self.config.table_name
        );
        let connection = self.conn().lock().await;
        let stored = {
            let mut statement =
                connection
                    .prepare(&sql)
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to prepare DuckDB TTL for key {key}: {error}"),
                    })?;
            let mut rows = statement.query(params![collection, key]).map_err(|error| {
                Error::StoreConnection {
                    message: format!("failed to query DuckDB TTL for key {key}: {error}"),
                }
            })?;
            match rows.next().map_err(|error| Error::StoreConnection {
                message: format!("failed to read DuckDB TTL for key {key}: {error}"),
            })? {
                Some(row) => Some((
                    row.get::<_, Vec<u8>>(0)
                        .map_err(|error| Error::Deserialization(error.to_string()))?,
                    row.get::<_, Option<DateTime<Utc>>>(1)
                        .map_err(|error| Error::Deserialization(error.to_string()))?,
                )),
                None => None,
            }
        };
        let Some((raw_entry, expires_at)) = stored else {
            return Ok(None);
        };
        let raw_entry = Bytes::from(raw_entry);
        let entry = Self::decode_entry(key, raw_entry.clone(), expires_at)?;
        if entry.is_expired() {
            connection
                .execute(
                    &format!(
                        "DELETE FROM {} WHERE collection = ?1 AND key = ?2 \
                         AND entry = ?3 AND expires_at IS NOT DISTINCT FROM ?4",
                        self.config.table_name
                    ),
                    params![collection, key, raw_entry.as_ref(), expires_at],
                )
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to conditionally delete expired DuckDB key {key}: {error}"
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
        let collection = self.collection_name(collection);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        self.conn()
            .lock()
            .await
            .execute(
                &format!(
                    "INSERT INTO {} (collection, key, entry, expires_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT (collection, key) DO UPDATE SET \
                     entry = excluded.entry, expires_at = excluded.expires_at",
                    self.config.table_name
                ),
                params![collection, key, entry.encode(), entry.expires_at],
            )
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to put DuckDB key {key}: {error}"),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let collection = self.collection_name(collection);
        let affected = self
            .conn()
            .lock()
            .await
            .execute(
                &format!(
                    "DELETE FROM {} WHERE collection = ?1 AND key = ?2",
                    self.config.table_name
                ),
                params![collection, key],
            )
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to delete DuckDB key {key}: {error}"),
            })?;
        Ok(affected == 1)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let collection = self.collection_name(collection);
        let requested = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let placeholders = std::iter::repeat_n("?", requested.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT key, entry, expires_at FROM {} \
             WHERE collection = ? AND key IN ({placeholders})",
            self.config.table_name
        );
        let mut parameters = Vec::with_capacity(requested.len() + 1);
        parameters.push(DuckValue::Text(collection.to_string()));
        parameters.extend(
            requested
                .iter()
                .map(|key| DuckValue::Text((*key).to_string())),
        );

        let mut connection = self.conn().lock().await;
        let (values, expired) = {
            let mut statement =
                connection
                    .prepare(&sql)
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to prepare DuckDB batch get: {error}"),
                    })?;
            let mut rows = statement
                .query(params_from_iter(parameters.iter()))
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to query DuckDB batch get: {error}"),
                })?;
            let mut values = HashMap::with_capacity(requested.len());
            let mut expired = Vec::new();
            while let Some(row) = rows.next().map_err(|error| Error::StoreConnection {
                message: format!("failed to read DuckDB batch get row: {error}"),
            })? {
                let key: String = row
                    .get(0)
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                if !requested.contains(key.as_str()) {
                    return Err(Error::Deserialization(format!(
                        "DuckDB batch query returned unrequested key {key}"
                    )));
                }
                let raw_entry = Bytes::from(
                    row.get::<_, Vec<u8>>(1)
                        .map_err(|error| Error::Deserialization(error.to_string()))?,
                );
                let expires_at: Option<DateTime<Utc>> = row
                    .get(2)
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
                        "DuckDB batch query returned duplicate key {key}"
                    )));
                }
            }
            (values, expired)
        };

        if !expired.is_empty() {
            let transaction = connection
                .transaction()
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to start DuckDB expired cleanup transaction: {error}"),
                })?;
            {
                let mut statement = transaction
                    .prepare(&format!(
                        "DELETE FROM {} WHERE collection = ?1 AND key = ?2 \
                         AND entry = ?3 AND expires_at IS NOT DISTINCT FROM ?4",
                        self.config.table_name
                    ))
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to prepare DuckDB expired cleanup batch: {error}"),
                    })?;
                for row in expired {
                    statement
                        .execute(params![
                            row.collection,
                            row.key,
                            row.raw_entry.as_ref(),
                            row.expires_at
                        ])
                        .map_err(|error| Error::StoreConnection {
                            message: format!(
                                "failed to conditionally delete expired DuckDB row: {error}"
                            ),
                        })?;
                }
            }
            transaction
                .commit()
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to commit DuckDB expired cleanup: {error}"),
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
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let collection = self.collection_name(collection);
        let requested = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let placeholders = std::iter::repeat_n("?", requested.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT key, entry, expires_at FROM {} \
             WHERE collection = ? AND key IN ({placeholders})",
            self.config.table_name
        );
        let mut parameters = Vec::with_capacity(requested.len() + 1);
        parameters.push(DuckValue::Text(collection.to_string()));
        parameters.extend(
            requested
                .iter()
                .map(|key| DuckValue::Text((*key).to_string())),
        );

        let mut connection = self.conn().lock().await;
        let (values, expired) = {
            let mut statement =
                connection
                    .prepare(&sql)
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to prepare DuckDB TTL batch: {error}"),
                    })?;
            let mut rows = statement
                .query(params_from_iter(parameters.iter()))
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to query DuckDB TTL batch: {error}"),
                })?;
            let mut values = HashMap::with_capacity(requested.len());
            let mut expired = Vec::new();
            while let Some(row) = rows.next().map_err(|error| Error::StoreConnection {
                message: format!("failed to read DuckDB TTL batch row: {error}"),
            })? {
                let key: String = row
                    .get(0)
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                if !requested.contains(key.as_str()) {
                    return Err(Error::Deserialization(format!(
                        "DuckDB TTL batch returned unrequested key {key}"
                    )));
                }
                let raw_entry = Bytes::from(
                    row.get::<_, Vec<u8>>(1)
                        .map_err(|error| Error::Deserialization(error.to_string()))?,
                );
                let expires_at: Option<DateTime<Utc>> = row
                    .get(2)
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
                        "DuckDB TTL batch returned duplicate key {key}"
                    )));
                }
            }
            (values, expired)
        };

        if !expired.is_empty() {
            let transaction = connection
                .transaction()
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to start DuckDB TTL cleanup transaction: {error}"),
                })?;
            {
                let mut statement = transaction
                    .prepare(&format!(
                        "DELETE FROM {} WHERE collection = ?1 AND key = ?2 \
                         AND entry = ?3 AND expires_at IS NOT DISTINCT FROM ?4",
                        self.config.table_name
                    ))
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to prepare DuckDB TTL cleanup batch: {error}"),
                    })?;
                for row in expired {
                    statement
                        .execute(params![
                            row.collection,
                            row.key,
                            row.raw_entry.as_ref(),
                            row.expires_at
                        ])
                        .map_err(|error| Error::StoreConnection {
                            message: format!(
                                "failed to conditionally delete expired DuckDB TTL row: {error}"
                            ),
                        })?;
                }
            }
            transaction
                .commit()
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to commit DuckDB TTL cleanup: {error}"),
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
        if keys.is_empty() {
            return Ok(());
        }

        let collection = self.collection_name(collection);
        let mut last_indices = HashMap::with_capacity(keys.len());
        for (index, key) in keys.iter().enumerate() {
            last_indices.insert(key.as_str(), index);
        }

        let mut connection = self.conn().lock().await;
        let transaction = connection
            .transaction()
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to start DuckDB batch write transaction: {error}"),
            })?;
        {
            let mut statement = transaction
                .prepare(&format!(
                    "INSERT INTO {} (collection, key, entry, expires_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT (collection, key) DO UPDATE SET \
                     entry = excluded.entry, expires_at = excluded.expires_at",
                    self.config.table_name
                ))
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to prepare DuckDB batch write: {error}"),
                })?;
            for index in last_indices.into_values() {
                let entry = match ttl {
                    Some(seconds) => ManagedEntry::with_ttl(values[index].clone(), seconds)?,
                    None => ManagedEntry::new(values[index].clone()),
                };
                statement
                    .execute(params![
                        collection,
                        &keys[index],
                        entry.encode(),
                        entry.expires_at
                    ])
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to write DuckDB batch key {}: {error}",
                            keys[index]
                        ),
                    })?;
            }
        }
        transaction
            .commit()
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to commit DuckDB batch write: {error}"),
            })?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let collection = self.collection_name(collection);
        let unique = keys.iter().map(String::as_str).collect::<HashSet<_>>();
        let placeholders = std::iter::repeat_n("?", unique.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "DELETE FROM {} WHERE collection = ? AND key IN ({placeholders})",
            self.config.table_name
        );
        let mut parameters = Vec::with_capacity(unique.len() + 1);
        parameters.push(DuckValue::Text(collection.to_string()));
        parameters.extend(unique.iter().map(|key| DuckValue::Text((*key).to_string())));
        self.conn()
            .lock()
            .await
            .execute(&sql, params_from_iter(parameters.iter()))
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to delete DuckDB batch: {error}"),
            })
    }
}

#[async_trait]
impl AsyncCull for DuckDBStore {
    async fn cull(&self) -> Result<()> {
        let mut connection = self.conn().lock().await;
        let expired = {
            let mut statement = connection
                .prepare(&format!(
                    "SELECT collection, key, entry, expires_at FROM {} \
                     WHERE expires_at IS NOT NULL AND expires_at <= now()",
                    self.config.table_name
                ))
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to prepare DuckDB cull: {error}"),
                })?;
            let mut rows = statement
                .query([])
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to query DuckDB cull: {error}"),
                })?;
            let mut expired = Vec::new();
            while let Some(row) = rows.next().map_err(|error| Error::StoreConnection {
                message: format!("failed to read DuckDB cull row: {error}"),
            })? {
                let collection: String = row
                    .get(0)
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                let key: String = row
                    .get(1)
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                let raw_entry = Bytes::from(
                    row.get::<_, Vec<u8>>(2)
                        .map_err(|error| Error::Deserialization(error.to_string()))?,
                );
                let expires_at: Option<DateTime<Utc>> = row
                    .get(3)
                    .map_err(|error| Error::Deserialization(error.to_string()))?;
                let entry = Self::decode_entry(&key, raw_entry.clone(), expires_at)?;
                if !entry.is_expired() {
                    return Err(Error::Deserialization(format!(
                        "DuckDB expiration query returned live key {key}"
                    )));
                }
                expired.push(StoredRow {
                    collection,
                    key,
                    raw_entry,
                    expires_at,
                });
            }
            expired
        };

        if expired.is_empty() {
            return Ok(());
        }

        let transaction = connection
            .transaction()
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to start DuckDB cull transaction: {error}"),
            })?;
        {
            let mut statement = transaction
                .prepare(&format!(
                    "DELETE FROM {} WHERE collection = ?1 AND key = ?2 \
                     AND entry = ?3 AND expires_at IS NOT DISTINCT FROM ?4",
                    self.config.table_name
                ))
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to prepare DuckDB cull delete: {error}"),
                })?;
            for row in expired {
                statement
                    .execute(params![
                        row.collection,
                        row.key,
                        row.raw_entry.as_ref(),
                        row.expires_at
                    ])
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to conditionally delete expired DuckDB row: {error}"
                        ),
                    })?;
            }
        }
        transaction
            .commit()
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to commit DuckDB cull: {error}"),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for DuckDBStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let collection = self.collection_name(collection);
        let connection = self.conn().lock().await;
        let mut statement = connection
            .prepare(&format!(
                "SELECT key, entry, expires_at FROM {} \
                 WHERE collection = ?1 \
                 AND (expires_at IS NULL OR expires_at > now()) \
                 ORDER BY key LIMIT ?2",
                self.config.table_name
            ))
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to prepare DuckDB key enumeration: {error}"),
            })?;
        let mut rows = statement
            .query(params![collection, limit as i64])
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to query DuckDB keys: {error}"),
            })?;
        let mut keys = Vec::with_capacity(limit);
        while let Some(row) = rows.next().map_err(|error| Error::StoreConnection {
            message: format!("failed to read DuckDB key row: {error}"),
        })? {
            let key: String = row
                .get(0)
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let raw_entry = Bytes::from(
                row.get::<_, Vec<u8>>(1)
                    .map_err(|error| Error::Deserialization(error.to_string()))?,
            );
            let expires_at: Option<DateTime<Utc>> = row
                .get(2)
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let entry = Self::decode_entry(&key, raw_entry, expires_at)?;
            if entry.is_expired() {
                return Err(Error::Deserialization(format!(
                    "DuckDB key enumeration returned expired key {key}"
                )));
            }
            keys.push(key);
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for DuckDBStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let connection = self.conn().lock().await;
        let mut statement = connection
            .prepare(&format!(
                "SELECT collection, key, entry, expires_at FROM {} \
                 WHERE expires_at IS NULL OR expires_at > now() \
                 ORDER BY collection, key",
                self.config.table_name
            ))
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to prepare DuckDB collection enumeration: {error}"),
            })?;
        let mut rows = statement
            .query([])
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to query DuckDB collections: {error}"),
            })?;
        let mut collections = Vec::with_capacity(limit);
        let mut seen = HashSet::with_capacity(limit);
        while let Some(row) = rows.next().map_err(|error| Error::StoreConnection {
            message: format!("failed to read DuckDB collection row: {error}"),
        })? {
            let collection: String = row
                .get(0)
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let key: String = row
                .get(1)
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let raw_entry = Bytes::from(
                row.get::<_, Vec<u8>>(2)
                    .map_err(|error| Error::Deserialization(error.to_string()))?,
            );
            let expires_at: Option<DateTime<Utc>> = row
                .get(3)
                .map_err(|error| Error::Deserialization(error.to_string()))?;
            let entry = Self::decode_entry(&key, raw_entry, expires_at)?;
            if entry.is_expired() {
                return Err(Error::Deserialization(format!(
                    "DuckDB collection enumeration returned expired key {key}"
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
impl AsyncDestroyCollection for DuckDBStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let affected = self
            .conn()
            .lock()
            .await
            .execute(
                &format!(
                    "DELETE FROM {} WHERE collection = ?1",
                    self.config.table_name
                ),
                [collection],
            )
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to destroy DuckDB collection {collection}: {error}"),
            })?;
        Ok(affected > 0)
    }
}

#[async_trait]
impl AsyncDestroyStore for DuckDBStore {
    async fn destroy(&self) -> Result<bool> {
        let connection = self.conn().lock().await;
        let table_exists: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = current_schema() AND table_name = ?1",
                [&self.config.table_name],
                |row| row.get(0),
            )
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to inspect DuckDB table {} for destruction: {error}",
                    self.config.table_name
                ),
            })?;
        if table_exists == 0 {
            return Ok(false);
        }
        if table_exists != 1 {
            return Err(Error::StoreConnection {
                message: format!(
                    "DuckDB schema contains multiple tables named {}",
                    self.config.table_name
                ),
            });
        }
        connection
            .execute(&format!("DROP TABLE {}", self.config.table_name), [])
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to destroy DuckDB table {}: {error}",
                    self.config.table_name
                ),
            })?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[tokio::test]
    async fn duckdb_uses_strict_blob_schema_and_replaces_ttl() {
        let connection = duckdb::Connection::open_in_memory().unwrap();
        let store = DuckDBStore::from_conn(connection, None).await.unwrap();

        store
            .put("key", Value::utf8("ttl"), Some("entries"), Some(60.0))
            .await
            .unwrap();
        {
            let connection = store.conn().lock().await;
            let (entry, expires_at): (Vec<u8>, Option<DateTime<Utc>>) = connection
                .query_row(
                    "SELECT entry, expires_at FROM kv_entries \
                     WHERE collection = 'entries' AND key = 'key'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(&entry[..5], b"OKVE1");
            assert!(expires_at.is_some());
        }

        store
            .put("key", Value::utf8("without-ttl"), Some("entries"), None)
            .await
            .unwrap();
        assert_eq!(
            store.get("key", Some("entries")).await.unwrap(),
            Some(Value::utf8("without-ttl"))
        );
        let connection = store.conn().lock().await;
        let expires_at: Option<DateTime<Utc>> = connection
            .query_row(
                "SELECT expires_at FROM kv_entries \
                 WHERE collection = 'entries' AND key = 'key'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(expires_at.is_none());
    }

    #[tokio::test]
    async fn duckdb_rejects_old_schema_and_conflicting_index() {
        let connection = duckdb::Connection::open_in_memory().unwrap();
        connection
            .execute(
                "CREATE TABLE kv_entries (\
                    collection VARCHAR NOT NULL,\
                    key VARCHAR NOT NULL,\
                    value JSON NOT NULL,\
                    created_at TIMESTAMPTZ,\
                    expires_at TIMESTAMPTZ,\
                    PRIMARY KEY (collection, key)\
                )",
                [],
            )
            .unwrap();
        assert!(matches!(
            DuckDBStore::from_conn(connection, None).await,
            Err(Error::StoreSetup { .. })
        ));

        let connection = duckdb::Connection::open_in_memory().unwrap();
        connection
            .execute(
                "CREATE TABLE kv_entries (\
                    collection VARCHAR NOT NULL,\
                    key VARCHAR NOT NULL,\
                    entry BLOB NOT NULL,\
                    expires_at TIMESTAMPTZ,\
                    PRIMARY KEY (collection, key)\
                )",
                [],
            )
            .unwrap();
        connection
            .execute(
                "CREATE INDEX wrong_expires_index ON kv_entries(expires_at)",
                [],
            )
            .unwrap();
        assert!(matches!(
            DuckDBStore::from_conn(connection, None).await,
            Err(Error::StoreSetup { .. })
        ));
    }

    #[tokio::test]
    async fn duckdb_batches_cleanup_enumeration_and_destroy_are_strict() {
        let connection = duckdb::Connection::open_in_memory().unwrap();
        let store = DuckDBStore::from_conn(connection, None).await.unwrap();
        let keys = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        let values = vec![
            Value::utf8("first"),
            Value::utf8("second"),
            Value::utf8("last"),
        ];
        store
            .put_many(&keys, &values, Some("entries"), None)
            .await
            .unwrap();

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

        let expired = ManagedEntry {
            value: Value::utf8("expired"),
            created_at: Some(Utc::now() - TimeDelta::seconds(10)),
            expires_at: Some(Utc::now() - TimeDelta::seconds(5)),
        };
        {
            let connection = store.conn().lock().await;
            connection
                .execute(
                    "INSERT INTO kv_entries (collection, key, entry, expires_at) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params!["entries", "expired", expired.encode(), expired.expires_at],
                )
                .unwrap();
        }
        store.cull().await.unwrap();
        assert_eq!(
            store.keys(Some("entries"), None).await.unwrap(),
            vec!["b".to_string()]
        );
        assert_eq!(
            store.collections(None).await.unwrap(),
            vec!["entries".to_string()]
        );

        {
            let connection = store.conn().lock().await;
            connection
                .execute(
                    "INSERT INTO kv_entries (collection, key, entry, expires_at) \
                     VALUES ('entries', 'legacy', '{\"value\":null}', NULL)",
                    [],
                )
                .unwrap();
        }
        assert!(store.get("legacy", Some("entries")).await.is_err());
        assert!(store.keys(Some("entries"), None).await.is_err());
        assert!(store.collections(None).await.is_err());

        assert!(store.destroy_collection("entries").await.unwrap());
        assert!(!store.destroy_collection("entries").await.unwrap());
        assert!(store.destroy().await.unwrap());
        assert!(!store.destroy().await.unwrap());
    }

    #[tokio::test]
    async fn duckdb_rejects_expiration_mismatch() {
        let connection = duckdb::Connection::open_in_memory().unwrap();
        let store = DuckDBStore::from_conn(connection, None).await.unwrap();
        let entry = ManagedEntry::with_ttl(Value::utf8("value"), 60.0).unwrap();
        {
            let connection = store.conn().lock().await;
            connection
                .execute(
                    "INSERT INTO kv_entries (collection, key, entry, expires_at) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        "entries",
                        "key",
                        entry.encode(),
                        entry.expires_at.unwrap() + TimeDelta::milliseconds(1)
                    ],
                )
                .unwrap();
        }
        assert!(store.get("key", Some("entries")).await.is_err());
    }
}
