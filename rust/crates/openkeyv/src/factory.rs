use serde_json::Value;

use crate::{Error, Result, StoreConfig, StoreHandle};

fn string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn required(config: &Value, key: &str) -> Result<String> {
    string(config, key).ok_or_else(|| Error::StoreSetup {
        message: format!("store configuration requires '{key}'"),
    })
}

pub async fn open_store(config: StoreConfig) -> Result<StoreHandle> {
    match config.store.as_str() {
        "memory" => {
            let store = std::sync::Arc::new(crate::store::memory::MemoryStore::new());
            let base = store.clone();
            Ok(StoreHandle::with_capabilities(
                store,
                Some(base.clone()),
                Some(base.clone()),
                Some(base.clone()),
                Some(base),
            ))
        }
        "simple" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::simple::SimpleStore::new(),
        ))),
        "null" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::null::NullStore::new(),
        ))),
        "filetree" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::filetree::FileTreeStore::new(required(&config.config, "path")?),
        ))),

        #[cfg(feature = "disk")]
        "disk" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::disk::DiskStore::new(std::path::Path::new(&required(
                &config.config,
                "path",
            )?))?,
        ))),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::rocksdb::RocksDBStore::new(required(&config.config, "path")?)?,
        ))),

        #[cfg(feature = "redis")]
        "redis" => {
            let store = std::sync::Arc::new(
                crate::store::redis::RedisStore::new(&required(&config.config, "url")?).await?,
            );
            let base = store.clone();
            Ok(StoreHandle::with_capabilities(
                store.clone(),
                Some(base.clone()),
                Some(base.clone()),
                Some(base.clone()),
                Some(base),
            ))
        }
        #[cfg(feature = "valkey")]
        "valkey" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::valkey::ValkeyStore::new(&required(&config.config, "url")?).await?,
        ))),

        #[cfg(feature = "postgres")]
        "postgres" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::postgres::PostgresStore::new(
                &required(&config.config, "url")?,
                string(&config.config, "table").as_deref(),
            )
            .await?,
        ))),
        #[cfg(feature = "sqlite")]
        "sqlite" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::sqlite::SqliteStore::new(
                string(&config.config, "path").as_deref(),
                string(&config.config, "table").as_deref(),
            )
            .await?,
        ))),
        #[cfg(feature = "duckdb")]
        "duckdb" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::duckdb::DuckDBStore::new(
                string(&config.config, "path").as_deref(),
                string(&config.config, "table").as_deref(),
            )
            .await?,
        ))),
        #[cfg(feature = "mongodb")]
        "mongodb" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::mongodb::MongoDBStore::new(required(&config.config, "url")?).await?,
        ))),
        #[cfg(feature = "memcached")]
        "memcached" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::memcached::MemcachedStore::new(&required(&config.config, "url")?)?,
        ))),
        #[cfg(feature = "vault")]
        "vault" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::vault::VaultStore::new(
                &required(&config.config, "url")?,
                &required(&config.config, "token")?,
                string(&config.config, "mount_point").as_deref(),
            )?,
        ))),
        #[cfg(feature = "keyring")]
        "keyring" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::keyring::KeyringStore::new(string(&config.config, "service").as_deref()),
        ))),
        #[cfg(feature = "s3")]
        "s3" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::s3::S3Store::new(required(&config.config, "bucket")?).await?,
        ))),
        #[cfg(feature = "dynamodb")]
        "dynamodb" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::dynamodb::DynamoDBStore::new(required(&config.config, "table")?).await?,
        ))),
        #[cfg(feature = "firestore")]
        "firestore" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::firestore::FirestoreStore::new(&required(&config.config, "project")?)
                .await?,
        ))),
        #[cfg(feature = "opensearch")]
        "opensearch" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::opensearch::OpenSearchStore::from_url(
                required(&config.config, "url")?,
                string(&config.config, "index").unwrap_or_else(|| "openkeyv".into()),
            )
            .await?,
        ))),
        _ => Err(Error::StoreSetup {
            message: format!(
                "unknown or unavailable Store '{}', enable its OpenKeyv feature",
                config.store
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn opens_memory_store_without_connection_fields() {
        let handle = open_store(StoreConfig::memory()).await.unwrap();

        assert_eq!(handle.base.store_name(), "MemoryStore");
        assert!(handle.capabilities.change_feed);
    }

    #[tokio::test]
    async fn rejects_unknown_store_by_name() {
        let error = match open_store(StoreConfig::new("does-not-exist", Value::Null)).await {
            Ok(_) => panic!("unknown Store must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("does-not-exist"));
    }
}
