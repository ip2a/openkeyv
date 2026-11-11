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

fn optional_usize(config: &Value, key: &str) -> Option<usize> {
    config
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
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
        "disk" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::disk::DiskStore::new(std::path::Path::new(&required(
                &config.config,
                "path",
            )?))?,
        ))),

        "redis" => {
            let mut store_config = crate::store::redis::RedisConfig::default()
                .with_keyspace(string(&config.config, "keyspace").unwrap_or_default());
            if let Some(retention) = optional_usize(&config.config, "change_retention") {
                store_config = store_config.with_change_retention(retention);
            }
            let store = std::sync::Arc::new(
                crate::store::redis::RedisStore::new_with_config(
                    &required(&config.config, "url")?,
                    store_config,
                )
                .await?,
            );
            let base = store.clone();
            Ok(StoreHandle::with_capabilities(
                store.clone(),
                Some(base.clone()),
                Some(base.clone()),
                Some(base.clone()),
                Some(base),
            )
            .with_keyspace_migration(store))
        }

        #[cfg(unix)]
        "shm" => {
            use crate::store::shm::{ShmConfig, ShmOpenMode, ShmStore};

            let mut shm_config = ShmConfig::open(required(&config.config, "path")?);
            match string(&config.config, "mode").as_deref() {
                None | Some("open") => {}
                Some("create") => shm_config.mode = ShmOpenMode::Create,
                Some(other) => {
                    return Err(Error::StoreSetup {
                        message: format!("shm mode must be 'create' or 'open', got '{other}'"),
                    });
                }
            }
            if let Some(capacity) = optional_usize(&config.config, "capacity_bytes") {
                shm_config = shm_config.with_capacity_bytes(capacity as u64);
            }
            if let Some(entries) = optional_usize(&config.config, "index_entries") {
                shm_config =
                    shm_config.with_index_entries(u32::try_from(entries).map_err(|_| {
                        Error::StoreSetup {
                            message: format!("shm index_entries {entries} exceeds u32"),
                        }
                    })?);
            }
            if let Some(retention) = optional_usize(&config.config, "change_retention") {
                shm_config =
                    shm_config.with_change_retention(u32::try_from(retention).map_err(|_| {
                        Error::StoreSetup {
                            message: format!("shm change_retention {retention} exceeds u32"),
                        }
                    })?);
            }
            let store = match shm_config.mode {
                ShmOpenMode::Create => ShmStore::create(shm_config.create_config())?,
                ShmOpenMode::Open => ShmStore::open(shm_config.path.clone())?,
            };
            let store = std::sync::Arc::new(store);
            let base = store.clone();
            let feed = store
                .change_feed_enabled()
                .then(|| base.clone() as std::sync::Arc<dyn crate::protocol::AsyncChangeFeed>);
            Ok(StoreHandle::with_capabilities(
                store,
                Some(base.clone()),
                Some(base.clone()),
                Some(base.clone()),
                feed,
            ))
        }

        "valkey" => {
            let mut store_config = crate::store::valkey::ValkeyConfig::default()
                .with_keyspace(string(&config.config, "keyspace").unwrap_or_default());
            if let Some(retention) = optional_usize(&config.config, "change_retention") {
                store_config = store_config.with_change_retention(retention);
            }
            let store = std::sync::Arc::new(
                crate::store::valkey::ValkeyStore::new_with_config(
                    &required(&config.config, "url")?,
                    store_config,
                )
                .await?,
            );
            let base = store.clone();
            Ok(StoreHandle::with_capabilities(
                store.clone(),
                Some(base.clone()),
                Some(base.clone()),
                Some(base.clone()),
                Some(base),
            )
            .with_keyspace_migration(store))
        }

        "memcached" => Ok(StoreHandle::basic(std::sync::Arc::new(
            crate::store::memcached::MemcachedStore::new(&required(&config.config, "url")?)?,
        ))),
        _ => Err(Error::StoreSetup {
            message: format!(
                "unknown Store '{}', sdk-backed backends live in the openkeyv-bridges crate",
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

    #[test]
    fn optional_usize_reads_positive_integers_only() {
        let config: Value = serde_json::json!({"a": 5, "b": -1, "c": "7", "d": 1.5});

        assert_eq!(optional_usize(&config, "a"), Some(5));
        assert_eq!(optional_usize(&config, "b"), None);
        assert_eq!(optional_usize(&config, "c"), None);
        assert_eq!(optional_usize(&config, "d"), None);
        assert_eq!(optional_usize(&config, "missing"), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn opens_shm_store_via_store_config() {
        // The store refuses group/world-writable parents; build a private dir.
        let dir = std::env::temp_dir().join(format!("openkeyv-shm-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let path = dir.join("factory.shm");

        let handle = open_store(StoreConfig::shm(serde_json::json!({
            "path": path.display().to_string(),
            "mode": "create",
            "capacity_bytes": 32 * 1024 * 1024,
        })))
        .await
        .unwrap();
        assert_eq!(handle.base.store_name(), "ShmStore");
        assert!(handle.capabilities.change_feed);
        drop(handle);

        // Reopen through the same constructor path with the default mode.
        let handle = open_store(StoreConfig::shm(serde_json::json!({
            "path": path.display().to_string(),
        })))
        .await
        .unwrap();
        assert_eq!(handle.base.store_name(), "ShmStore");
        drop(handle);

        std::fs::remove_dir_all(&dir).unwrap();
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
