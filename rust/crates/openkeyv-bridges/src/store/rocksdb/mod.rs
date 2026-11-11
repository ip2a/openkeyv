mod client;
mod config;
mod error;
mod store;

use super::required;
pub use client::RocksDBClient;
pub use config::RocksDBConfig;
pub use store::RocksDBStore;

/// Open a rocksdb-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = RocksDBStore::new(required(&config.config, "path")?)?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
