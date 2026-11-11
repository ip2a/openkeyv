mod client;
mod config;
mod error;
mod store;

use super::required;
pub use client::MongoDBClient;
pub use config::MongoDBConfig;
pub use store::MongoDBStore;

/// Open a mongodb-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = MongoDBStore::new(required(&config.config, "url")?).await?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
