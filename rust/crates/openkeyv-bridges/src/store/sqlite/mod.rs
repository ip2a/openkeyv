mod client;
mod config;
mod error;
mod store;

use super::string;
pub use client::SqliteClient;
pub use config::SqliteConfig;
pub use store::SqliteStore;

/// Open a sqlite-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = SqliteStore::new(
        string(&config.config, "path").as_deref(),
        string(&config.config, "table").as_deref(),
    )
    .await?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
