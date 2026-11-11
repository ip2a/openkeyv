mod client;
mod config;
mod error;
mod store;

use super::{optional_usize, required, string};
pub use client::PostgresClient;
pub use config::PostgresConfig;
pub use store::PostgresStore;

/// Open a postgres-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let mut store_config = PostgresConfig::new(string(&config.config, "table").as_deref())?;
    if let Some(retention) = optional_usize(&config.config, "change_retention") {
        store_config = store_config.with_change_retention(retention);
    }
    let store = std::sync::Arc::new(
        PostgresStore::new_with_config(&required(&config.config, "url")?, store_config).await?,
    );
    let base = store.clone();
    Ok(openkeyv::StoreHandle::with_capabilities(
        store,
        Some(base.clone()),
        Some(base.clone()),
        Some(base.clone()),
        Some(base),
    ))
}
