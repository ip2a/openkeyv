mod client;
mod config;
mod error;
mod store;

use super::{required, string};
pub use client::VaultClient;
pub use config::VaultConfig;
pub use store::VaultStore;

/// Open a vault-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = VaultStore::new(
        &required(&config.config, "url")?,
        &required(&config.config, "token")?,
        string(&config.config, "mount_point").as_deref(),
    )?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
