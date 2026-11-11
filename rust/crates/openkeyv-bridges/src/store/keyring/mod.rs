mod client;
mod config;
mod error;
mod store;

use super::string;
pub use client::KeyringClient;
pub use config::KeyringConfig;
pub use store::KeyringStore;

/// Open a keyring-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = KeyringStore::new(string(&config.config, "service").as_deref());
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
