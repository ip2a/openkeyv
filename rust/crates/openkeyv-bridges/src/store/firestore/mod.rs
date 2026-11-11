mod client;
mod config;
mod error;
mod store;

use super::required;
pub use client::FirestoreClient;
pub use config::FirestoreConfig;
pub use store::FirestoreStore;

/// Open a firestore-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = FirestoreStore::new(&required(&config.config, "project")?).await?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
