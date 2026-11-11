mod client;
mod config;
mod error;
mod store;

use super::{required, string};
pub use client::OpenSearchClient;
pub use config::OpenSearchConfig;
pub use store::OpenSearchStore;

/// Open an opensearch-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = OpenSearchStore::from_url(
        required(&config.config, "url")?,
        string(&config.config, "index").unwrap_or_else(|| "openkeyv".into()),
    )
    .await?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
