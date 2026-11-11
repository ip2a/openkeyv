mod client;
mod config;
mod error;
mod store;

use super::required;
pub use client::S3Client;
pub use config::S3Config;
pub use store::S3Store;

/// Open an s3-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = S3Store::new(required(&config.config, "bucket")?).await?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
