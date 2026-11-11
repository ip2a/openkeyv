mod client;
mod config;
mod error;
mod store;

use super::required;
pub use client::DynamoDBClient;
pub use config::DynamoDBConfig;
pub use store::DynamoDBStore;

/// Open a dynamodb-backed store from a [`StoreConfig`].
pub async fn open(config: &openkeyv::StoreConfig) -> openkeyv::Result<openkeyv::StoreHandle> {
    let store = DynamoDBStore::new(required(&config.config, "table")?).await?;
    Ok(openkeyv::StoreHandle::basic(std::sync::Arc::new(store)))
}
