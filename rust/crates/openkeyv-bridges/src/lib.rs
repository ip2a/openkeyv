//! SDK-backed store bridges for openkeyv.
//!
//! Every backend here is opt-in via its own feature. Backend names that miss
//! the local table fall through to `openkeyv::factory::open_store`, so native
//! openkeyv backends keep working through the same entry point.

pub mod store;

use openkeyv::{Result, StoreConfig, StoreHandle};

/// Open a store, consulting the bridge table first, then the openkeyv core.
pub async fn open_store(config: StoreConfig) -> Result<StoreHandle> {
    match config.store.as_str() {
        #[cfg(feature = "postgres")]
        "postgres" => store::postgres::open(&config).await,
        #[cfg(feature = "sqlite")]
        "sqlite" => store::sqlite::open(&config).await,
        #[cfg(feature = "duckdb")]
        "duckdb" => store::duckdb::open(&config).await,
        #[cfg(feature = "mongodb")]
        "mongodb" => store::mongodb::open(&config).await,
        #[cfg(feature = "firestore")]
        "firestore" => store::firestore::open(&config).await,
        #[cfg(feature = "opensearch")]
        "opensearch" => store::opensearch::open(&config).await,
        #[cfg(feature = "s3")]
        "s3" => store::s3::open(&config).await,
        #[cfg(feature = "dynamodb")]
        "dynamodb" => store::dynamodb::open(&config).await,
        #[cfg(feature = "vault")]
        "vault" => store::vault::open(&config).await,
        #[cfg(feature = "keyring")]
        "keyring" => store::keyring::open(&config).await,
        #[cfg(feature = "rocksdb")]
        "rocksdb" => store::rocksdb::open(&config).await,
        _ => openkeyv::factory::open_store(config).await,
    }
}
