//! # openkeyv
//!
//! Async Key-Value Store — A pluggable interface for KV stores in Rust.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use openkeyv::store::memory::MemoryStore;
//! use openkeyv::protocol::AsyncKeyValue;
//!
//! # async fn run() {
//! let store = MemoryStore::new();
//! # }
//! ```
//!
//! ## Backends
//!
//! | Backend | Feature | Description |
//! |---------|---------|-------------|
//! | Memory | default | In-memory DashMap |
//! | Disk | `disk` | Sled embedded DB |
//! | Redis | `redis` | Redis protocol |
//! | RocksDB | `rocksdb` | RocksDB embedded |
//! | Postgres | `postgres` | PostgreSQL via sqlx |
//! | MongoDB | `mongodb` | MongoDB |
//! | DynamoDB | `dynamodb` | AWS DynamoDB |
//! | S3 | `s3` | AWS S3 |
//! | DuckDB | `duckdb` | DuckDB embedded |
//! | SQLite | `sqlite` | SQLite embedded |
//! | Memcached | `memcached` | Memcached |
//! | Valkey | `valkey` | Valkey (Redis-compatible) |
//! | Vault | `vault` | HashiCorp Vault |
//! | Keyring | `keyring` | OS keyring |
//! | Firestore | `firestore` | Google Firestore |
//! | OpenSearch | `opensearch` | OpenSearch |

#[cfg(any(feature = "redis", feature = "valkey"))]
mod cas;
pub mod change;
pub mod entry;
pub mod error;
#[cfg(feature = "json")]
pub mod factory;
pub mod handle;
pub mod migration;
pub mod protocol;
pub mod store;
#[cfg(feature = "json")]
pub mod store_config;
pub mod utils;
pub mod value;
pub mod wrapper;

#[cfg(feature = "python")]
pub mod py;

pub use change::{
    ChangeCursor, ChangeFeedRequest, ChangeFilter, ChangeOperation, ChangeStart, ChangeStream,
    ChangeSubscription, StoreChange,
};
pub use entry::ManagedEntry;
pub use error::{Error, Result};
pub use handle::{StoreCapabilities, StoreHandle};
pub use migration::AsyncKeyspaceMigration;
#[cfg(any(feature = "redis", feature = "valkey"))]
pub use migration::migrate_into_keyspace;
pub use migration::{
    MigrationOptions, MigrationReport, apply_change, copy_snapshot, copy_snapshot_with_feed,
    merge_report,
};
pub use protocol::{
    AsyncChangeFeed, AsyncCompareAndSwap, AsyncCull, AsyncDestroyCollection, AsyncDestroyStore,
    AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue, BaseStore,
    CompareAndDeleteResult, CompareAndSwapResult, Revision, RevisionedValue,
};
#[cfg(feature = "json")]
pub use store_config::StoreConfig;
pub use utils::compound::{
    Subspace, compound_key, decompound_key, subspace_compound_key, subspace_decompound_key,
};
pub use value::{StructuredValue, Value, ValueKind};
