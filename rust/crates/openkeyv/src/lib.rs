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
//! Native backends ship unconditionally. SDK-backed backends live in the
//! `openkeyv-bridges` crate, each behind its own feature.
//!
//! | Backend | Where | Description |
//! |---------|-------|-------------|
//! | Memory | core | In-memory DashMap |
//! | Disk | core | Sled embedded DB |
//! | Redis | core | Redis protocol |
//! | Valkey | core | Valkey (Redis-compatible) |
//! | Memcached | core | Memcached |
//! | SHM | core (unix only) | Shared-memory store |
//! | RocksDB | bridges `rocksdb` | RocksDB embedded |
//! | Postgres | bridges `postgres` | PostgreSQL via sqlx |
//! | MongoDB | bridges `mongodb` | MongoDB |
//! | DynamoDB | bridges `dynamodb` | AWS DynamoDB |
//! | S3 | bridges `s3` | AWS S3 |
//! | DuckDB | bridges `duckdb` | DuckDB embedded |
//! | SQLite | bridges `sqlite` | SQLite embedded |
//! | Vault | bridges `vault` | HashiCorp Vault |
//! | Keyring | bridges `keyring` | OS keyring |
//! | Firestore | bridges `firestore` | Google Firestore |
//! | OpenSearch | bridges `opensearch` | OpenSearch |

mod cas;
pub mod change;
pub mod entry;
pub mod error;
pub mod factory;
pub mod handle;
pub mod migration;
pub mod protocol;
pub mod store;
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
pub use store_config::StoreConfig;
pub use utils::compound::{
    Subspace, compound_key, decompound_key, subspace_compound_key, subspace_decompound_key,
};
pub use value::{StructuredValue, Value, ValueKind};
