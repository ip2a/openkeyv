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
//! | DuckDB | `duckdb-store` | DuckDB embedded |
//! | Memcached | `memcached` | Memcached |
//! | Valkey | `valkey` | Valkey (Redis-compatible) |
//! | Vault | `vault` | HashiCorp Vault |
//! | Keyring | `keyring-store` | OS keyring |
//! | Firestore | `firestore-store` | Google Firestore |
//! | OpenSearch | `opensearch` | OpenSearch |

pub mod adapter;
pub mod entry;
pub mod error;
pub mod protocol;
pub mod store;
pub mod utils;
pub mod value;
pub mod wrapper;

#[cfg(feature = "python")]
pub mod py;

pub use entry::ManagedEntry;
pub use error::{Error, Result};
pub use protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
pub use value::{Value, ValueKind};
