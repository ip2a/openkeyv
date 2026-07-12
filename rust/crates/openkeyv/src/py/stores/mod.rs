mod filetree;
mod memory;
mod null;
mod simple;

#[cfg(feature = "disk")]
mod disk;
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "dynamodb")]
mod dynamodb;
#[cfg(feature = "firestore")]
mod firestore;
#[cfg(feature = "keyring")]
mod keyring;
#[cfg(feature = "memcached")]
mod memcached;
#[cfg(feature = "mongodb")]
mod mongodb;
#[cfg(feature = "opensearch")]
mod opensearch;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "rocksdb")]
mod rocksdb;
#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "valkey")]
mod valkey;
#[cfg(feature = "vault")]
mod vault;

use pyo3::prelude::*;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<memory::PyMemoryStore>()?;
    m.add_class::<simple::PySimpleStore>()?;
    m.add_class::<filetree::PyFileTreeStore>()?;
    m.add_class::<null::PyNullStore>()?;
    #[cfg(feature = "disk")]
    m.add_class::<disk::PyDiskStore>()?;
    #[cfg(feature = "redis")]
    m.add_class::<redis::PyRedisStore>()?;
    #[cfg(feature = "valkey")]
    m.add_class::<valkey::PyValkeyStore>()?;
    #[cfg(feature = "rocksdb")]
    m.add_class::<rocksdb::PyRocksDBStore>()?;
    #[cfg(feature = "postgres")]
    m.add_class::<postgres::PyPostgresStore>()?;
    #[cfg(feature = "mongodb")]
    m.add_class::<mongodb::PyMongoDBStore>()?;
    #[cfg(feature = "dynamodb")]
    m.add_class::<dynamodb::PyDynamoDBStore>()?;
    #[cfg(feature = "s3")]
    m.add_class::<s3::PyS3Store>()?;
    #[cfg(feature = "duckdb")]
    m.add_class::<duckdb::PyDuckDBStore>()?;
    #[cfg(feature = "memcached")]
    m.add_class::<memcached::PyMemcachedStore>()?;
    #[cfg(feature = "vault")]
    m.add_class::<vault::PyVaultStore>()?;
    #[cfg(feature = "keyring")]
    m.add_class::<keyring::PyKeyringStore>()?;
    #[cfg(feature = "firestore")]
    m.add_class::<firestore::PyFirestoreStore>()?;
    #[cfg(feature = "opensearch")]
    m.add_class::<opensearch::PyOpenSearchStore>()?;
    Ok(())
}
