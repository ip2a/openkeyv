#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
#[cfg(feature = "firestore")]
pub mod firestore;
#[cfg(feature = "keyring")]
pub mod keyring;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "opensearch")]
pub mod opensearch;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "vault")]
pub mod vault;

use openkeyv::{Error, Result};
use serde_json::Value;

pub(crate) fn string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

pub(crate) fn required(config: &Value, key: &str) -> Result<String> {
    string(config, key).ok_or_else(|| Error::StoreSetup {
        message: format!("store configuration requires '{key}'"),
    })
}

pub(crate) fn optional_usize(config: &Value, key: &str) -> Option<usize> {
    config
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}
