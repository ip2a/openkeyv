pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn map_rocksdb_err(e: rocksdb::Error) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}
