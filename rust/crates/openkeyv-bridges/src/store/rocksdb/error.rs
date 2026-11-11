pub type Error = openkeyv::error::Error;
pub type Result<T> = openkeyv::error::Result<T>;

pub fn map_rocksdb_err(e: rocksdb::Error) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}
