pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn map_os_err(e: opensearch::Error) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}
