pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn connection_err(message: impl Into<String>) -> Error {
    Error::StoreConnection {
        message: message.into(),
    }
}
