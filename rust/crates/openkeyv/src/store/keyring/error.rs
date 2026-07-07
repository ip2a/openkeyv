pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn map_keyring_err(e: keyring::Error) -> Error {
    match e {
        keyring::Error::TooLong(_name, len) => Error::ValueTooLarge {
            size: len as usize,
            max: len as usize,
        },
        _ => Error::StoreConnection {
            message: e.to_string(),
        },
    }
}
