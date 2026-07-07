pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn map_vault_err(e: vaultrs::error::ClientError) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}
