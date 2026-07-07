pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn map_firestore_err(e: firestore::errors::FirestoreError) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}
