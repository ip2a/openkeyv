pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn build_err(e: aws_sdk_dynamodb::error::BuildError) -> Error {
    Error::StoreSetup {
        message: e.to_string(),
    }
}
