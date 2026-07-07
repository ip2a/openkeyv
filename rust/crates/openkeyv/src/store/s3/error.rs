pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn build_err(e: aws_sdk_s3::error::BuildError) -> Error {
    Error::StoreSetup {
        message: e.to_string(),
    }
}

pub fn is_s3_not_found<E, R>(e: &aws_sdk_s3::error::SdkError<E, R>) -> bool {
    let msg = format!("{}", e);
    msg.contains("NoSuchKey") || msg.contains("404") || msg.contains("NotFound")
}
