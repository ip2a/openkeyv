pub type Error = crate::error::Error;
pub type Result<T> = crate::error::Result<T>;

pub fn map_redis_err(e: redis::RedisError) -> Error {
    Error::StoreConnection {
        message: e.to_string(),
    }
}
