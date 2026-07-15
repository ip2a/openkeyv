use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum Error {
    #[error("store setup failed: {message}")]
    StoreSetup { message: String },

    #[error("store connection error: {message}")]
    StoreConnection { message: String },

    #[error("serialization failed: {0}")]
    Serialization(String),

    #[error("deserialization failed: {0}")]
    Deserialization(String),

    #[error("missing key: {0}")]
    MissingKey(String),

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("invalid ttl: {0}")]
    InvalidTtl(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("value too large: {size} bytes (max {max})")]
    ValueTooLarge { size: usize, max: usize },

    #[error("entry too large")]
    EntryTooLarge,

    #[error("entry too small")]
    EntryTooSmall,

    #[error("read-only store")]
    ReadOnly,

    #[error("encryption error: {0}")]
    Encryption(String),

    #[error("decryption error: {0}")]
    Decryption(String),

    #[error("corrupted data")]
    CorruptedData,

    #[error("encryption version mismatch")]
    EncryptionVersion,

    #[error("path security error: {0}")]
    PathSecurity(String),

    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    #[error("batch size mismatch: keys={keys} values={values}")]
    BatchSizeMismatch { keys: usize, values: usize },

    #[error("invalid change cursor: {0}")]
    InvalidChangeCursor(String),

    #[error("change cursor {requested} is older than retained history starting at {oldest}")]
    ChangeCursorExpired { requested: String, oldest: String },

    #[error("change subscriber lagged by {skipped} records")]
    ChangeFeedLagged { skipped: u64 },
}
