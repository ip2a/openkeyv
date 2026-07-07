const DEFAULT_COLLECTION: &str = "default_collection";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RocksDBConfig {
    pub default_collection: String,
}

impl RocksDBConfig {
    pub fn new(default_collection: Option<String>) -> Self {
        Self {
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
        }
    }
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        Self::new(None)
    }
}
