const DEFAULT_COLLECTION: &str = "default_collection";

pub use crate::utils::compound::ForeignKeyPolicy;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisConfig {
    pub default_collection: String,
    pub foreign_key_policy: ForeignKeyPolicy,
}

impl RedisConfig {
    pub fn new(default_collection: Option<String>) -> Self {
        Self {
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
            foreign_key_policy: ForeignKeyPolicy::default(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self::new(None)
    }
}
