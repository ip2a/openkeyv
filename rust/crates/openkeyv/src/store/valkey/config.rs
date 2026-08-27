const DEFAULT_COLLECTION: &str = "default_collection";

use crate::utils::compound::Subspace;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValkeyConfig {
    pub default_collection: String,
    pub keyspace: Subspace,
}

impl ValkeyConfig {
    pub fn new(default_collection: Option<String>) -> Self {
        Self {
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
            keyspace: Subspace::default(),
        }
    }

    pub fn with_keyspace(mut self, keyspace: impl Into<String>) -> Self {
        self.keyspace = Subspace::new(keyspace);
        self
    }
}

impl Default for ValkeyConfig {
    fn default() -> Self {
        Self::new(None)
    }
}
