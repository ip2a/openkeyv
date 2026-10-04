const DEFAULT_COLLECTION: &str = "default_collection";
const DEFAULT_CHANGE_RETENTION: usize = 10_000;

use crate::utils::compound::Subspace;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValkeyConfig {
    pub default_collection: String,
    pub keyspace: Subspace,
    /// Change records kept in the backing stream.
    /// 0 disables change recording entirely; subscribe() then returns an error.
    pub change_retention: usize,
}

impl ValkeyConfig {
    pub fn new(default_collection: Option<String>) -> Self {
        Self {
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
            keyspace: Subspace::default(),
            change_retention: DEFAULT_CHANGE_RETENTION,
        }
    }

    pub fn with_keyspace(mut self, keyspace: impl Into<String>) -> Self {
        self.keyspace = Subspace::new(keyspace);
        self
    }

    pub fn with_change_retention(mut self, retention: usize) -> Self {
        self.change_retention = retention;
        self
    }
}

impl Default for ValkeyConfig {
    fn default() -> Self {
        Self::new(None)
    }
}
