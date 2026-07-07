use crate::value::Value;
use std::collections::HashMap;

pub type SeedData = HashMap<String, HashMap<String, Value>>;

const DEFAULT_COLLECTION: &str = "default_collection";

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryConfig {
    pub max_entries_per_collection: Option<usize>,
    pub default_collection: String,
    pub seed: Option<SeedData>,
}

impl MemoryConfig {
    pub fn new(
        max_entries_per_collection: Option<usize>,
        default_collection: Option<String>,
        seed: Option<SeedData>,
    ) -> Self {
        Self {
            max_entries_per_collection,
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
            seed,
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self::new(None, None, None)
    }
}
