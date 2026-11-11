const DEFAULT_COLLECTION: &str = "default_collection";
pub const DEFAULT_PAGE_SIZE: usize = 10_000;
pub const PAGE_LIMIT: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenSearchConfig {
    pub index_prefix: String,
    pub default_collection: String,
}

impl OpenSearchConfig {
    pub fn new(index_prefix: impl Into<String>) -> Self {
        Self {
            index_prefix: index_prefix.into(),
            default_collection: DEFAULT_COLLECTION.to_string(),
        }
    }
}
