const DEFAULT_COLLECTION: &str = "default_collection";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileTreeConfig {
    pub default_collection: String,
}

impl FileTreeConfig {
    pub fn new(default_collection: Option<String>) -> Self {
        Self {
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
        }
    }
}

impl Default for FileTreeConfig {
    fn default() -> Self {
        Self::new(None)
    }
}
