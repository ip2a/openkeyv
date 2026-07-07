const DEFAULT_COLLECTION: &str = "default_collection";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3Config {
    pub bucket_name: String,
    pub default_collection: String,
}

impl S3Config {
    pub fn new(bucket_name: impl Into<String>, default_collection: Option<String>) -> Self {
        Self {
            bucket_name: bucket_name.into(),
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
        }
    }
}
