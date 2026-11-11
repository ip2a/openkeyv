const DEFAULT_COLLECTION: &str = "default_collection";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamoDBConfig {
    pub table_name: String,
    pub default_collection: String,
}

impl DynamoDBConfig {
    pub fn new(table_name: impl Into<String>, default_collection: Option<String>) -> Self {
        Self {
            table_name: table_name.into(),
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
        }
    }
}
