const DEFAULT_COLLECTION: &str = "default_collection";
const DEFAULT_SERVICE_NAME: &str = "openkeyv";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyringConfig {
    pub default_collection: String,
    pub service_name: String,
}

impl KeyringConfig {
    pub fn new(service_name: Option<String>, default_collection: Option<String>) -> Self {
        Self {
            service_name: service_name.unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string()),
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
        }
    }
}

impl Default for KeyringConfig {
    fn default() -> Self {
        Self::new(None, None)
    }
}
