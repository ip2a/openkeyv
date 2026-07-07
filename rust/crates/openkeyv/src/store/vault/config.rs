const DEFAULT_COLLECTION: &str = "default_collection";
const DEFAULT_MOUNT_POINT: &str = "secret";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VaultConfig {
    pub default_collection: String,
    pub mount_point: String,
}

impl VaultConfig {
    pub fn new(default_collection: Option<String>, mount_point: Option<String>) -> Self {
        Self {
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
            mount_point: mount_point.unwrap_or_else(|| DEFAULT_MOUNT_POINT.to_string()),
        }
    }
}

impl Default for VaultConfig {
    fn default() -> Self {
        Self::new(None, None)
    }
}
