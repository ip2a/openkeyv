use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct StoreConfig {
    pub store: String,
    pub config: Value,
}

impl StoreConfig {
    pub fn new(store: impl Into<String>, config: Value) -> Self {
        Self {
            store: store.into().to_ascii_lowercase(),
            config,
        }
    }

    pub fn memory() -> Self {
        Self::new("memory", Value::Null)
    }

    pub fn redis(config: Value) -> Self {
        Self::new("redis", config)
    }

    /// Shared-memory store configuration (Unix only, `shm` feature).
    ///
    /// Keys: `path` (required), `mode` (`"create"` initializes or resets the
    /// backing file exclusively; `"open"`/absent attaches to an existing one),
    /// plus optional `capacity_bytes`, `index_entries`, and
    /// `change_retention` layout hints. One process creates the backing;
    /// every other process opens it.
    pub fn shm(config: Value) -> Self {
        Self::new("shm", config)
    }
}
