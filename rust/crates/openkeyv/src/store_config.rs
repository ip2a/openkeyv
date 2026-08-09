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
}
