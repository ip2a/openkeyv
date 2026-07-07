use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use serde_json;

/// Abstract serialization adapter for converting `ManagedEntry` to/from store formats.
pub trait SerializationAdapter: Send + Sync {
    /// Serialize a `ManagedEntry` into a storage string.
    fn dump_json(&self, entry: &ManagedEntry) -> Result<String>;

    /// Deserialize a storage string back into a `ManagedEntry`.
    fn load_json(&self, json_str: &str) -> Result<ManagedEntry>;
}

/// Basic JSON serialization adapter using `serde_json`.
#[derive(Debug, Clone, Default)]
pub struct BasicSerializationAdapter;

impl BasicSerializationAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl SerializationAdapter for BasicSerializationAdapter {
    fn dump_json(&self, entry: &ManagedEntry) -> Result<String> {
        serde_json::to_string(entry).map_err(|e| Error::Serialization(e.to_string()))
    }

    fn load_json(&self, json_str: &str) -> Result<ManagedEntry> {
        serde_json::from_str(json_str).map_err(|e| Error::Deserialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_basic_serialization_roundtrip() {
        let adapter = BasicSerializationAdapter::new();
        let entry = ManagedEntry::new(Value::null());

        let json = adapter.dump_json(&entry).unwrap();
        let restored = adapter.load_json(&json).unwrap();

        assert_eq!(entry.value, restored.value);
    }
}
