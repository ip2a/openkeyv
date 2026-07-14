use bytes::Bytes;

use crate::error::Result;

/// Structured value owned by the Rust core.
///
/// Python bindings convert Python `dict` and `list` objects into this model
/// before encoding. The core model deliberately uses string dictionary keys.
#[derive(Debug, Clone, PartialEq)]
pub enum StructuredValue {
    Null,
    Bool(bool),
    Integer(i64),
    UnsignedInteger(u64),
    Float(f64),
    String(String),
    Bytes(Bytes),
    List(Vec<StructuredValue>),
    Dict(Vec<(String, StructuredValue)>),
}

impl StructuredValue {
    pub fn encode(&self) -> Result<Bytes> {
        super::codec::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        super::codec::decode(bytes)
    }
}
