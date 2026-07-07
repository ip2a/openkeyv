use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Protocol-level value stored by the Rust core.
///
/// The core treats values as typed bytes. Structured Python objects are encoded
/// at the PyO3 boundary and carried as opaque `Structured` bytes here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Value {
    kind: ValueKind,
    bytes: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueKind {
    Binary,
    Utf8,
    Integer,
    Float,
    Bool,
    Null,
    Structured,
}

impl ValueKind {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Binary => 0,
            Self::Utf8 => 1,
            Self::Integer => 2,
            Self::Float => 3,
            Self::Bool => 4,
            Self::Null => 5,
            Self::Structured => 6,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Binary),
            1 => Some(Self::Utf8),
            2 => Some(Self::Integer),
            3 => Some(Self::Float),
            4 => Some(Self::Bool),
            5 => Some(Self::Null),
            6 => Some(Self::Structured),
            _ => None,
        }
    }
}

impl Value {
    pub fn new(kind: ValueKind, bytes: impl Into<Bytes>) -> Self {
        Self {
            kind,
            bytes: bytes.into(),
        }
    }

    pub fn binary(bytes: impl Into<Bytes>) -> Self {
        Self::new(ValueKind::Binary, bytes)
    }

    pub fn utf8(value: impl Into<String>) -> Self {
        Self::new(ValueKind::Utf8, value.into())
    }

    pub fn integer(value: i64) -> Self {
        Self::new(ValueKind::Integer, value.to_le_bytes().to_vec())
    }

    pub fn float(value: f64) -> Self {
        Self::new(ValueKind::Float, value.to_le_bytes().to_vec())
    }

    pub fn bool(value: bool) -> Self {
        Self::new(ValueKind::Bool, [u8::from(value)].to_vec())
    }

    pub fn null() -> Self {
        Self::new(ValueKind::Null, Bytes::new())
    }

    pub fn structured(bytes: impl Into<Bytes>) -> Self {
        Self::new(ValueKind::Structured, bytes)
    }

    pub fn kind(&self) -> ValueKind {
        self.kind
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::binary(value)
    }
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Self::binary(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::utf8(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::utf8(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::float(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::bool(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_carries_kind_and_bytes() {
        let value = Value::integer(42);

        assert_eq!(value.kind(), ValueKind::Integer);
        assert_eq!(value.bytes(), &Bytes::from(42_i64.to_le_bytes().to_vec()));
    }
}
