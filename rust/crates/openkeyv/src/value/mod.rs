mod codec;
mod kind;
mod structured;

use bytes::Bytes;

pub use kind::ValueKind;
pub use structured::StructuredValue;

/// Protocol-level value stored by the Rust core.
///
/// The core treats values as typed bytes. Structured values are represented by
/// `StructuredValue` before being encoded into the `Structured` byte kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    kind: ValueKind,
    bytes: Bytes,
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

    pub fn unsigned_integer(value: u64) -> Self {
        Self::new(ValueKind::UnsignedInteger, value.to_le_bytes().to_vec())
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

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::unsigned_integer(value)
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

    #[test]
    fn unsigned_integer_uses_exact_little_endian_bytes() {
        let value = Value::unsigned_integer(u64::MAX);

        assert_eq!(value.kind(), ValueKind::UnsignedInteger);
        assert_eq!(value.bytes().as_ref(), &u64::MAX.to_le_bytes());
        assert_eq!(Value::from(u64::MAX), value);
    }
}
