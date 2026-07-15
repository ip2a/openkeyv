mod codec;
#[cfg(feature = "json")]
mod json;
mod kind;
mod structured;

use crate::error::{Error, Result};
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
    /// Construct a value from a raw kind and payload after validating the payload.
    pub fn new(kind: ValueKind, bytes: impl Into<Bytes>) -> Result<Self> {
        let bytes = bytes.into();
        validate_payload(kind, &bytes)?;
        Ok(Self::new_unchecked(kind, bytes))
    }

    /// Construct a value without validating that the payload matches its kind.
    ///
    /// Prefer the typed constructors or [`Value::new`] at protocol boundaries.
    /// This method is intended for bytes that were produced by an already checked
    /// OpenKeyV path and does not provide protocol validity by itself.
    pub fn new_unchecked(kind: ValueKind, bytes: impl Into<Bytes>) -> Self {
        Self {
            kind,
            bytes: bytes.into(),
        }
    }

    pub fn binary(bytes: impl Into<Bytes>) -> Self {
        Self::new_unchecked(ValueKind::Binary, bytes)
    }

    pub fn utf8(value: impl Into<String>) -> Self {
        Self::new_unchecked(ValueKind::Utf8, value.into())
    }

    pub fn integer(value: i64) -> Self {
        Self::new_unchecked(ValueKind::Integer, value.to_le_bytes().to_vec())
    }

    pub fn unsigned_integer(value: u64) -> Self {
        Self::new_unchecked(ValueKind::UnsignedInteger, value.to_le_bytes().to_vec())
    }

    pub fn float(value: f64) -> Self {
        Self::new_unchecked(ValueKind::Float, value.to_le_bytes().to_vec())
    }

    pub fn bool(value: bool) -> Self {
        Self::new_unchecked(ValueKind::Bool, [u8::from(value)].to_vec())
    }

    pub fn null() -> Self {
        Self::new_unchecked(ValueKind::Null, Bytes::new())
    }

    /// Encode a structured value into a checked `Value`.
    pub fn from_structured(value: &StructuredValue) -> Result<Self> {
        Ok(Self::structured_unchecked(value.encode()?))
    }

    /// Decode this value as a structured value.
    pub fn decode_structured(&self) -> Result<StructuredValue> {
        if self.kind != ValueKind::Structured {
            return Err(Error::InvalidValue(format!(
                "expected Structured kind, got {:?}",
                self.kind
            )));
        }
        StructuredValue::decode(&self.bytes)
    }

    /// Construct a structured value from raw OKV1 bytes without validating them.
    ///
    /// Prefer [`Value::from_structured`] for normal construction or [`Value::new`]
    /// when accepting raw protocol bytes.
    pub fn structured_unchecked(bytes: impl Into<Bytes>) -> Self {
        Self::new_unchecked(ValueKind::Structured, bytes)
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

fn validate_payload(kind: ValueKind, bytes: &[u8]) -> Result<()> {
    match kind {
        ValueKind::Binary => Ok(()),
        ValueKind::Utf8 => std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|error| Error::InvalidValue(format!("invalid UTF-8 payload: {error}"))),
        ValueKind::Integer => validate_fixed_width(bytes, 8, "integer"),
        ValueKind::UnsignedInteger => validate_fixed_width(bytes, 8, "unsigned integer"),
        ValueKind::Float => validate_fixed_width(bytes, 8, "float"),
        ValueKind::Bool if matches!(bytes, [0] | [1]) => Ok(()),
        ValueKind::Bool => Err(Error::InvalidValue(
            "bool payload must be exactly one byte containing 0 or 1".to_string(),
        )),
        ValueKind::Null if bytes.is_empty() => Ok(()),
        ValueKind::Null => Err(Error::InvalidValue(
            "null payload must be empty".to_string(),
        )),
        ValueKind::Structured => StructuredValue::decode(bytes)
            .map(|_| ())
            .map_err(|error| Error::InvalidValue(format!("invalid structured payload: {error}"))),
    }
}

fn validate_fixed_width(bytes: &[u8], expected: usize, label: &str) -> Result<()> {
    if bytes.len() == expected {
        Ok(())
    } else {
        Err(Error::InvalidValue(format!(
            "invalid {label} payload length: expected {expected} bytes, got {}",
            bytes.len()
        )))
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

    #[test]
    fn checked_constructor_accepts_valid_payloads() {
        let structured = StructuredValue::List(vec![StructuredValue::UnsignedInteger(u64::MAX)]);
        let structured_bytes = structured.encode().unwrap();
        let values = [
            (ValueKind::Binary, Bytes::from_static(&[0, 1, 2])),
            (ValueKind::Utf8, Bytes::from_static(b"hello")),
            (
                ValueKind::Integer,
                Bytes::copy_from_slice(&i64::MIN.to_le_bytes()),
            ),
            (
                ValueKind::UnsignedInteger,
                Bytes::copy_from_slice(&u64::MAX.to_le_bytes()),
            ),
            (
                ValueKind::Float,
                Bytes::copy_from_slice(&1.5_f64.to_le_bytes()),
            ),
            (ValueKind::Bool, Bytes::from_static(&[1])),
            (ValueKind::Null, Bytes::new()),
            (ValueKind::Structured, structured_bytes),
        ];

        for (kind, bytes) in values {
            let value = Value::new(kind, bytes.clone()).unwrap();

            assert_eq!(value.kind(), kind);
            assert_eq!(value.bytes(), &bytes);
        }
    }

    #[test]
    fn checked_constructor_rejects_invalid_payloads() {
        let invalid_values = [
            (ValueKind::Utf8, Bytes::from_static(&[0xff])),
            (ValueKind::Integer, Bytes::from_static(&[0; 7])),
            (ValueKind::UnsignedInteger, Bytes::from_static(&[0; 9])),
            (ValueKind::Float, Bytes::new()),
            (ValueKind::Bool, Bytes::new()),
            (ValueKind::Bool, Bytes::from_static(&[2])),
            (ValueKind::Bool, Bytes::from_static(&[0, 1])),
            (ValueKind::Null, Bytes::from_static(&[0])),
            (ValueKind::Structured, Bytes::from_static(b"OKV1")),
            (ValueKind::Structured, Bytes::from_static(b"OKV1\x00\x00")),
        ];

        for (kind, bytes) in invalid_values {
            assert!(Value::new(kind, bytes).is_err(), "kind {kind:?}");
        }
    }

    #[test]
    fn structured_value_uses_checked_roundtrip_api() {
        let structured = StructuredValue::Dict(vec![(
            "value".to_string(),
            StructuredValue::UnsignedInteger(u64::MAX),
        )]);

        let value = Value::from_structured(&structured).unwrap();

        assert_eq!(value.kind(), ValueKind::Structured);
        assert_eq!(value.decode_structured().unwrap(), structured);
        assert!(Value::binary(Bytes::new()).decode_structured().is_err());
    }

    #[test]
    fn unchecked_constructor_is_explicit() {
        let value = Value::new_unchecked(ValueKind::Integer, Bytes::new());
        let structured = Value::structured_unchecked(Bytes::from_static(b"invalid"));

        assert!(value.bytes().is_empty());
        assert!(structured.decode_structured().is_err());
    }
}
