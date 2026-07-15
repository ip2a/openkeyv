//! Optional loss-checked conversion between [`StructuredValue`] and
//! [`serde_json::Value`].
//!
//! This module is only available when the `json` Cargo feature is enabled.
//! The conversion is strict: every variant must have a natural, lossless
//! counterpart in the JSON type system. Binary blobs do not and are rejected
//! rather than silently base64-encoded.

use serde_json::Value as JsonValue;

use crate::error::{Error, Result};

use super::StructuredValue;

impl StructuredValue {
    /// Convert this structured value into a [`serde_json::Value`].
    ///
    /// Returns `Err` for [`StructuredValue::Bytes`] because JSON has no native
    /// binary type; encoding it as a string or array would be a lossy
    /// convention, not a type-preserving conversion.
    pub fn to_json(&self) -> Result<JsonValue> {
        to_json(self)
    }

    /// Construct a structured value from a checked [`serde_json::Value`].
    ///
    /// Integers that fit in `i64` become [`StructuredValue::Integer`]; integers
    /// in the `i64`–`u64` overflow range become [`StructuredValue::UnsignedInteger`]
    /// without truncation.
    pub fn from_json(value: &JsonValue) -> Result<Self> {
        from_json(value)
    }
}

fn to_json(value: &StructuredValue) -> Result<JsonValue> {
    Ok(match value {
        StructuredValue::Null => JsonValue::Null,
        StructuredValue::Bool(b) => JsonValue::Bool(*b),
        StructuredValue::Integer(n) => JsonValue::Number((*n).into()),
        StructuredValue::UnsignedInteger(n) => {
            let number = serde_json::Number::from(*n);
            JsonValue::Number(number)
        }
        StructuredValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                Error::Serialization(format!("float {f} is not representable in JSON"))
            })?,
        StructuredValue::String(s) => JsonValue::String(s.clone()),
        StructuredValue::Bytes(_) => {
            return Err(Error::Serialization(
                "StructuredValue::Bytes has no lossless JSON representation".to_string(),
            ));
        }
        StructuredValue::List(items) => {
            let array = items.iter().map(to_json).collect::<Result<Vec<_>>>()?;
            JsonValue::Array(array)
        }
        StructuredValue::Dict(entries) => {
            let mut map = serde_json::Map::with_capacity(entries.len());
            for (key, val) in entries {
                if map.insert(key.clone(), to_json(val)?).is_some() {
                    return Err(Error::Serialization(format!(
                        "duplicate structured dict key: {key}"
                    )));
                }
            }
            JsonValue::Object(map)
        }
    })
}

fn from_json(value: &JsonValue) -> Result<StructuredValue> {
    Ok(match value {
        JsonValue::Null => StructuredValue::Null,
        JsonValue::Bool(b) => StructuredValue::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    StructuredValue::Integer(u as i64)
                } else {
                    StructuredValue::UnsignedInteger(u)
                }
            } else if let Some(i) = n.as_i64() {
                StructuredValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                StructuredValue::Float(f)
            } else {
                return Err(Error::Deserialization(format!(
                    "JSON number {n} is not representable as i64, u64, or f64"
                )));
            }
        }
        JsonValue::String(s) => StructuredValue::String(s.clone()),
        JsonValue::Array(items) => {
            let list = items.iter().map(from_json).collect::<Result<Vec<_>>>()?;
            StructuredValue::List(list)
        }
        JsonValue::Object(map) => {
            let entries = map
                .iter()
                .map(|(key, val)| from_json(val).map(|sv| (key.clone(), sv)))
                .collect::<Result<Vec<_>>>()?;
            StructuredValue::Dict(entries)
        }
    })
}

impl TryFrom<&StructuredValue> for JsonValue {
    type Error = Error;

    fn try_from(value: &StructuredValue) -> Result<Self> {
        to_json(value)
    }
}

impl TryFrom<&JsonValue> for StructuredValue {
    type Error = Error;

    fn try_from(value: &JsonValue) -> Result<Self> {
        from_json(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn null_roundtrips() {
        let sv = StructuredValue::Null;
        let json = sv.to_json().unwrap();
        assert_eq!(json, JsonValue::Null);
        assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
    }

    #[test]
    fn bool_roundtrips() {
        for b in [false, true] {
            let sv = StructuredValue::Bool(b);
            let json = sv.to_json().unwrap();
            assert_eq!(json, JsonValue::Bool(b));
            assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
        }
    }

    #[test]
    fn integer_roundtrips() {
        for n in [0_i64, -1, 1, i64::MAX, i64::MIN] {
            let sv = StructuredValue::Integer(n);
            let json = sv.to_json().unwrap();
            assert_eq!(json.as_i64().unwrap(), n);
            assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
        }
    }

    #[test]
    fn unsigned_integer_overflow_range_maps_losslessly() {
        let n = u64::MAX;
        let sv = StructuredValue::UnsignedInteger(n);
        let json = sv.to_json().unwrap();
        // serde_json preserves the u64 without truncating to i64.
        assert_eq!(json.as_u64().unwrap(), n);
        // Round-trip must reconstruct UnsignedInteger, not truncate to Integer.
        let back = StructuredValue::from_json(&json).unwrap();
        assert_eq!(back, sv);
    }

    #[test]
    fn boundary_i64_max_stays_integer() {
        let sv = StructuredValue::Integer(i64::MAX);
        let json = sv.to_json().unwrap();
        let back = StructuredValue::from_json(&json).unwrap();
        assert_eq!(back, StructuredValue::Integer(i64::MAX));
    }

    #[test]
    fn boundary_i64_max_plus_one_becomes_unsigned() {
        let n = i64::MAX as u64 + 1;
        let json = serde_json::Value::Number(n.into());
        let sv = StructuredValue::from_json(&json).unwrap();
        assert_eq!(sv, StructuredValue::UnsignedInteger(n));
    }

    #[test]
    fn float_roundtrips() {
        for f in [0.0_f64, -1.5, 3.14159, f64::INFINITY] {
            // Infinity cannot be represented in JSON and must error.
            if f.is_infinite() {
                assert!(StructuredValue::Float(f).to_json().is_err());
                continue;
            }
            let sv = StructuredValue::Float(f);
            let json = sv.to_json().unwrap();
            assert_eq!(json.as_f64().unwrap(), f);
            assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
        }
    }

    #[test]
    fn nan_rejected() {
        assert!(StructuredValue::Float(f64::NAN).to_json().is_err());
    }

    #[test]
    fn string_roundtrips() {
        let sv = StructuredValue::String("héllo 世界".to_string());
        let json = sv.to_json().unwrap();
        assert_eq!(json, JsonValue::String("héllo 世界".to_string()));
        assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
    }

    #[test]
    fn bytes_rejected() {
        let sv = StructuredValue::Bytes(Bytes::from_static(&[0, 1, 2]));
        assert!(sv.to_json().is_err());
    }

    #[test]
    fn list_roundtrips() {
        let sv = StructuredValue::List(vec![
            StructuredValue::Integer(1),
            StructuredValue::String("two".to_string()),
            StructuredValue::Bool(true),
            StructuredValue::Null,
        ]);
        let json = sv.to_json().unwrap();
        assert_eq!(json.as_array().unwrap().len(), 4);
        assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
    }

    #[test]
    fn dict_roundtrips() {
        let sv = StructuredValue::Dict(vec![
            ("a".to_string(), StructuredValue::Integer(1)),
            ("b".to_string(), StructuredValue::UnsignedInteger(u64::MAX)),
            ("c".to_string(), StructuredValue::Float(2.5)),
        ]);
        let json = sv.to_json().unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 3);
        assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
    }

    #[test]
    fn nested_structures_roundtrip() {
        let sv = StructuredValue::Dict(vec![
            (
                "list".to_string(),
                StructuredValue::List(vec![
                    StructuredValue::Integer(-1),
                    StructuredValue::UnsignedInteger(u64::MAX),
                ]),
            ),
            (
                "nested".to_string(),
                StructuredValue::Dict(vec![(
                    "key".to_string(),
                    StructuredValue::String("value".to_string()),
                )]),
            ),
        ]);
        let json = sv.to_json().unwrap();
        assert_eq!(StructuredValue::from_json(&json).unwrap(), sv);
    }

    #[test]
    fn duplicate_dict_keys_rejected_in_to_json() {
        let sv = StructuredValue::Dict(vec![
            ("k".to_string(), StructuredValue::Integer(1)),
            ("k".to_string(), StructuredValue::Integer(2)),
        ]);
        assert!(sv.to_json().is_err());
    }

    #[test]
    fn try_from_traits_work() {
        let sv = StructuredValue::Dict(vec![(
            "version".to_string(),
            StructuredValue::UnsignedInteger(u64::MAX),
        )]);
        let json: JsonValue = (&sv).try_into().unwrap();
        let back: StructuredValue = (&json).try_into().unwrap();
        assert_eq!(back, sv);
    }

    #[test]
    fn json_array_with_mixed_types_roundtrips() {
        let json = serde_json::json!([null, true, 42, -7, "text", [1, 2], {"k": "v"}]);
        let sv = StructuredValue::from_json(&json).unwrap();
        let back = sv.to_json().unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn full_roundtrip_preserves_u64_max() {
        let sv = StructuredValue::Dict(vec![(
            "version".to_string(),
            StructuredValue::UnsignedInteger(u64::MAX),
        )]);
        let json = sv.to_json().unwrap();
        let decoded = StructuredValue::from_json(&json).unwrap();
        assert_eq!(decoded, sv);
    }
}
