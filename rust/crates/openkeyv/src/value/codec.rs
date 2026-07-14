use super::StructuredValue;
use crate::error::{Error, Result};
use bytes::Bytes;
use std::collections::BTreeSet;

const STRUCTURED_MAGIC: &[u8; 4] = b"OKV1";
const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_INT: u8 = 3;
const TAG_FLOAT: u8 = 4;
const TAG_STR: u8 = 5;
const TAG_BYTES: u8 = 6;
const TAG_LIST: u8 = 7;
const TAG_DICT: u8 = 8;
const TAG_UINT: u8 = 9;

pub(crate) fn encode(value: &StructuredValue) -> Result<Bytes> {
    let mut out = Vec::with_capacity(STRUCTURED_MAGIC.len() + 32);
    out.extend_from_slice(STRUCTURED_MAGIC);
    encode_value(value, &mut out)?;
    Ok(Bytes::from(out))
}

pub(crate) fn decode(bytes: &[u8]) -> Result<StructuredValue> {
    if !bytes.starts_with(STRUCTURED_MAGIC) {
        return Err(Error::Deserialization(
            "invalid structured value header".to_string(),
        ));
    }
    let mut cursor = Cursor::new(&bytes[STRUCTURED_MAGIC.len()..]);
    let value = decode_value(&mut cursor)?;
    if cursor.remaining() != 0 {
        return Err(Error::Deserialization(
            "trailing bytes in structured value".to_string(),
        ));
    }
    Ok(value)
}

fn encode_value(value: &StructuredValue, out: &mut Vec<u8>) -> Result<()> {
    match value {
        StructuredValue::Null => out.push(TAG_NULL),
        StructuredValue::Bool(false) => out.push(TAG_FALSE),
        StructuredValue::Bool(true) => out.push(TAG_TRUE),
        StructuredValue::Integer(value) => {
            out.push(TAG_INT);
            out.extend_from_slice(&value.to_le_bytes());
        }
        StructuredValue::UnsignedInteger(value) => {
            out.push(TAG_UINT);
            out.extend_from_slice(&value.to_le_bytes());
        }
        StructuredValue::Float(value) => {
            out.push(TAG_FLOAT);
            out.extend_from_slice(&value.to_le_bytes());
        }
        StructuredValue::String(value) => {
            out.push(TAG_STR);
            write_len(out, value.len())?;
            out.extend_from_slice(value.as_bytes());
        }
        StructuredValue::Bytes(value) => {
            out.push(TAG_BYTES);
            write_len(out, value.len())?;
            out.extend_from_slice(value);
        }
        StructuredValue::List(values) => {
            out.push(TAG_LIST);
            write_len(out, values.len())?;
            for value in values {
                encode_value(value, out)?;
            }
        }
        StructuredValue::Dict(entries) => {
            out.push(TAG_DICT);
            write_len(out, entries.len())?;
            let mut entries: Vec<_> = entries.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let mut previous_key: Option<&str> = None;
            for (key, value) in entries {
                if previous_key == Some(key.as_str()) {
                    return Err(Error::Serialization(format!(
                        "duplicate structured dict key: {key}"
                    )));
                }
                previous_key = Some(key);
                write_len(out, key.len())?;
                out.extend_from_slice(key.as_bytes());
                encode_value(value, out)?;
            }
        }
    }
    Ok(())
}

fn decode_value(cursor: &mut Cursor<'_>) -> Result<StructuredValue> {
    match cursor.read_u8()? {
        TAG_NULL => Ok(StructuredValue::Null),
        TAG_FALSE => Ok(StructuredValue::Bool(false)),
        TAG_TRUE => Ok(StructuredValue::Bool(true)),
        TAG_INT => Ok(StructuredValue::Integer(i64::from_le_bytes(
            cursor.read_array::<8>()?,
        ))),
        TAG_UINT => Ok(StructuredValue::UnsignedInteger(u64::from_le_bytes(
            cursor.read_array::<8>()?,
        ))),
        TAG_FLOAT => Ok(StructuredValue::Float(f64::from_le_bytes(
            cursor.read_array::<8>()?,
        ))),
        TAG_STR => {
            let len = cursor.read_len()?;
            let bytes = cursor.read_bytes(len)?;
            let text = std::str::from_utf8(bytes)
                .map_err(|e| Error::Deserialization(format!("invalid structured UTF-8: {e}")))?;
            Ok(StructuredValue::String(text.to_string()))
        }
        TAG_BYTES => {
            let len = cursor.read_len()?;
            Ok(StructuredValue::Bytes(Bytes::copy_from_slice(
                cursor.read_bytes(len)?,
            )))
        }
        TAG_LIST => {
            let len = cursor.read_len()?;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(decode_value(cursor)?);
            }
            Ok(StructuredValue::List(values))
        }
        TAG_DICT => {
            let len = cursor.read_len()?;
            let mut entries = Vec::with_capacity(len);
            let mut keys = BTreeSet::new();
            for _ in 0..len {
                let key_len = cursor.read_len()?;
                let key_bytes = cursor.read_bytes(key_len)?;
                let key = std::str::from_utf8(key_bytes).map_err(|e| {
                    Error::Deserialization(format!("invalid structured dict key: {e}"))
                })?;
                if !keys.insert(key.to_string()) {
                    return Err(Error::Deserialization(format!(
                        "duplicate structured dict key: {key}"
                    )));
                }
                entries.push((key.to_string(), decode_value(cursor)?));
            }
            Ok(StructuredValue::Dict(entries))
        }
        tag => Err(Error::Deserialization(format!(
            "unknown structured value tag: {tag}"
        ))),
    }
}

fn write_len(out: &mut Vec<u8>, len: usize) -> Result<()> {
    let len = u32::try_from(len)
        .map_err(|_| Error::Serialization("structured value is too large".to_string()))?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    fn read_len(&mut self) -> Result<usize> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?) as usize)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.read_bytes(N)?;
        Ok(bytes.try_into().expect("slice length checked"))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(len).ok_or_else(|| {
            Error::Deserialization("structured value length overflow".to_string())
        })?;
        if end > self.bytes.len() {
            return Err(Error::Deserialization(
                "truncated structured value".to_string(),
            ));
        }
        let bytes = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dict_encoding_is_key_sorted() {
        let left = StructuredValue::Dict(vec![
            ("b".to_string(), StructuredValue::Integer(2)),
            ("a".to_string(), StructuredValue::Integer(1)),
        ]);
        let right = StructuredValue::Dict(vec![
            ("a".to_string(), StructuredValue::Integer(1)),
            ("b".to_string(), StructuredValue::Integer(2)),
        ]);

        assert_eq!(encode(&left).unwrap(), encode(&right).unwrap());
    }

    #[test]
    fn duplicate_dict_keys_are_rejected_when_encoding() {
        let value = StructuredValue::Dict(vec![
            ("key".to_string(), StructuredValue::Integer(1)),
            ("key".to_string(), StructuredValue::Integer(2)),
        ]);

        assert!(encode(&value).is_err());
    }

    #[test]
    fn duplicate_dict_keys_are_rejected_when_decoding() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(STRUCTURED_MAGIC);
        bytes.push(TAG_DICT);
        bytes.extend_from_slice(&2_u32.to_le_bytes());
        for value in [1_i64, 2_i64] {
            bytes.extend_from_slice(&3_u32.to_le_bytes());
            bytes.extend_from_slice(b"key");
            bytes.push(TAG_INT);
            bytes.extend_from_slice(&value.to_le_bytes());
        }

        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn unsigned_integer_tag_is_appended_and_roundtrips_nested_values() {
        let value = StructuredValue::Dict(vec![(
            "values".to_string(),
            StructuredValue::List(vec![
                StructuredValue::Integer(i64::MAX),
                StructuredValue::UnsignedInteger(u64::MAX),
            ]),
        )]);

        let encoded = encode(&value).unwrap();

        assert!(
            encoded
                .windows(9)
                .any(|bytes| bytes[0] == 9 && bytes[1..] == u64::MAX.to_le_bytes())
        );
        assert_eq!(decode(&encoded).unwrap(), value);
    }

    #[test]
    fn truncated_unsigned_integer_is_rejected() {
        let mut bytes = Vec::from(STRUCTURED_MAGIC.as_slice());
        bytes.push(9);
        bytes.extend_from_slice(&u64::MAX.to_le_bytes()[..7]);

        let err = decode(&bytes).unwrap_err();

        assert!(err.to_string().contains("truncated structured value"));
    }

    #[test]
    fn unknown_structured_tag_is_rejected() {
        let mut bytes = Vec::from(STRUCTURED_MAGIC.as_slice());
        bytes.push(10);

        let err = decode(&bytes).unwrap_err();

        assert!(err.to_string().contains("unknown structured value tag"));
    }
}
