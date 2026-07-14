use super::ManagedEntry;
use crate::error::{Error, Result};
use crate::value::{Value, ValueKind};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};

const MAGIC: &[u8; 5] = b"OKVE1";
const FLAG_CREATED_AT: u8 = 0b0000_0001;
const FLAG_EXPIRES_AT: u8 = 0b0000_0010;
const FIXED_HEADER_LEN: usize = MAGIC.len() + 1 + 1 + 8;

pub(super) fn encode(entry: &ManagedEntry) -> Vec<u8> {
    let value_bytes = entry.value.bytes();
    let value_len = value_bytes.len() as u64;

    let mut flags = 0;
    if entry.created_at.is_some() {
        flags |= FLAG_CREATED_AT;
    }
    if entry.expires_at.is_some() {
        flags |= FLAG_EXPIRES_AT;
    }

    let metadata_len =
        usize::from(entry.created_at.is_some()) * 8 + usize::from(entry.expires_at.is_some()) * 8;
    let mut out = Vec::with_capacity(FIXED_HEADER_LEN + metadata_len + value_bytes.len());

    out.extend_from_slice(MAGIC);
    out.push(entry.value.kind().tag());
    out.push(flags);
    out.extend_from_slice(&value_len.to_le_bytes());

    if let Some(created_at) = entry.created_at {
        out.extend_from_slice(&created_at.timestamp_millis().to_le_bytes());
    }
    if let Some(expires_at) = entry.expires_at {
        out.extend_from_slice(&expires_at.timestamp_millis().to_le_bytes());
    }

    out.extend_from_slice(value_bytes);
    out
}

pub(super) fn decode(bytes: Bytes) -> Result<ManagedEntry> {
    let mut offset = 0;
    let magic = read_array::<5>(&bytes, &mut offset)?;
    if &magic != MAGIC {
        return Err(Error::Deserialization(
            "invalid OpenKeyV entry magic".to_string(),
        ));
    }

    let kind_tag = read_u8(&bytes, &mut offset)?;
    let kind = ValueKind::from_tag(kind_tag).ok_or_else(|| {
        Error::Deserialization(format!("invalid OpenKeyV entry value kind: {}", kind_tag))
    })?;

    let flags = read_u8(&bytes, &mut offset)?;
    if flags & !(FLAG_CREATED_AT | FLAG_EXPIRES_AT) != 0 {
        return Err(Error::Deserialization(format!(
            "invalid OpenKeyV entry flags: {}",
            flags
        )));
    }

    let encoded_value_len = u64::from_le_bytes(read_array::<8>(&bytes, &mut offset)?);
    let value_len = usize::try_from(encoded_value_len).map_err(|_| {
        Error::Deserialization(format!(
            "OpenKeyV entry value length does not fit this platform: {}",
            encoded_value_len
        ))
    })?;
    let created_at = if flags & FLAG_CREATED_AT != 0 {
        Some(read_datetime(&bytes, &mut offset)?)
    } else {
        None
    };
    let expires_at = if flags & FLAG_EXPIRES_AT != 0 {
        Some(read_datetime(&bytes, &mut offset)?)
    } else {
        None
    };

    let end = offset
        .checked_add(value_len)
        .ok_or_else(|| Error::Deserialization("OpenKeyV entry length overflow".to_string()))?;
    if end != bytes.len() {
        return Err(Error::Deserialization(
            "OpenKeyV entry length does not match payload".to_string(),
        ));
    }

    Ok(ManagedEntry {
        value: Value::new(kind, bytes.slice(offset..end)),
        created_at,
        expires_at,
    })
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8> {
    let value = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| Error::Deserialization("unexpected end of OpenKeyV entry".to_string()))?;
    *offset += 1;
    Ok(value)
}

fn read_array<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| Error::Deserialization("OpenKeyV entry offset overflow".to_string()))?;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| Error::Deserialization("unexpected end of OpenKeyV entry".to_string()))?;
    *offset = end;
    Ok(slice.try_into().expect("slice length is checked"))
}

fn read_datetime(bytes: &[u8], offset: &mut usize) -> Result<DateTime<Utc>> {
    let millis = i64::from_le_bytes(read_array::<8>(bytes, offset)?);
    Utc.timestamp_millis_opt(millis).single().ok_or_else(|| {
        Error::Deserialization(format!("invalid OpenKeyV entry timestamp: {}", millis))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[test]
    fn entry_codec_roundtrips_value_and_ttl_metadata() {
        let created_at = Utc.with_ymd_and_hms(2026, 7, 7, 12, 0, 0).single().unwrap();
        let entry = ManagedEntry {
            value: Value::utf8("Alice"),
            created_at: Some(created_at),
            expires_at: Some(created_at + TimeDelta::seconds(30)),
        };

        let encoded = encode(&entry);
        let decoded = decode(encoded.into()).unwrap();

        assert_eq!(decoded, entry);
    }

    #[test]
    fn entry_codec_rejects_json_payloads() {
        let err = decode(Bytes::from_static(br#"{"value":null}"#)).unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[test]
    fn entry_codec_rejects_unknown_value_kind() {
        let mut encoded = encode(&ManagedEntry::new(Value::null()));
        encoded[MAGIC.len()] = 250;

        let err = decode(encoded.into()).unwrap_err();

        assert!(
            err.to_string()
                .contains("invalid OpenKeyV entry value kind")
        );
    }

    #[test]
    fn entry_codec_roundtrips_every_value_kind() {
        let values = [
            Value::binary(Bytes::from_static(&[0, 1, 2])),
            Value::utf8("hello"),
            Value::integer(i64::MIN),
            Value::unsigned_integer(u64::MAX),
            Value::float(std::f64::consts::PI),
            Value::bool(true),
            Value::null(),
            Value::structured(Bytes::from_static(&[9, 8, 7])),
        ];

        for value in values {
            let entry = ManagedEntry {
                value,
                created_at: None,
                expires_at: None,
            };

            let decoded = decode(encode(&entry).into()).unwrap();

            assert_eq!(decoded, entry);
        }
    }

    #[test]
    fn entry_codec_rejects_unknown_flags() {
        let mut encoded = encode(&ManagedEntry::new(Value::null()));
        encoded[MAGIC.len() + 1] = 0b1000_0000;

        let err = decode(encoded.into()).unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry flags"));
    }

    #[test]
    fn entry_codec_rejects_truncated_data() {
        let encoded = encode(&ManagedEntry::with_ttl(Value::utf8("value"), 30.0).unwrap());

        for end in 0..encoded.len() {
            assert!(decode(Bytes::copy_from_slice(&encoded[..end])).is_err());
        }
    }

    #[test]
    fn entry_codec_rejects_trailing_data() {
        let mut encoded = encode(&ManagedEntry::new(Value::null()));
        encoded.push(0);

        let err = decode(encoded.into()).unwrap_err();

        assert!(
            err.to_string()
                .contains("entry length does not match payload")
        );
    }

    #[test]
    fn entry_codec_keeps_value_payload_zero_copy() {
        let entry = ManagedEntry {
            value: Value::binary(Bytes::from_static(&[1, 2, 3, 4])),
            created_at: None,
            expires_at: None,
        };
        let encoded = Bytes::from(encode(&entry));
        let payload_ptr = encoded[FIXED_HEADER_LEN..].as_ptr();

        let decoded = decode(encoded).unwrap();

        assert_eq!(decoded.value.bytes().as_ptr(), payload_ptr);
    }
}
