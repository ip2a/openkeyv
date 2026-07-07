use super::ManagedEntry;
use crate::error::{Error, Result};
use crate::value::{Value, ValueKind};
use chrono::{DateTime, TimeZone, Utc};

const MAGIC: &[u8; 5] = b"OKVE1";
const FLAG_CREATED_AT: u8 = 0b0000_0001;
const FLAG_EXPIRES_AT: u8 = 0b0000_0010;

pub(super) fn encode(entry: &ManagedEntry) -> Result<Vec<u8>> {
    let value_bytes = entry.value.bytes();
    let value_len = u64::try_from(value_bytes.len())
        .map_err(|_| Error::Serialization("entry value exceeds u64 length".to_string()))?;

    let mut flags = 0;
    if entry.created_at.is_some() {
        flags |= FLAG_CREATED_AT;
    }
    if entry.expires_at.is_some() {
        flags |= FLAG_EXPIRES_AT;
    }

    let metadata_len =
        usize::from(entry.created_at.is_some()) * 8 + usize::from(entry.expires_at.is_some()) * 8;
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + 1 + 8 + metadata_len + value_bytes.len());

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
    Ok(out)
}

pub(super) fn decode(bytes: &[u8]) -> Result<ManagedEntry> {
    let mut offset = 0;
    let magic = read_array::<5>(bytes, &mut offset)?;
    if &magic != MAGIC {
        return Err(Error::Deserialization(
            "invalid OpenKeyV entry magic".to_string(),
        ));
    }

    let kind_tag = read_u8(bytes, &mut offset)?;
    let kind = ValueKind::from_tag(kind_tag).ok_or_else(|| {
        Error::Deserialization(format!("invalid OpenKeyV entry value kind: {}", kind_tag))
    })?;

    let flags = read_u8(bytes, &mut offset)?;
    if flags & !(FLAG_CREATED_AT | FLAG_EXPIRES_AT) != 0 {
        return Err(Error::Deserialization(format!(
            "invalid OpenKeyV entry flags: {}",
            flags
        )));
    }

    let value_len = u64::from_le_bytes(read_array::<8>(bytes, &mut offset)?) as usize;
    let created_at = if flags & FLAG_CREATED_AT != 0 {
        Some(read_datetime(bytes, &mut offset)?)
    } else {
        None
    };
    let expires_at = if flags & FLAG_EXPIRES_AT != 0 {
        Some(read_datetime(bytes, &mut offset)?)
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
        value: Value::new(kind, bytes[offset..end].to_vec()),
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

        let encoded = encode(&entry).unwrap();
        let decoded = decode(&encoded).unwrap();

        assert_eq!(decoded, entry);
    }

    #[test]
    fn entry_codec_rejects_json_payloads() {
        let err = decode(br#"{"value":null}"#).unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[test]
    fn entry_codec_rejects_unknown_value_kind() {
        let mut encoded = encode(&ManagedEntry::new(Value::null())).unwrap();
        encoded[MAGIC.len()] = 250;

        let err = decode(&encoded).unwrap_err();

        assert!(
            err.to_string()
                .contains("invalid OpenKeyV entry value kind")
        );
    }
}
