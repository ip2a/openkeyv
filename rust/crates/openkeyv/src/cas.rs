//! CAS revision envelope used by networked CAS-capable backends.
//!
//! Memory co-locates value and revision in a process-local struct, but Redis and
//! Valkey must persist the revision in the same value bytes so that reconnects and
//! server-side persistence preserve it. Per the CAS/revision ADR (section 7), the
//! wire layout is one strict binary envelope:
//!
//! ```text
//! 5 bytes   magic: "OKVC1"
//! 16 bytes  revision token
//! remaining OKVE1 ManagedEntry bytes
//! ```
//!
//! The revision sits at a fixed offset so server-side scripts can compare it
//! without decoding the managed entry or the application payload. There is no
//! metadata key, no auxiliary revision key, and no fallback to raw `OKVE1`.

use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::Revision;
use crate::value::Value;
use bytes::Bytes;

/// CAS envelope magic bytes.
pub(crate) const MAGIC: &[u8; 5] = b"OKVC1";

/// Number of bytes preceding the revision token: the magic header.
pub(crate) const MAGIC_LEN: usize = MAGIC.len();

/// Fixed prefix length that every CAS-capable value starts with.
pub(crate) const PREFIX_LEN: usize = MAGIC_LEN + Revision::BYTE_LEN;

/// Encode a managed entry and revision into the CAS-capable `OKVC1` envelope.
pub(crate) fn encode(entry: &ManagedEntry, revision: Revision) -> Vec<u8> {
    let entry_bytes = entry.encode();
    let mut out = Vec::with_capacity(PREFIX_LEN + entry_bytes.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(revision.as_bytes());
    out.extend_from_slice(&entry_bytes);
    out
}

/// An entry and revision decoded from a single `OKVC1` envelope.
#[derive(Debug)]
pub(crate) struct CasEntry {
    pub entry: ManagedEntry,
    pub revision: Revision,
}

/// Decode a `OKVC1` envelope into its managed entry and revision.
///
/// Returns `Ok(None)` only when the stored value is absent (the caller observed
/// no bytes). A present byte sequence that is not a valid `OKVC1` envelope is a
/// strict deserialization error: raw `OKVE1`, JSON, a truncated or malformed
/// `OKVC1`, or trailing/invalid nested entry data all fail directly.
pub(crate) fn decode(bytes: Option<Vec<u8>>) -> Result<Option<CasEntry>> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    if bytes.len() < PREFIX_LEN {
        return Err(Error::Deserialization(format!(
            "CAS envelope too short: {} bytes (minimum {})",
            bytes.len(),
            PREFIX_LEN
        )));
    }
    if &bytes[..MAGIC_LEN] != MAGIC {
        return Err(Error::Deserialization(
            "invalid CAS envelope magic".to_string(),
        ));
    }
    let mut revision_bytes = [0u8; Revision::BYTE_LEN];
    revision_bytes.copy_from_slice(&bytes[MAGIC_LEN..PREFIX_LEN]);
    let revision = Revision::from_bytes(revision_bytes);
    let entry = ManagedEntry::decode(Bytes::copy_from_slice(&bytes[PREFIX_LEN..]))?;
    Ok(Some(CasEntry { entry, revision }))
}

/// Build a [`RevisionedValue`] from a decoded CAS entry, returning `None` when
/// the entry has expired (expired entries are treated exactly as absent).
pub(crate) fn to_revisioned_value(cas: CasEntry) -> Option<RevisionedEntry> {
    if cas.entry.is_expired() {
        return None;
    }
    let ttl = cas.entry.ttl();
    Some(RevisionedEntry {
        value: cas.entry.value,
        revision: cas.revision,
        ttl,
    })
}

/// An immutable value/revision/TTL snapshot built from a CAS entry.
#[derive(Debug)]
pub(crate) struct RevisionedEntry {
    pub value: Value,
    pub revision: Revision,
    pub ttl: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrips_permanent_entry_and_revision() {
        let entry = ManagedEntry::new(Value::utf8("hello"));
        let revision = Revision::from_bytes([7u8; Revision::BYTE_LEN]);
        let encoded = encode(&entry, revision);
        let decoded = decode(Some(encoded)).unwrap().unwrap();
        assert_eq!(decoded.entry.value, Value::utf8("hello"));
        assert_eq!(decoded.revision, revision);
        assert!(decoded.entry.expires_at.is_none());
    }

    #[test]
    fn envelope_roundtrips_ttl_entry_and_revision() {
        let entry = ManagedEntry::with_ttl(Value::integer(42), 3600.0).unwrap();
        let revision = Revision::from_bytes([0xAB; Revision::BYTE_LEN]);
        let encoded = encode(&entry, revision);
        let decoded = decode(Some(encoded)).unwrap().unwrap();
        assert_eq!(decoded.entry.value, Value::integer(42));
        assert_eq!(decoded.revision, revision);
        assert!(decoded.entry.ttl().is_some());
    }

    #[test]
    fn decode_absent_is_none() {
        assert!(decode(None).unwrap().is_none());
    }

    #[test]
    fn decode_rejects_raw_okve1() {
        let entry = ManagedEntry::new(Value::null());
        let raw_okve1 = entry.encode();
        let err = decode(Some(raw_okve1)).unwrap_err();
        assert!(matches!(err, Error::Deserialization(_)));
    }

    #[test]
    fn decode_rejects_json_payload() {
        let err = decode(Some(br#"{"value":null}"#.to_vec())).unwrap_err();
        assert!(matches!(err, Error::Deserialization(_)));
    }

    #[test]
    fn decode_rejects_truncated_envelope() {
        let entry = ManagedEntry::new(Value::null());
        let revision = Revision::from_bytes([1u8; Revision::BYTE_LEN]);
        let mut encoded = encode(&entry, revision);
        // Truncate in the middle of the revision token.
        encoded.truncate(PREFIX_LEN - 1);
        let err = decode(Some(encoded)).unwrap_err();
        assert!(matches!(err, Error::Deserialization(_)));
    }

    #[test]
    fn decode_rejects_truncated_entry_payload() {
        let entry = ManagedEntry::new(Value::utf8("data"));
        let revision = Revision::from_bytes([2u8; Revision::BYTE_LEN]);
        let mut encoded = encode(&entry, revision);
        encoded.pop();
        let err = decode(Some(encoded)).unwrap_err();
        assert!(matches!(err, Error::Deserialization(_)));
    }

    #[test]
    fn decode_rejects_malformed_magic() {
        let entry = ManagedEntry::new(Value::null());
        let revision = Revision::from_bytes([3u8; Revision::BYTE_LEN]);
        let mut encoded = encode(&entry, revision);
        encoded[0] = b'X';
        let err = decode(Some(encoded)).unwrap_err();
        assert!(matches!(err, Error::Deserialization(_)));
    }

    #[test]
    fn decode_rejects_trailing_entry_data() {
        let entry = ManagedEntry::new(Value::null());
        let revision = Revision::from_bytes([4u8; Revision::BYTE_LEN]);
        let mut encoded = encode(&entry, revision);
        encoded.push(0xFF);
        let err = decode(Some(encoded)).unwrap_err();
        assert!(matches!(err, Error::Deserialization(_)));
    }

    #[test]
    fn expired_entry_snapshots_as_absent() {
        let mut entry = ManagedEntry::new(Value::utf8("stale"));
        entry.expires_at = Some(chrono::Utc::now() - chrono::TimeDelta::seconds(10));
        let revision = Revision::from_bytes([5u8; Revision::BYTE_LEN]);
        let encoded = encode(&entry, revision);
        let decoded = decode(Some(encoded)).unwrap().unwrap();
        assert!(to_revisioned_value(decoded).is_none());
    }
}
