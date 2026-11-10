//! Entry-block payload encoding, the seqlock snapshot reader, and block writers.
//!
//! Blocks are immutable once published (`sequence` even). Updates are
//! copy-on-write: a mutation writes a fresh block (odd sequence), publishes it
//! into the index, and frees the old one. Readers therefore never observe an
//! in-place rewrite; they only have to guard against reading a block that is
//! being written for the first time or has already been freed and reused —
//! both detected by the sequence re-check plus the caller's slot/generation
//! re-verification.

use super::arena;
use super::layout::{EntryBlockHeader, SharedMap, BLOCK_HEADER_BYTES};
use crate::error::{Error, Result};
use std::sync::atomic::Ordering;

/// A stable copy of one entry block, decoded into owned data.
#[derive(Clone, Debug)]
pub(crate) struct EntrySnapshot {
    pub record_kind: u64,
    pub value_kind: u64,
    pub expires_at_ms: u64,
    pub revision: [u8; 16],
    pub collection: String,
    pub key: String,
    pub value: Vec<u8>,
}

impl EntrySnapshot {
    pub(crate) fn is_expired(&self, now_ms: u64) -> bool {
        self.expires_at_ms != 0 && self.expires_at_ms <= now_ms
    }

    /// Remaining TTL in seconds, mirroring `ManagedEntry::ttl`: `None` when the
    /// entry never expires.
    pub(crate) fn ttl_remaining(&self, now_ms: u64) -> Option<f64> {
        if self.expires_at_ms == 0 {
            return None;
        }
        Some(self.expires_at_ms.saturating_sub(now_ms) as f64 / 1000.0)
    }
}

fn bytes_to_words(bytes: &[u8]) -> Vec<u64> {
    let mut words = vec![0u64; bytes.len().div_ceil(8)];
    for (index, byte) in bytes.iter().enumerate() {
        words[index / 8] |= (*byte as u64) << ((index % 8) * 8);
    }
    words
}

fn words_to_bytes(words: &[u64], len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    for byte in words.iter().flat_map(|w| w.to_le_bytes()) {
        if bytes.len() == len {
            break;
        }
        bytes.push(byte);
    }
    bytes
}

/// One seqlock attempt. Returns `Ok(None)` when the block was observed mid-write
/// or mutated while being copied; callers retry (lock-free) or treat the slot
/// as unstable. `Err` is reserved for corrupt layout — a block that stays odd,
/// claims impossible lengths, or holds non-UTF-8 identifiers.
pub(crate) fn snapshot_once(map: &SharedMap, block_offset: u64) -> Result<Option<EntrySnapshot>> {
    let header = map.entry_block(block_offset)?;
    let sequence0 = header.sequence.load(Ordering::Acquire);
    if sequence0 == 0 || sequence0 % 2 == 1 {
        return Ok(None);
    }

    let collection_len = header.collection_len.load(Ordering::Relaxed) as usize;
    let key_len = header.key_len.load(Ordering::Relaxed) as usize;
    let value_len = header.value_len.load(Ordering::Relaxed) as usize;
    let payload_bytes = collection_len + key_len + value_len;
    let words = payload_bytes.div_ceil(8);
    let payload = map.read_words(block_offset + BLOCK_HEADER_BYTES as u64, words)?;

    let sequence1 = header.sequence.load(Ordering::Acquire);
    if sequence1 != sequence0 {
        return Ok(None);
    }

    let mut payload_bytes_all = words_to_bytes(&payload, payload.len() * 8);
    payload_bytes_all.truncate(payload_bytes);
    let collection_bytes = payload_bytes_all[..collection_len].to_vec();
    let key_bytes = payload_bytes_all[collection_len..collection_len + key_len].to_vec();
    let value = payload_bytes_all[collection_len + key_len..].to_vec();

    let collection = String::from_utf8(collection_bytes).map_err(|_| Error::StoreFormat {
        message: format!("entry block at {block_offset:#x} holds a non-UTF-8 collection"),
    })?;
    let key = String::from_utf8(key_bytes).map_err(|_| Error::StoreFormat {
        message: format!("entry block at {block_offset:#x} holds a non-UTF-8 key"),
    })?;

    let revision0 = header.revision0.load(Ordering::Relaxed);
    let revision1 = header.revision1.load(Ordering::Relaxed);
    let mut revision = [0u8; 16];
    revision[..8].copy_from_slice(&revision0.to_le_bytes());
    revision[8..].copy_from_slice(&revision1.to_le_bytes());

    Ok(Some(EntrySnapshot {
        record_kind: header.flags.load(Ordering::Relaxed),
        value_kind: header.value_kind.load(Ordering::Relaxed),
        expires_at_ms: header.expires_at_ms.load(Ordering::Relaxed),
        revision,
        collection,
        key,
        value,
    }))
}

/// Header and payload of one entry block, ready to be published.
pub(crate) struct EntryBlockFields<'a> {
    pub record_kind: u64,
    pub value_kind: u64,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub revision: &'a [u8; 16],
    pub collection: &'a str,
    pub key: &'a str,
    pub value: &'a [u8],
}

/// Write a complete entry block into `handle` and publish it (sequence even,
/// Release) so lock-free readers can pick it up. Must be called under the
/// writer lock.
pub(crate) fn write_entry_block(
    map: &SharedMap,
    handle: &arena::BlockHandle,
    fields: EntryBlockFields<'_>,
) -> Result<()> {
    let header: &EntryBlockHeader = map.entry_block(handle.offset)?;
    header.sequence.store(1, Ordering::Relaxed);
    header.flags.store(fields.record_kind, Ordering::Relaxed);
    header.value_kind.store(fields.value_kind, Ordering::Relaxed);
    header.created_at_ms.store(fields.created_at_ms, Ordering::Relaxed);
    header.expires_at_ms.store(fields.expires_at_ms, Ordering::Relaxed);
    header.revision0.store(
        u64::from_le_bytes(fields.revision[..8].try_into().unwrap()),
        Ordering::Relaxed,
    );
    header.revision1.store(
        u64::from_le_bytes(fields.revision[8..].try_into().unwrap()),
        Ordering::Relaxed,
    );
    header.collection_len.store(fields.collection.len() as u64, Ordering::Relaxed);
    header.key_len.store(fields.key.len() as u64, Ordering::Relaxed);
    header.value_len.store(fields.value.len() as u64, Ordering::Relaxed);
    header.size_class.store(handle.class as u64, Ordering::Relaxed);
    header.next_free.store(0, Ordering::Relaxed);

    // Payload is one packed byte string (collection || key || value) padded to
    // a word boundary; readers slice it back by lengths, so no per-field
    // padding may be inserted.
    let mut payload_bytes = Vec::with_capacity(fields.collection.len() + fields.key.len() + fields.value.len());
    payload_bytes.extend_from_slice(fields.collection.as_bytes());
    payload_bytes.extend_from_slice(fields.key.as_bytes());
    payload_bytes.extend_from_slice(fields.value);
    map.write_words(handle.offset + BLOCK_HEADER_BYTES as u64, &bytes_to_words(&payload_bytes))?;

    // Publish: even sequence with Release so readers that load it see the full
    // block contents.
    header.sequence.store(2, Ordering::Release);
    Ok(())
}

/// Payload byte length implied by identifier and value sizes.
pub(crate) fn payload_len(collection: &str, key: &str, value_len: usize) -> usize {
    collection.len() + key.len() + value_len
}
