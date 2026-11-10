//! Shared change-feed ring and the subscription stream.
//!
//! Writers append event blocks to the arena under the writer lock and publish
//! them through ring slots. A slot's `sequence` equals the global change
//! revision of the event it holds, so slot `(rev - 1) % retention` is a stable
//! address for revision `rev`. A slot whose sequence does not match the
//! expected revision is a crash gap and is skipped: the event was replaced by
//! the recovery path, which re-appends it at a fresh revision.

use super::client::ShmClient;
use super::layout::{ChangeEventBlock, EVENT_HEADER_BYTES, Layout, SharedMap};
use crate::change::{ChangeCursor, ChangeFilter, ChangeOperation, ChangeStream, StoreChange};
use crate::error::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

const BACKOFF_MIN_MS: u64 = 1;
const BACKOFF_MAX_MS: u64 = 20;

fn operation_from_tag(tag: u64) -> Result<ChangeOperation> {
    match tag {
        super::layout::OP_PUT => Ok(ChangeOperation::Put),
        super::layout::OP_DELETE => Ok(ChangeOperation::Delete),
        _ => Err(Error::StoreFormat {
            message: format!("change event carries unknown operation tag {tag}"),
        }),
    }
}

/// Append one event and return its global revision. Writer lock held.
pub(crate) fn append_event(
    map: &SharedMap,
    layout: &Layout,
    operation: u64,
    collection: &str,
    key: &str,
    operation_id: u64,
) -> Result<u64> {
    if layout.change_retention == 0 {
        return Ok(0);
    }
    let header = map.header();
    let revision = header.change_revision.fetch_add(1, Ordering::Relaxed) + 1;
    let generation = header.store_generation.load(Ordering::Relaxed);

    let payload_len = collection.len() + key.len();
    let handle = super::arena::alloc(map, layout, payload_len)?;
    let block: &ChangeEventBlock = map.event_block(handle.offset)?;
    block.sequence.store(1, Ordering::Relaxed);
    block.revision.store(revision, Ordering::Relaxed);
    block.operation.store(operation, Ordering::Relaxed);
    block.occurred_at_ms.store(now_unix_ms(), Ordering::Relaxed);
    block.store_generation.store(generation, Ordering::Relaxed);
    block.operation_id.store(operation_id, Ordering::Relaxed);
    block
        .collection_len
        .store(collection.len() as u64, Ordering::Relaxed);
    block.key_len.store(key.len() as u64, Ordering::Relaxed);

    let mut payload_bytes = Vec::with_capacity(payload_len);
    payload_bytes.extend_from_slice(collection.as_bytes());
    payload_bytes.extend_from_slice(key.as_bytes());
    map.write_words(
        handle.offset + EVENT_HEADER_BYTES as u64,
        &chunk_to_words(&payload_bytes),
    )?;
    block.sequence.store(2, Ordering::Release);

    let slot = map.ring_slot((revision - 1) % layout.change_retention, layout)?;
    slot.event_offset.store(handle.offset, Ordering::Relaxed);
    slot.sequence.store(revision, Ordering::Release);
    Ok(revision)
}

fn chunk_to_word(chunk: &[u8]) -> u64 {
    let mut word = 0u64;
    for (index, byte) in chunk.iter().enumerate() {
        word |= (*byte as u64) << (index * 8);
    }
    word
}

/// Pack bytes into little-endian words, zero-padded to a word boundary.
fn chunk_to_words(bytes: &[u8]) -> Vec<u64> {
    bytes.chunks(8).map(chunk_to_word).collect()
}

/// True when the retained ring already holds an event for `operation_id`.
/// Used by crash recovery to guarantee exactly-once change delivery.
pub(crate) fn ring_contains_operation(
    map: &SharedMap,
    layout: &Layout,
    operation_id: u64,
) -> Result<bool> {
    if layout.change_retention == 0 {
        return Ok(false);
    }
    let count = map.header().change_revision.load(Ordering::Acquire);
    let first = count.saturating_sub(layout.change_retention) + 1;
    for revision in first..=count {
        let slot = map.ring_slot((revision - 1) % layout.change_retention, layout)?;
        if slot.sequence.load(Ordering::Acquire) != revision {
            continue;
        }
        let block = map.event_block(slot.event_offset.load(Ordering::Acquire))?;
        if block.operation_id.load(Ordering::Relaxed) == operation_id {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn now_unix_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

fn read_event(map: &SharedMap, layout: &Layout, revision: u64) -> Result<StoreChange> {
    let slot = map.ring_slot((revision - 1) % layout.change_retention, layout)?;
    let offset = slot.event_offset.load(Ordering::Acquire);
    let block = map.event_block(offset)?;

    let collection_len = block.collection_len.load(Ordering::Relaxed) as usize;
    let key_len = block.key_len.load(Ordering::Relaxed) as usize;
    let words = (collection_len + key_len).div_ceil(8);
    let payload = map.read_words(offset + EVENT_HEADER_BYTES as u64, words)?;
    let bytes = payload
        .iter()
        .flat_map(|w| w.to_le_bytes())
        .collect::<Vec<u8>>();
    let collection =
        String::from_utf8(bytes[..collection_len].to_vec()).map_err(|_| Error::StoreFormat {
            message: "change event holds a non-UTF-8 collection".to_string(),
        })?;
    let key = String::from_utf8(bytes[collection_len..collection_len + key_len].to_vec()).map_err(
        |_| Error::StoreFormat {
            message: "change event holds a non-UTF-8 key".to_string(),
        },
    )?;

    let revision_value = block.revision.load(Ordering::Relaxed);
    let occurred_at =
        DateTime::from_timestamp_millis(block.occurred_at_ms.load(Ordering::Relaxed) as i64)
            .unwrap_or_else(Utc::now);

    Ok(StoreChange {
        cursor: ChangeCursor::new(revision_value.to_string()),
        revision: revision_value,
        collection,
        key,
        operation: operation_from_tag(block.operation.load(Ordering::Relaxed))?,
        occurred_at,
    })
}

/// A resumable stream over the shared change ring.
pub(crate) struct ShmChangeStream {
    client: Arc<ShmClient>,
    next_revision: u64,
    generation: u64,
    filter: ChangeFilter,
    backoff_ms: u64,
}

impl ShmChangeStream {
    pub(crate) fn new(
        client: Arc<ShmClient>,
        next_revision: u64,
        generation: u64,
        filter: ChangeFilter,
    ) -> Self {
        Self {
            client,
            next_revision,
            generation,
            filter,
            backoff_ms: BACKOFF_MIN_MS,
        }
    }
}

#[async_trait]
impl ChangeStream for ShmChangeStream {
    async fn recv(&mut self) -> Result<Option<StoreChange>> {
        loop {
            let map = &self.client.map;
            let layout = &self.client.layout;
            if map.header().store_generation.load(Ordering::Acquire) != self.generation {
                return Ok(None);
            }
            let count = map.header().change_revision.load(Ordering::Acquire);
            if self.next_revision > count {
                tokio::time::sleep(Duration::from_millis(self.backoff_ms)).await;
                self.backoff_ms = (self.backoff_ms * 2).min(BACKOFF_MAX_MS);
                continue;
            }
            let oldest = if count == 0 {
                1
            } else {
                count.saturating_sub(layout.change_retention) + 1
            };
            if self.next_revision < oldest {
                let skipped = oldest - self.next_revision;
                self.next_revision = oldest;
                return Err(Error::ChangeFeedLagged { skipped });
            }

            let slot = map.ring_slot((self.next_revision - 1) % layout.change_retention, layout)?;
            let slot_sequence = slot.sequence.load(Ordering::Acquire);
            if slot_sequence != self.next_revision {
                // Crash gap: recovery re-appended this change at a fresh
                // revision; skip the empty slot.
                self.next_revision += 1;
                continue;
            }
            let change = read_event(map, layout, self.next_revision)?;
            self.next_revision += 1;
            self.backoff_ms = BACKOFF_MIN_MS;
            if self.filter.matches(&change) {
                return Ok(Some(change));
            }
        }
    }
}
