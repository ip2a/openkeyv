//! Shared change-feed ring and the subscription stream.
//!
//! The ring is an array of self-contained event slots (see
//! [`super::layout::ChangeRingSlot`]). Slot `(rev - 1) % retention` is a
//! stable address for revision `rev`; the slot's `sequence` carries the
//! revision once published and [`SLOT_WRITING`] while it is being filled.
//!
//! Readers are lock-free. They wait while a reservation is still unpublished
//! (`next_revision == count`) or a write is in progress, and skip a slot only
//! when the writer has provably moved past it — a crash gap whose change the
//! recovery path re-appended at a fresh revision. `read_event` re-verifies the
//! slot sequence around the copy (seqlock), so a slot rewritten mid-read is
//! retried, never torn.

use super::client::ShmClient;
use super::config::{MAX_COLLECTION_BYTES, MAX_KEY_BYTES};
use super::layout::{EVENT_PAYLOAD_WORDS, Layout, SLOT_WRITING, SharedMap};
use super::notify::NotifyRegistration;
use crate::change::{ChangeCursor, ChangeFilter, ChangeOperation, ChangeStream, StoreChange};
use crate::error::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
    let payload_len = collection.len() + key.len();
    if payload_len > MAX_COLLECTION_BYTES + MAX_KEY_BYTES {
        return Err(Error::StoreFormat {
            message: format!("change event payload of {payload_len} bytes exceeds the slot"),
        });
    }
    let header = map.header();
    // AcqRel on the reservation publishes the previous event's slot store to
    // any reader that observes this reservation: a reader that sees
    // `count >= R + 1` can trust that revision R's slot is fully published.
    let revision = header.change_revision.fetch_add(1, Ordering::AcqRel) + 1;
    let generation = header.store_generation.load(Ordering::Relaxed);

    let slot = map.ring_slot((revision - 1) % layout.change_retention, layout)?;
    slot.sequence.store(SLOT_WRITING, Ordering::Relaxed);
    slot.operation.store(operation, Ordering::Relaxed);
    slot.occurred_at_ms.store(now_unix_ms(), Ordering::Relaxed);
    slot.store_generation.store(generation, Ordering::Relaxed);
    slot.operation_id.store(operation_id, Ordering::Relaxed);
    slot.collection_len
        .store(collection.len() as u64, Ordering::Relaxed);
    slot.key_len.store(key.len() as u64, Ordering::Relaxed);
    slot.cause.store(super::layout::CAUSE_EXPLICIT, Ordering::Relaxed);

    let mut payload_bytes = Vec::with_capacity(payload_len);
    payload_bytes.extend_from_slice(collection.as_bytes());
    payload_bytes.extend_from_slice(key.as_bytes());
    for (target, word) in slot.payload.iter().zip(chunk_to_words(&payload_bytes)) {
        target.store(word, Ordering::Relaxed);
    }
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
        if slot.operation_id.load(Ordering::Relaxed) == operation_id {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn now_unix_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

/// Read one retained event, seqlock-guarded. `Ok(None)` means the slot changed
/// under us (write in progress or rewritten); the caller retries the same
/// revision.
fn read_event(map: &SharedMap, layout: &Layout, revision: u64) -> Result<Option<StoreChange>> {
    let slot = map.ring_slot((revision - 1) % layout.change_retention, layout)?;
    let sequence = slot.sequence.load(Ordering::Acquire);
    if sequence != revision {
        return Ok(None);
    }

    let operation_tag = slot.operation.load(Ordering::Relaxed);
    let occurred_ms = slot.occurred_at_ms.load(Ordering::Relaxed);
    let collection_len = slot.collection_len.load(Ordering::Relaxed) as usize;
    let key_len = slot.key_len.load(Ordering::Relaxed) as usize;
    if collection_len + key_len > EVENT_PAYLOAD_WORDS * 8 {
        return Err(Error::StoreFormat {
            message: format!(
                "change event holds identifiers of {} bytes, above the slot capacity",
                collection_len + key_len
            ),
        });
    }
    let words = (collection_len + key_len).div_ceil(8);
    let payload: Vec<u64> = slot
        .payload
        .iter()
        .take(words)
        .map(|word| word.load(Ordering::Relaxed))
        .collect();
    // Seqlock re-verify: a rewrite (or a publish in progress) invalidates
    // everything copied above.
    if slot.sequence.load(Ordering::Acquire) != sequence {
        return Ok(None);
    }

    let bytes = payload
        .iter()
        .flat_map(|word| word.to_le_bytes())
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
    let occurred_at = DateTime::from_timestamp_millis(occurred_ms as i64).unwrap_or_else(Utc::now);

    Ok(Some(StoreChange {
        cursor: ChangeCursor::new(revision.to_string()),
        revision,
        collection,
        key,
        operation: operation_from_tag(operation_tag)?,
        occurred_at,
    }))
}

/// A resumable stream over the shared change ring.
///
/// Idle waiting uses the named-semaphore wakeup (see
/// [`super::notify`]): one `sem_timedwait` of [`IDLE_WAIT`], then the ring is
/// re-polled. Field order matters — `notify` must drop before `client` so
/// unregistration still sees a live map.
pub(crate) struct ShmChangeStream {
    notify: Option<NotifyRegistration>,
    client: Arc<ShmClient>,
    next_revision: u64,
    generation: u64,
    filter: ChangeFilter,
}

impl ShmChangeStream {
    pub(crate) fn new(
        client: Arc<ShmClient>,
        next_revision: u64,
        generation: u64,
        filter: ChangeFilter,
    ) -> Self {
        Self {
            notify: None,
            client,
            next_revision,
            generation,
            filter,
        }
    }

    /// Wait for a wakeup signal. The semaphore slot is claimed lazily on the
    /// first idle wait — a stream that never idles never claims a slot.
    async fn idle_wait(&mut self) -> Result<()> {
        if self.notify.is_none() {
            self.notify = Some(NotifyRegistration::register(&self.client)?);
        }
        self.notify.as_mut().expect("registered above").wait().await
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
                self.idle_wait().await?;
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
                // The writer has reserved this revision but not published it
                // yet (or is filling the slot right now): wake with the
                // publish post. Skip only once the writer has provably moved
                // past it — a crash gap whose change recovery re-appended at
                // a fresh revision.
                if slot_sequence == SLOT_WRITING || self.next_revision == count {
                    self.idle_wait().await?;
                } else {
                    self.next_revision += 1;
                }
                continue;
            }
            match read_event(map, layout, self.next_revision)? {
                Some(change) => {
                    self.next_revision += 1;
                    if self.filter.matches(&change) {
                        return Ok(Some(change));
                    }
                }
                // Torn read: the slot changed under us; retry this revision.
                None => continue,
            }
        }
    }
}
