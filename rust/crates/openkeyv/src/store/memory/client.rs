use crate::change::{
    ChangeCursor, ChangeFilter, ChangeOperation, ChangeStart, ChangeStream, StoreChange,
};
use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::Revision;
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use std::collections::VecDeque;
use tokio::sync::{Mutex, RwLock, broadcast};

/// A stored entry paired with its opaque revision.
///
/// This is private to the memory backend; the revision lives alongside the
/// managed entry so that conditional operations can observe and mutate both in
/// a single DashMap entry operation.
#[derive(Debug, Clone)]
pub(crate) struct RevisionedEntry {
    pub entry: ManagedEntry,
    pub revision: Revision,
}

impl RevisionedEntry {
    pub(crate) fn snapshot(&self) -> RevisionedEntrySnapshot {
        RevisionedEntrySnapshot {
            value: self.entry.value.clone(),
            revision: self.revision,
            ttl: self.entry.ttl(),
        }
    }
}

/// An immutable atomic snapshot used to build protocol result types.
pub(crate) struct RevisionedEntrySnapshot {
    pub value: crate::value::Value,
    pub revision: Revision,
    pub ttl: Option<f64>,
}

pub(crate) type MemoryCollections = DashMap<String, DashMap<String, RevisionedEntry>>;

pub(crate) const CHANGE_RETENTION: usize = 10_000;

/// Generate a fresh opaque revision from OS randomness.
///
/// Must be called before mutation so that a randomness failure cannot leave a
/// partial write behind.
pub(crate) fn fresh_revision() -> Result<Revision> {
    let mut bytes = [0u8; Revision::BYTE_LEN];
    getrandom::fill(&mut bytes).map_err(|err| Error::RevisionGeneration(err.to_string()))?;
    Ok(Revision::from_bytes(bytes))
}

struct MemoryChangeState {
    revision: u64,
    entries: VecDeque<StoreChange>,
    sender: broadcast::Sender<StoreChange>,
}

pub(crate) struct MemoryChangeStream {
    replay: VecDeque<StoreChange>,
    receiver: broadcast::Receiver<StoreChange>,
    filter: ChangeFilter,
}

#[async_trait]
impl ChangeStream for MemoryChangeStream {
    async fn recv(&mut self) -> Result<Option<StoreChange>> {
        loop {
            if let Some(change) = self.replay.pop_front() {
                if self.filter.matches(&change) {
                    return Ok(Some(change));
                }
                continue;
            }

            match self.receiver.recv().await {
                Ok(change) if self.filter.matches(&change) => return Ok(Some(change)),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Closed) => return Ok(None),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(Error::ChangeFeedLagged { skipped });
                }
            }
        }
    }
}

pub struct MemoryClient {
    collections: MemoryCollections,
    setup_complete: RwLock<bool>,
    mutation_lock: Mutex<()>,
    changes: Mutex<MemoryChangeState>,
}

impl Default for MemoryClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryClient {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANGE_RETENTION);
        Self {
            collections: MemoryCollections::new(),
            setup_complete: RwLock::new(false),
            mutation_lock: Mutex::new(()),
            changes: Mutex::new(MemoryChangeState {
                revision: 0,
                entries: VecDeque::with_capacity(CHANGE_RETENTION),
                sender,
            }),
        }
    }

    pub(crate) fn collections(&self) -> &MemoryCollections {
        &self.collections
    }

    pub(crate) fn setup_complete(&self) -> &RwLock<bool> {
        &self.setup_complete
    }

    pub(crate) fn mutation_lock(&self) -> &Mutex<()> {
        &self.mutation_lock
    }

    pub(crate) async fn record_change(
        &self,
        collection: &str,
        key: &str,
        operation: ChangeOperation,
    ) {
        let mut state = self.changes.lock().await;
        state.revision += 1;
        let change = StoreChange {
            cursor: ChangeCursor::new(state.revision.to_string()),
            revision: state.revision,
            collection: collection.to_string(),
            key: key.to_string(),
            operation,
            occurred_at: Utc::now(),
        };

        if state.entries.len() == CHANGE_RETENTION {
            state.entries.pop_front();
        }
        state.entries.push_back(change.clone());
        let _ = state.sender.send(change);
    }

    pub(crate) async fn subscribe(
        &self,
        start: ChangeStart,
        filter: ChangeFilter,
    ) -> Result<MemoryChangeStream> {
        let _mutation = self.mutation_lock.lock().await;
        let state = self.changes.lock().await;
        let receiver = state.sender.subscribe();
        let replay = match start {
            ChangeStart::Beginning => state.entries.clone(),
            ChangeStart::Latest => VecDeque::new(),
            ChangeStart::After(cursor) => {
                let requested = cursor
                    .as_str()
                    .parse::<u64>()
                    .map_err(|_| Error::InvalidChangeCursor(cursor.to_string()))?;
                if requested > state.revision {
                    return Err(Error::InvalidChangeCursor(cursor.to_string()));
                }
                if let Some(oldest) = state.entries.front().map(|change| change.revision) {
                    if requested < oldest.saturating_sub(1) {
                        return Err(Error::ChangeCursorExpired {
                            requested: cursor.to_string(),
                            oldest: oldest.to_string(),
                        });
                    }
                }
                state
                    .entries
                    .iter()
                    .filter(|change| change.revision > requested)
                    .cloned()
                    .collect()
            }
        };

        Ok(MemoryChangeStream {
            replay,
            receiver,
            filter,
        })
    }
}
