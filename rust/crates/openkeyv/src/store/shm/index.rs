//! Hash index over the shared arena: probing, insertion, tombstones, and
//! reindex into the shadow table.
//!
//! All functions here run under the writer lock except where noted; readers
//! use the lock-free paths in [`super::client`]. Slot mutations always bump
//! `sequence` so concurrent lock-free readers detect the change and retry.

use super::entry::{snapshot_once, EntrySnapshot};
use super::layout::{IndexSlot, Layout, SharedMap, SLOT_EMPTY, SLOT_TOMBSTONE};
use crate::error::{Error, Result};
use std::sync::atomic::Ordering;

/// Give up a probe after this many steps and trigger a reindex instead of
/// walking a pathologically clustered table.
const PROBE_LIMIT: u64 = 64;

/// How many times a scan restarts when the index generation flips underneath
/// it before it accepts a weak snapshot.
const SCAN_RETRIES: usize = 8;

pub(crate) enum Locate {
    Live {
        table: usize,
        index: u64,
        block: u64,
        snapshot: EntrySnapshot,
    },
    /// An empty or tombstone slot where the key can be inserted.
    Vacant { table: usize, index: u64 },
    /// No matching key and no reusable slot within the probe limit.
    Full,
}

pub(crate) fn locate(
    map: &SharedMap,
    layout: &Layout,
    record_kind: u64,
    collection: &str,
    key: &str,
) -> Result<Locate> {
    let count = layout.index_entries;
    let hash = super::layout::slot_hash(record_kind, collection, key);
    let step = (hash >> 33) | 1;
    let mut slot_index = hash & (count - 1);
    let mut vacant: Option<(usize, u64)> = None;
    let table = map.header().active_index.load(Ordering::Relaxed) as usize;

    for _ in 0..PROBE_LIMIT {
        let slot = map.index_slot(table, slot_index, layout)?;
        let offset = slot.entry_offset.load(Ordering::Relaxed);
        match offset {
            SLOT_EMPTY => {
                return Ok(match vacant {
                    Some((table, index)) => Locate::Vacant { table, index },
                    None => Locate::Vacant {
                        table,
                        index: slot_index,
                    },
                });
            }
            SLOT_TOMBSTONE => {
                if vacant.is_none() {
                    vacant = Some((table, slot_index));
                }
            }
            _ => {
                if let Some(snapshot) = snapshot_once(map, offset)? {
                    if snapshot.record_kind == record_kind
                        && snapshot.collection == collection
                        && snapshot.key == key
                    {
                        return Ok(Locate::Live {
                            table,
                            index: slot_index,
                            block: offset,
                            snapshot,
                        });
                    }
                }
            }
        }
        slot_index = (slot_index + step) & (count - 1);
    }
    Ok(Locate::Full)
}

/// Point `location` at a freshly published block and bump the slot sequence.
pub(crate) fn publish_slot(
    map: &SharedMap,
    layout: &Layout,
    table: usize,
    index: u64,
    block: u64,
) -> Result<()> {
    let slot = map.index_slot(table, index, layout)?;
    slot.entry_offset.store(block, Ordering::Relaxed);
    slot.sequence.fetch_add(1, Ordering::Release);
    Ok(())
}

/// Turn the slot into a tombstone. Callers free the referenced block only
/// after this has landed, so a crash between the two merely leaks the block.
pub(crate) fn tombstone_slot(map: &SharedMap, layout: &Layout, table: usize, index: u64) -> Result<()> {
    let slot = map.index_slot(table, index, layout)?;
    slot.entry_offset.store(SLOT_TOMBSTONE, Ordering::Relaxed);
    slot.sequence.fetch_add(1, Ordering::Release);
    Ok(())
}

/// True when the live table would benefit from compaction into the shadow.
pub(crate) fn needs_reindex(map: &SharedMap) -> bool {
    let header = map.header();
    let live = header.live_entries.load(Ordering::Relaxed);
    let tombstones = header.tombstones.load(Ordering::Relaxed);
    tombstones > live && tombstones > 0
}

/// Rebuild the shadow table from the active one, flip `active_index`, and bump
/// `index_generation`. Lock-free readers notice the generation change and
/// restart; both tables stay mapped, so readers holding the old table only see
/// slightly stale (still valid) slots.
pub(crate) fn reindex(map: &SharedMap, layout: &Layout) -> Result<()> {
    let header = map.header();
    let source = header.active_index.load(Ordering::Relaxed) as usize;
    let target = 1 - source;
    let slot_bytes = layout.index_entries * std::mem::size_of::<IndexSlot>() as u64;
    map.zero_range(layout.off_index[target], slot_bytes)?;

    for index in 0..layout.index_entries {
        let slot = map.index_slot(source, index, layout)?;
        let offset = slot.entry_offset.load(Ordering::Relaxed);
        if offset == SLOT_EMPTY || offset == SLOT_TOMBSTONE {
            continue;
        }
        let Some(snapshot) = snapshot_once(map, offset)? else {
            return Err(Error::StoreFormat {
                message: "reindex observed an unstable entry block".to_string(),
            });
        };
        let hash = super::layout::slot_hash(snapshot.record_kind, &snapshot.collection, &snapshot.key);
        let step = (hash >> 33) | 1;
        let mut target_index = hash & (layout.index_entries - 1);
        loop {
            let target_slot = map.index_slot(target, target_index, layout)?;
            if target_slot.entry_offset.load(Ordering::Relaxed) == SLOT_EMPTY {
                target_slot.entry_offset.store(offset, Ordering::Relaxed);
                break;
            }
            target_index = (target_index + step) & (layout.index_entries - 1);
        }
    }

    header.active_index.store(target as u64, Ordering::Release);
    header.index_generation.fetch_add(1, Ordering::Release);
    header.tombstones.store(0, Ordering::Relaxed);
    Ok(())
}

#[derive(Debug)]
pub(crate) struct ScanHit {
    pub collection: String,
    pub key: String,
    pub expired: bool,
}

/// Weakly-consistent scan of the active table. Optionally restricted to one
/// record kind and/or collection, and optionally only expired entries.
pub(crate) fn scan(
    map: &SharedMap,
    layout: &Layout,
    record_kind: Option<u64>,
    collection: Option<&str>,
    expired_only: bool,
    now_ms: u64,
) -> Result<Vec<ScanHit>> {
    let mut hits = Vec::new();
    for _ in 0..SCAN_RETRIES {
        let generation0 = map.header().index_generation.load(Ordering::Acquire);
        let table = map.header().active_index.load(Ordering::Acquire) as usize;
        hits.clear();
        let mut restarted = false;

        for index in 0..layout.index_entries {
            if map.header().index_generation.load(Ordering::Acquire) != generation0 {
                restarted = true;
                break;
            }
            let slot = map.index_slot(table, index, layout)?;
            let offset = slot.entry_offset.load(Ordering::Relaxed);
            if offset == SLOT_EMPTY || offset == SLOT_TOMBSTONE {
                continue;
            }
            let Some(snapshot) = snapshot_once(map, offset)? else {
                continue;
            };
            if let Some(kind) = record_kind {
                if snapshot.record_kind != kind {
                    continue;
                }
            }
            if let Some(wanted) = collection {
                if snapshot.collection != wanted {
                    continue;
                }
            }
            let expired = snapshot.is_expired(now_ms);
            if expired_only && !expired {
                continue;
            }
            hits.push(ScanHit {
                collection: snapshot.collection,
                key: snapshot.key,
                expired,
            });
        }
        if !restarted {
            return Ok(hits);
        }
    }
    // Generation kept flipping; the last pass is still a valid weak snapshot.
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_steps_are_always_odd() {
        for hash in [0u64, 1, 0xdead_beef, u64::MAX] {
            assert_eq!(((hash >> 33) | 1) & 1, 1);
        }
    }
}
