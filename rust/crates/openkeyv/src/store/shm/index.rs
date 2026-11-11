//! Hash index over the shared arena: probing, incremental rehash between the
//! two reserved tables, and lock-free scans.
//!
//! All functions here run under the writer lock except where noted; readers
//! use the lock-free paths in [`super::client`]. Slot mutations always bump
//! `sequence` so concurrent lock-free readers detect the change and retry.
//!
//! The index is two tables reserved at `max_index_entries` slots each; the
//! logical size lives in the header (`index_entries`, `index_entries_target`)
//! and evolves through incremental rehash (redis dict.c model): each mutation
//! advances the rehash by at most [`REHASH_STEP_BUCKETS`] source buckets,
//! readers probe source-then-target while `rehash_cursor != REHASH_IDLE`,
//! and completion flips `active_index` with a single generation bump. The
//! regions never move for the life of the file, so growing and shrinking are
//! purely in-place.

use super::entry::{EntrySnapshot, snapshot_once};
use super::layout::{
    IndexSlot, Layout, REHASH_IDLE, SLOT_EMPTY, SLOT_TOMBSTONE, SharedMap,
};
use crate::error::{Error, Result};
use std::sync::atomic::Ordering;

/// Give up a probe after this many steps rather than walking a pathologically
/// clustered table; the caller restarts through a rehash step.
const PROBE_LIMIT: u64 = 64;

/// How many times a scan restarts when the index generation flips underneath
/// it before it accepts a weak snapshot.
const SCAN_RETRIES: usize = 8;

/// Source buckets visited per rehash step (redis `dictRehash(n)` semantics).
pub(crate) const REHASH_STEP_BUCKETS: u64 = 64;

/// Empty source slots a single step may skip before yielding.
const REHASH_EMPTY_VISITS: u64 = REHASH_STEP_BUCKETS * 10;

/// Smallest logical table size; shrink never goes below the floor the layout
/// plan itself enforces.
const MIN_TABLE_ENTRIES: u64 = 1024;

#[derive(Debug)]
pub(crate) enum Locate {
    Live {
        table: usize,
        index: u64,
        block: u64,
        snapshot: EntrySnapshot,
    },
    /// An empty or tombstone slot where the key can be inserted. During a
    /// rehash this is always a slot in the TARGET table: new keys never go
    /// into the source (redis: inserts land in ht[1]).
    Vacant { table: usize, index: u64 },
    /// No matching key and no reusable slot within the probe limit.
    Full,
}

/// Outcome of probing ONE table for a key.
enum Probe {
    Hit { index: u64, block: u64, snapshot: EntrySnapshot },
    /// First reusable (tombstone) or empty slot seen while probing.
    VacantAt(u64),
    /// Probe limit exhausted without a conclusion.
    Exhausted,
}

/// Bounded single-table probe (writer side).
fn probe_table(
    map: &SharedMap,
    layout: &Layout,
    table: usize,
    count: u64,
    record_kind: u64,
    collection: &str,
    key: &str,
) -> Result<Probe> {
    let hash = super::layout::slot_hash(record_kind, collection, key);
    let step = (hash >> 33) | 1;
    let mut slot_index = hash & (count - 1);
    let mut vacant: Option<u64> = None;
    for _ in 0..PROBE_LIMIT.min(count) {
        let slot = map.index_slot(table, slot_index, layout)?;
        let offset = slot.entry_offset.load(Ordering::Acquire);
        match offset {
            SLOT_EMPTY => {
                return Ok(Probe::VacantAt(vacant.unwrap_or(slot_index)));
            }
            SLOT_TOMBSTONE => {
                if vacant.is_none() {
                    vacant = Some(slot_index);
                }
            }
            _ => {
                if let Some(snapshot) = snapshot_once(map, offset)? {
                    if snapshot.record_kind == record_kind
                        && snapshot.collection == collection
                        && snapshot.key == key
                    {
                        return Ok(Probe::Hit { index: slot_index, block: offset, snapshot });
                    }
                }
            }
        }
        slot_index = (slot_index + step) & (count - 1);
    }
    Ok(Probe::Exhausted)
}

/// Locate a key for a mutation. Writer lock held. Dual-table while a rehash
/// is in flight: probe the source first (keys not yet moved live there),
/// then the target. Vacancies are only reported for the target — the source
/// is read-only during a rehash.
pub(crate) fn locate(
    map: &SharedMap,
    layout: &Layout,
    record_kind: u64,
    collection: &str,
    key: &str,
) -> Result<Locate> {
    let header = map.header();
    let source = header.active_index.load(Ordering::Acquire) as usize;
    if header.rehash_cursor.load(Ordering::Acquire) == REHASH_IDLE {
        let count = header.index_entries.load(Ordering::Acquire);
        return match probe_table(map, layout, source, count, record_kind, collection, key)? {
            Probe::Hit { index, block, snapshot } => Ok(Locate::Live {
                table: source,
                index,
                block,
                snapshot,
            }),
            Probe::VacantAt(index) => Ok(Locate::Vacant {
                table: source,
                index,
            }),
            Probe::Exhausted => Ok(Locate::Full),
        };
    }

    let source_count = header.index_entries.load(Ordering::Acquire);
    let target = 1 - source;
    let target_count = header.index_entries_target.load(Ordering::Acquire);
    match probe_table(map, layout, source, source_count, record_kind, collection, key)? {
        Probe::Hit { index, block, snapshot } => Ok(Locate::Live {
            table: source,
            index,
            block,
            snapshot,
        }),
        source_probe => {
            match probe_table(map, layout, target, target_count, record_kind, collection, key)? {
                Probe::Hit { index, block, snapshot } => Ok(Locate::Live {
                    table: target,
                    index,
                    block,
                    snapshot,
                }),
                Probe::VacantAt(index) => Ok(Locate::Vacant {
                    table: target,
                    index,
                }),
                Probe::Exhausted => {
                    // A vacant source slot means the key is definitively not
                    // in the source, so Full means the target is unusable.
                    let _ = source_probe;
                    Ok(Locate::Full)
                }
            }
        }
    }
}

/// Point `location` at a freshly published block and bump the slot sequence.
/// Reusing a tombstone slot decrements the tombstone counter — the counter
/// counts dead slots, and this slot is live again.
pub(crate) fn publish_slot(
    map: &SharedMap,
    layout: &Layout,
    table: usize,
    index: u64,
    block: u64,
) -> Result<()> {
    let slot = map.index_slot(table, index, layout)?;
    if slot.entry_offset.load(Ordering::Relaxed) == SLOT_TOMBSTONE {
        map.header()
            .tombstones
            .fetch_sub(1, Ordering::Relaxed);
    }
    slot.entry_offset.store(block, Ordering::Relaxed);
    slot.sequence.fetch_add(1, Ordering::Release);
    Ok(())
}

/// Relocate one live slot from the source table into the target mid-rehash
/// (an update hit the source). Writer lock held. The target store precedes
/// the source tombstone: a reader that Acquire-loads the tombstoned source
/// slot has cause to see the target entry (Release/Acquire message passing).
/// Idempotent — a crash between the two stores leaves the block reachable
/// from both tables and the next visit re-tombstones the source. Returns the
/// target (table, index) for the caller's commit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn migrate_slot(
    map: &SharedMap,
    layout: &Layout,
    record_kind: u64,
    collection: &str,
    key: &str,
    source: usize,
    source_index: u64,
    block: u64,
) -> Result<(usize, u64)> {
    let header = map.header();
    let target = 1 - source;
    let target_count = header.index_entries_target.load(Ordering::Acquire);
    let hash = super::layout::slot_hash(record_kind, collection, key);
    let step = (hash >> 33) | 1;
    let mut target_index = hash & (target_count - 1);
    let target_slot = loop {
        let slot = map.index_slot(target, target_index, layout)?;
        let current = slot.entry_offset.load(Ordering::Acquire);
        if current == block || current == SLOT_EMPTY {
            break slot;
        }
        target_index = (target_index + step) & (target_count - 1);
        if target_index == hash & (target_count - 1) {
            return Err(Error::StoreFormat {
                message: "rehash target table has no vacant slot for a migration".to_string(),
            });
        }
    };
    target_slot.entry_offset.store(block, Ordering::Release);
    let source_slot = map.index_slot(source, source_index, layout)?;
    // TOMBSTONE, never EMPTY: an in-flight source slot must not terminate
    // another key's probe chain, and the slot may still be a crash-recovery
    // double reference that the mover's idempotence check relies on.
    source_slot.entry_offset.store(SLOT_TOMBSTONE, Ordering::Release);
    source_slot.sequence.fetch_add(1, Ordering::Release);
    Ok((target, target_index))
}

/// Start an incremental rehash when a trigger fires, redis dictResize style:
/// expand when the live set reaches half the table (open addressing degrades
/// past α 0.5 — probe chains outgrow PROBE_LIMIT well before α 1), a
/// tombstone purge when tombstones outgrow live entries, and a shrink when
/// the table is 16× oversized above the floor. Writer lock held. No-op while
/// a rehash is already in flight.
pub(crate) fn maybe_start_rehash(map: &SharedMap, layout: &Layout) -> Result<()> {
    let header = map.header();
    if header.rehash_cursor.load(Ordering::Acquire) != REHASH_IDLE {
        return Ok(());
    }
    let live = header.live_entries.load(Ordering::Relaxed);
    let tombstones = header.tombstones.load(Ordering::Relaxed);
    let current = header.index_entries.load(Ordering::Acquire);
    let max = header.max_index_entries.load(Ordering::Relaxed);
    let target_size = if live * 2 >= current && current < max {
        // Expansion carries the tombstones along: the mover reverts them to
        // empty as the cursor passes, so this also serves as a purge when
        // the table is tight.
        current.saturating_mul(2).min(max)
    } else if tombstones > live && tombstones > 0 {
        current
    } else if current > MIN_TABLE_ENTRIES && live * 16 < current {
        current / 2
    } else {
        return Ok(());
    };
    // Zero the target region at its own stride; both regions are reserved at
    // the max stride so the file never moves.
    let active = header.active_index.load(Ordering::Relaxed) as usize;
    let slot_bytes = std::mem::size_of::<IndexSlot>() as u64;
    map.zero_range(layout.off_index[1 - active], target_size * slot_bytes)?;
    header
        .index_entries_target
        .store(target_size, Ordering::Release);
    header.rehash_cursor.store(0, Ordering::Release);
    Ok(())
}

/// Advance an in-flight rehash by one bounded slice: at most
/// [`REHASH_STEP_BUCKETS`] source buckets visited (≤10× that many empty
/// slots skipped), redis `dictRehash(n)` semantics. Writer lock held; call
/// once per mutation. Returns the number of live buckets relocated. Moves
/// are idempotent, so a crash mid-step costs nothing: recovery replays the
/// journal, and a half-moved bucket simply re-moves on the next visit.
pub(crate) fn rehash_step(map: &SharedMap, layout: &Layout) -> Result<u64> {
    let header = map.header();
    let mut cursor = header.rehash_cursor.load(Ordering::Acquire);
    if cursor == REHASH_IDLE {
        return Ok(0);
    }
    let source = header.active_index.load(Ordering::Acquire) as usize;
    let source_count = header.index_entries.load(Ordering::Acquire);
    let target = 1 - source;
    let target_count = header.index_entries_target.load(Ordering::Acquire);
    let mut moved = 0u64;
    let mut visited = 0u64;
    let mut empties = 0u64;
    while cursor < source_count
        && visited < REHASH_STEP_BUCKETS
        && empties < REHASH_EMPTY_VISITS
    {
        visited += 1;
        let slot = map.index_slot(source, cursor, layout)?;
        let offset = slot.entry_offset.load(Ordering::Acquire);
        if offset == SLOT_EMPTY {
            cursor += 1;
            empties += 1;
            continue;
        }
        if offset == SLOT_TOMBSTONE {
            // Dead slot: leave it — reverting tombstones to empty mid-flight
            // would cut the probe chain of keys still sitting beyond them.
            // The counter resets wholesale on completion, and the dormant
            // table is zeroed whole when the next flight targets it.
            cursor += 1;
            empties += 1;
            continue;
        }
        let snapshot = snapshot_once(map, offset)?.ok_or_else(|| Error::StoreFormat {
            message: "rehash observed an unstable entry block".to_string(),
        })?;
        let hash = super::layout::slot_hash(
            snapshot.record_kind,
            &snapshot.collection,
            &snapshot.key,
        );
        let step = (hash >> 33) | 1;
        let start = hash & (target_count - 1);
        let mut target_index = start;
        let target_slot = loop {
            let slot = map.index_slot(target, target_index, layout)?;
            let current = slot.entry_offset.load(Ordering::Acquire);
            if current == offset || current == SLOT_EMPTY {
                break slot;
            }
            target_index = (target_index + step) & (target_count - 1);
            if target_index == start {
                return Err(Error::StoreFormat {
                    message: "rehash target table has no vacant slot".to_string(),
                });
            }
        };
        // Idempotent on crash: `current == offset` re-stores the same value.
        // The source slot becomes a TOMBSTONE, never an empty — clearing it
        // would terminate the probe chain of keys parked beyond it that have
        // not been moved yet.
        target_slot.entry_offset.store(offset, Ordering::Release);
        slot.entry_offset.store(SLOT_TOMBSTONE, Ordering::Release);
        slot.sequence.fetch_add(1, Ordering::Release);
        moved += 1;
        cursor += 1;
    }
    if cursor >= source_count {
        header.active_index.store(target as u64, Ordering::Release);
        header.index_entries.store(target_count, Ordering::Relaxed);
        header
            .index_entries_target
            .store(target_count, Ordering::Relaxed);
        header.index_generation.fetch_add(1, Ordering::Release);
        header.tombstones.store(0, Ordering::Relaxed);
        header
            .rehash_cursor
            .store(REHASH_IDLE, Ordering::Release);
    } else {
        header.rehash_cursor.store(cursor, Ordering::Release);
    }
    Ok(moved)
}

#[derive(Debug)]
pub(crate) struct ScanHit {
    pub collection: String,
    pub key: String,
    pub expired: bool,
}

/// Weakly-consistent scan. Dual-table while a rehash is in flight; entries
/// dedupe by block offset (a crash mid-move can leave one reachable from
/// both tables until the next step clears the source). Optionally restricted
/// to one record kind and/or collection, and optionally only expired entries.
pub(crate) fn scan(
    map: &SharedMap,
    layout: &Layout,
    record_kind: Option<u64>,
    collection: Option<&str>,
    expired_only: bool,
    now_ms: u64,
) -> Result<Vec<ScanHit>> {
    let mut hits = Vec::new();
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for _ in 0..SCAN_RETRIES {
        let header = map.header();
        let generation0 = header.index_generation.load(Ordering::Acquire);
        let source = header.active_index.load(Ordering::Acquire) as usize;
        let rehashing =
            header.rehash_cursor.load(Ordering::Acquire) != REHASH_IDLE;
        let source_count = header.index_entries.load(Ordering::Acquire);
        let target_count = if rehashing {
            header.index_entries_target.load(Ordering::Acquire)
        } else {
            0
        };
        hits.clear();
        seen.clear();
        let mut restarted = false;

        'tables: for (table, count) in [
            (source, source_count),
            (1 - source, target_count),
        ] {
            for index in 0..count {
                if header.index_generation.load(Ordering::Acquire) != generation0 {
                    restarted = true;
                    break 'tables;
                }
                let slot = map.index_slot(table, index, layout)?;
                let offset = slot.entry_offset.load(Ordering::Acquire);
                if offset == SLOT_EMPTY || offset == SLOT_TOMBSTONE {
                    continue;
                }
                if !seen.insert(offset) {
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
    use super::super::layout::RECORD_ENTRY;

    #[test]
    fn probe_steps_are_always_odd() {
        for hash in [0u64, 1, 0xdead_beef, u64::MAX] {
            assert_eq!(((hash >> 33) | 1) & 1, 1);
        }
    }

    /// The rehash bound is the incremental contract: one step relocates at
    /// most REHASH_STEP_BUCKETS live buckets, a flight always runs to
    /// completion, and the surviving data hash-locates afterwards. The fill
    /// and churn phases exercise the expand/purge/shrink triggers implicitly;
    /// the measured flight is synthesized over a settled table so the moved
    /// total is exact.
    #[test]
    fn rehash_step_moves_at_most_64_buckets_per_call() {
        use crate::store::shm::client::{Require, ShmClient};
        use crate::store::shm::config::ShmCreateConfig;
        use crate::value::Value;

        let dir = std::env::temp_dir().join(format!(
            "openkeyv-rehash-bound-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bound.shm");
        let client = ShmClient::create(
            &ShmCreateConfig::new(&path)
                .with_capacity_bytes(16 * 1024 * 1024)
                .with_index_entries(1_024),
        )
        .expect("test store must create");

        // Fill past two expansion thresholds, then churn down: expansions
        // fire at live ≥ size/2, purges at tombstones > live, shrinks at
        // live < size/16 — all three run during this storm.
        for i in 0..1600u32 {
            client
                .perform_upsert("c", &format!("k{i}"), &Value::utf8("v"), None, Require::Absent)
                .unwrap_or_else(|e| panic!("put {i} failed: {e:?}"));
        }
        for i in 0..1550u32 {
            let outcome = client
                .perform_remove(RECORD_ENTRY, "c", &format!("k{i}"), None, false)
                .expect("delete must apply");
            assert!(
                matches!(outcome, crate::store::shm::client::RemoveOutcome::Removed),
                "delete {i} must land, never silently absent"
            );
        }
        // Settle: drive every trigger to quiescence (shrink floors at
        // MIN_TABLE_ENTRIES; purges zero the tombstones).
        for _ in 0..256 {
            maybe_start_rehash(&client.map, &client.layout).expect("rehash start");
            let moved = rehash_step(&client.map, &client.layout).expect("rehash step");
            let idle =
                client.map.header().rehash_cursor.load(Ordering::Acquire) == REHASH_IDLE;
            if idle && moved == 0 {
                break;
            }
        }
        {
            let header = client.map.header();
            assert_eq!(header.rehash_cursor.load(Ordering::Acquire), REHASH_IDLE);
            // 50 survivors plus the collection registry entry.
            assert_eq!(header.live_entries.load(Ordering::Relaxed), 51);
            assert_eq!(
                header.index_entries.load(Ordering::Acquire),
                1024,
                "shrink must floor at the minimum table size"
            );
        }

        // Fresh live entries, then synthesize the purge flight exactly as
        // maybe_start_rehash would (same table size, cursor at zero).
        for i in 100..150u32 {
            client
                .perform_upsert("c", &format!("k{i}"), &Value::utf8("w"), None, Require::Absent)
                .unwrap_or_else(|e| panic!("re-put {i} failed: {e:?}"));
        }
        // Synthesize the purge flight exactly as maybe_start_rehash would
        // (same table size, cursor at zero).
        {
            let header = client.map.header();
            let active = header.active_index.load(Ordering::Acquire) as usize;
            let size = header.index_entries.load(Ordering::Acquire);
            let slot_bytes = std::mem::size_of::<IndexSlot>() as u64;
            client
                .map
                .zero_range(client.layout.off_index[1 - active], size * slot_bytes)
                .expect("zero target");
            header.index_entries_target.store(size, Ordering::Release);
            header.rehash_cursor.store(0, Ordering::Release);
        }

        let mut steps = 0;
        let mut total_moved = 0u64;
        loop {
            let moved = rehash_step(&client.map, &client.layout).expect("rehash step");
            assert!(moved <= REHASH_STEP_BUCKETS, "step moved {moved} buckets");
            total_moved += moved;
            steps += 1;
            assert!(steps < 4096, "flight must terminate");
            if client.map.header().rehash_cursor.load(Ordering::Acquire) == REHASH_IDLE {
                break;
            }
        }
        let header = client.map.header();
        assert_eq!(header.tombstones.load(Ordering::Relaxed), 0);
        // 50 survivors + 50 fresh entries + the collection registry entry.
        assert_eq!(header.live_entries.load(Ordering::Relaxed), 101);
        assert_eq!(total_moved, 101, "every live bucket must relocate exactly once");
        assert!(
            steps >= 1024 / REHASH_STEP_BUCKETS as usize,
            "a 1024-bucket flight cannot finish in fewer than 16 bounded steps"
        );

        // Every survivor hash-locates through the rebuilt table.
        for i in [1550, 1551, 1597, 1598, 1599] {
            match locate(&client.map, &client.layout, RECORD_ENTRY, "c", &format!("k{i}"))
                .expect("locate must work")
            {
                Locate::Live { .. } => {}
                other => panic!("k{i} must survive the flight, got {other:?}"),
            }
        }
        for i in 100..150u32 {
            match locate(&client.map, &client.layout, RECORD_ENTRY, "c", &format!("k{i}"))
                .expect("locate must work")
            {
                Locate::Live { .. } => {}
                other => panic!("k{i} must survive the flight, got {other:?}"),
            }
        }
        drop(client);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
