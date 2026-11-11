//! Arena allocator for the shared-memory store.
//!
//! A bump allocator with per-size-class free lists. Blocks never move and are
//! never coalesced: freed blocks return to the exact size class they were
//! allocated from, and a larger free block is split downward to satisfy a
//! smaller request. This keeps `alloc`/`free` to a handful of atomic stores and
//! makes reuse of steady-state churn (the dominant mcpstore pattern) exact.
//! The known ceiling is fragmentation when value sizes grow across classes;
//! the remedy is a larger fixed file, not a remap.

use super::layout::{
    BLOCK_HEADER_BYTES, EntryBlockHeader, FREE_CLASSES, Layout, MIN_BLOCK_BYTES, SharedMap,
};
use crate::error::{Error, Result};
use std::sync::atomic::Ordering;

pub(crate) struct BlockHandle {
    pub offset: u64,
    pub class: usize,
}

/// Smallest class index whose blocks cover `bytes` total block size.
pub(crate) fn class_for(total_bytes: usize) -> usize {
    let bytes = total_bytes.max(MIN_BLOCK_BYTES);
    let class = bytes.next_power_of_two().trailing_zeros() as usize - 8;
    debug_assert!(class < FREE_CLASSES);
    class.min(FREE_CLASSES - 1)
}

pub(crate) fn class_bytes(class: usize) -> usize {
    MIN_BLOCK_BYTES << class
}

/// Size class a fresh block for `payload` bytes lands in — the single
/// authority for alloc and compaction alike.
pub(crate) fn minimal_class_for_payload(payload: usize) -> usize {
    class_for(BLOCK_HEADER_BYTES + align8(payload))
}

/// Allocate a block with a `BLOCK_HEADER_BYTES`-byte header plus `payload`
/// bytes. Only ever called while holding the writer lock.
pub(crate) fn alloc(map: &SharedMap, layout: &Layout, payload: usize) -> Result<BlockHandle> {
    let total = BLOCK_HEADER_BYTES + align8(payload);
    let class = class_for(total);
    if class >= FREE_CLASSES {
        return Err(Error::StoreCapacity {
            requested: total as u64,
            available: layout.arena_capacity,
        });
    }

    // Reuse the smallest free block that fits, splitting downward as needed.
    for source in class..FREE_CLASSES {
        let head = map.freelist_head(source, layout)?;
        let offset = head.load(Ordering::Relaxed);
        if offset != 0 {
            let header: &EntryBlockHeader = map.entry_block(offset)?;
            let next = header.next_free.load(Ordering::Relaxed);
            head.store(next, Ordering::Relaxed);
            map.header().free_counts[source].fetch_sub(1, Ordering::Relaxed);
            let mut current_class = source;
            while current_class > class {
                current_class -= 1;
                let buddy = offset + class_bytes(current_class) as u64;
                push_free(map, layout, buddy, current_class)?;
            }
            return Ok(BlockHandle { offset, class });
        }
    }

    // Nothing free: bump the frontier.
    let high_water = map.header().arena_high_water.load(Ordering::Relaxed);
    let size = class_bytes(class) as u64;
    let end = high_water + size;
    if end > layout.arena_capacity {
        return Err(Error::StoreCapacity {
            requested: size,
            available: layout.arena_capacity - high_water,
        });
    }
    map.header().arena_high_water.store(end, Ordering::Relaxed);
    Ok(BlockHandle {
        offset: layout.off_arena + high_water,
        class,
    })
}

/// Return a block to its size-class free list. The size class is read from the
/// block header, so callers never have to track it. `next_free` is stored
/// inside the same header the free list uses, and the field is meaningless
/// while the block is allocated, so no extra state is needed.
pub(crate) fn free(map: &SharedMap, layout: &Layout, offset: u64) -> Result<()> {
    let class = map.entry_block(offset)?.size_class.load(Ordering::Relaxed) as usize;
    debug_assert!(class < FREE_CLASSES);
    push_free(map, layout, offset, class)
}

fn push_free(map: &SharedMap, layout: &Layout, offset: u64, class: usize) -> Result<()> {
    let head = map.freelist_head(class, layout)?;
    let header = map.entry_block(offset)?;
    header
        .next_free
        .store(head.load(Ordering::Relaxed), Ordering::Relaxed);
    head.store(offset, Ordering::Relaxed);
    map.header().free_counts[class].fetch_add(1, Ordering::Relaxed);
    Ok(())
}

const fn align8(v: usize) -> usize {
    (v + 7) & !7
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_cover_their_ranges() {
        assert_eq!(class_for(0), 0);
        assert_eq!(class_for(MIN_BLOCK_BYTES), 0);
        assert_eq!(class_for(MIN_BLOCK_BYTES + 1), 1);
        assert_eq!(class_for(class_bytes(4)), 4);
        assert_eq!(class_bytes(0), 256);
        assert_eq!(class_bytes(1), 512);
    }
}
