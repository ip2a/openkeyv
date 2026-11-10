//! Shared-memory file format.
//!
//! This module owns every byte-level definition of the backing file and is the
//! only place in the crate allowed to turn mmap offsets into references. All
//! shared state is expressed as fixed-size `#[repr(C)]` structs of `AtomicU64`
//! so that multiple processes can map the same file and observe each other's
//! writes without any Rust-level pointers crossing the boundary.
//!
//! File layout (offsets are computed by [`Layout`]):
//!
//! ```text
//! HEADER      4096 B   format + counters (all atomics)
//! JOURNAL     4096 B   single in-flight mutation record
//! FREELISTS   FREE_CLASSES * 8 B   one head per size class
//! INDEX_A     index_entries * 16 B  open-addressed slots
//! INDEX_B     index_entries * 16 B  shadow table for compaction
//! RING        change_retention * 16 B  change-feed slots
//! ARENA       rest of the file      entry/event blocks
//! ```

use crate::error::{Error, Result};
use memmap2::MmapMut;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const MAGIC: u64 = u64::from_le_bytes(*b"OKVSHM1\0");
pub(crate) const FORMAT_VERSION: u64 = 1;
pub(crate) const HEADER_BYTES: usize = 4096;
pub(crate) const JOURNAL_BYTES: usize = 4096;

/// Minimum block size and allocator granularity.
pub(crate) const MIN_BLOCK_BYTES: usize = 256;
/// Fixed header of an entry block; payload follows immediately.
pub(crate) const BLOCK_HEADER_BYTES: usize = 128;
/// Fixed header of a change-feed event block.
pub(crate) const EVENT_HEADER_BYTES: usize = 64;
/// Size classes are `MIN_BLOCK_BYTES << class`. 32 classes cover up to 2^39 B.
pub(crate) const FREE_CLASSES: usize = 32;

pub(crate) const RECORD_ENTRY: u64 = 1;
pub(crate) const RECORD_REGISTRY: u64 = 2;

pub(crate) const SLOT_EMPTY: u64 = 0;
pub(crate) const SLOT_TOMBSTONE: u64 = 1;

pub(crate) const STATE_EMPTY: u64 = 0;
pub(crate) const STATE_READY: u64 = 1;
pub(crate) const STATE_DESTROYING: u64 = 2;

pub(crate) const OP_PUT: u64 = 1;
pub(crate) const OP_DELETE: u64 = 2;

pub(crate) const KIND_UPSERT: u64 = 1;
pub(crate) const KIND_REMOVE: u64 = 2;
pub(crate) const KIND_CULL: u64 = 3;

pub(crate) const JOURNAL_IDLE: u64 = 0;
pub(crate) const JOURNAL_PREPARED: u64 = 1;
pub(crate) const JOURNAL_PUBLISHED: u64 = 2;
pub(crate) const JOURNAL_RECORDED: u64 = 3;

const HEADER_FIELDS: usize = 19;
const JOURNAL_FIELDS: usize = 9;

/// One hash-table slot. `entry_offset` is `SLOT_EMPTY`, `SLOT_TOMBSTONE`, or an
/// 8-aligned arena offset. `sequence` is bumped on every slot mutation so
/// lock-free readers can detect that the slot changed underneath them.
#[repr(C)]
pub(crate) struct IndexSlot {
    pub entry_offset: AtomicU64,
    pub sequence: AtomicU64,
}

/// One change-feed ring slot. `sequence` doubles as the global change revision
/// of the event stored at `event_offset`; a slot whose `sequence` differs from
/// the expected revision is a gap (lost to a crash) and is skipped.
#[repr(C)]
pub(crate) struct ChangeRingSlot {
    pub event_offset: AtomicU64,
    pub sequence: AtomicU64,
}

/// Fixed file header. Every counter lives here so any process can observe
/// global progress without taking the writer lock.
#[repr(C)]
pub(crate) struct Header {
    pub magic: AtomicU64,
    pub format_version: AtomicU64,
    pub layout_bytes: AtomicU64,
    pub header_bytes: AtomicU64,
    pub store_id0: AtomicU64,
    pub store_id1: AtomicU64,
    /// Bumped by `destroy`; change streams end when they observe a new value.
    pub store_generation: AtomicU64,
    /// `STATE_EMPTY`, `STATE_READY`, or `STATE_DESTROYING`.
    pub state: AtomicU64,
    /// 0 or 1 — which index table is authoritative.
    pub active_index: AtomicU64,
    /// Bumped on every reindex; readers restart scans when it moves.
    pub index_generation: AtomicU64,
    /// Total change events ever assigned a revision in this generation.
    pub change_revision: AtomicU64,
    pub next_operation_id: AtomicU64,
    pub live_entries: AtomicU64,
    pub tombstones: AtomicU64,
    /// Arena-relative bump-allocation frontier.
    pub arena_high_water: AtomicU64,
    pub arena_capacity: AtomicU64,
    pub index_entries: AtomicU64,
    pub change_retention: AtomicU64,
    pub creation_unix_ms: AtomicU64,
    pub reserved: [AtomicU64; HEADER_BYTES / 8 - HEADER_FIELDS],
}

/// Fixed-size journal record describing the single in-flight mutation.
#[repr(C)]
pub(crate) struct JournalRecord {
    pub state: AtomicU64,
    pub operation_id: AtomicU64,
    /// `KIND_UPSERT`, `KIND_REMOVE`, or `KIND_CULL`.
    pub kind: AtomicU64,
    pub slot_table: AtomicU64,
    pub slot_index: AtomicU64,
    pub old_block: AtomicU64,
    /// Arena offset of the freshly written block; 0 for deletes.
    pub new_block: AtomicU64,
    /// 0 = nothing freed yet, 1 = `new_block` freed, 2 = `old_block` freed.
    /// Written *before* the corresponding free so a crash can never cause a
    /// double free; the worst case is a bounded leak.
    pub freed: AtomicU64,
    pub emit_change: AtomicU64,
    pub reserved: [AtomicU64; JOURNAL_BYTES / 8 - JOURNAL_FIELDS],
}

/// Fixed header of an entry block. Payload
/// (`collection || key || value`, zero-padded to 8) follows at
/// `BLOCK_HEADER_BYTES`. Blocks are immutable once `sequence` is even; updates
/// are copy-on-write, so readers only have to guard against a block being
/// freed and reused, which the sequence + slot re-verification catches.
#[repr(C)]
pub(crate) struct EntryBlockHeader {
    /// Odd = being written, even = published, 0 = never published.
    pub sequence: AtomicU64,
    /// `RECORD_ENTRY` or `RECORD_REGISTRY`.
    pub flags: AtomicU64,
    /// `ValueKind::tag()` of the stored value; 0 for registries.
    pub value_kind: AtomicU64,
    pub created_at_ms: AtomicU64,
    /// Unix millis; 0 = never expires.
    pub expires_at_ms: AtomicU64,
    pub revision0: AtomicU64,
    pub revision1: AtomicU64,
    pub collection_len: AtomicU64,
    pub key_len: AtomicU64,
    pub value_len: AtomicU64,
    /// Free-list link; only meaningful while the block is free.
    pub next_free: AtomicU64,
    /// Size class the block was allocated from, for exact free.
    pub size_class: AtomicU64,
    pub reserved: [AtomicU64; 4],
}

/// Fixed header of a change-feed event block. Payload
/// (`collection || key`, zero-padded to 8) follows at `EVENT_HEADER_BYTES`.
/// Event blocks are immutable once published.
#[repr(C)]
pub(crate) struct ChangeEventBlock {
    pub sequence: AtomicU64,
    /// Global change revision; equals the owning ring slot's `sequence`.
    pub revision: AtomicU64,
    /// `OP_PUT` or `OP_DELETE`.
    pub operation: AtomicU64,
    pub occurred_at_ms: AtomicU64,
    pub store_generation: AtomicU64,
    /// Mutation id that produced this event; used for crash-recovery dedupe.
    pub operation_id: AtomicU64,
    pub collection_len: AtomicU64,
    pub key_len: AtomicU64,
}

/// Computes every region offset from the three layout parameters.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Layout {
    pub file_len: u64,
    pub index_entries: u64,
    pub change_retention: u64,
    pub off_freelist: u64,
    pub off_index: [u64; 2],
    pub off_ring: u64,
    pub off_arena: u64,
    pub arena_capacity: u64,
}

impl Layout {
    /// Build the layout for a fresh file of `file_len` bytes.
    pub(crate) fn compute(index_entries: u64, change_retention: u64, file_len: u64) -> Layout {
        let off_freelist = (HEADER_BYTES + JOURNAL_BYTES) as u64;
        let off_index0 = align8(off_freelist + (FREE_CLASSES * 8) as u64);
        let slot_bytes = index_entries * std::mem::size_of::<IndexSlot>() as u64;
        let off_index1 = off_index0 + slot_bytes;
        let off_ring = off_index1 + slot_bytes;
        let off_arena =
            align8(off_ring + change_retention * std::mem::size_of::<ChangeRingSlot>() as u64);
        Layout {
            file_len,
            index_entries,
            change_retention,
            off_freelist,
            off_index: [off_index0, off_index1],
            off_ring,
            off_arena,
            arena_capacity: file_len.saturating_sub(off_arena),
        }
    }

    /// Recompute the layout from the header and validate it against the actual
    /// file length. Used by `open`; any disagreement is a `StoreFormat` error.
    pub(crate) fn from_header(header: &Header, file_len: u64) -> Result<Layout> {
        let layout_bytes = header.layout_bytes.load(Ordering::Acquire);
        if layout_bytes != file_len {
            return Err(Error::StoreFormat {
                message: format!(
                    "header records {layout_bytes} bytes but the file holds {file_len}"
                ),
            });
        }
        let index_entries = header.index_entries.load(Ordering::Acquire);
        let change_retention = header.change_retention.load(Ordering::Acquire);
        let layout = Self::compute(index_entries, change_retention, file_len);
        let expected_capacity = header.arena_capacity.load(Ordering::Acquire);
        if layout.arena_capacity != expected_capacity || layout.arena_capacity == 0 {
            return Err(Error::StoreFormat {
                message: format!(
                    "arena capacity mismatch: header {expected_capacity}, layout {}",
                    layout.arena_capacity
                ),
            });
        }
        Ok(layout)
    }
}

const fn align8(v: u64) -> u64 {
    (v + 7) & !7
}

/// Owns the mmap and turns validated offsets into references.
///
/// Every accessor checks bounds and alignment before the (single, localized)
/// unsafe pointer derivation; a corrupted offset is a loud error, never a panicky
/// out-of-bounds dereference. References borrow the map, and every shared field
/// is an `AtomicU64`, so cross-process visibility needs nothing else.
pub(crate) struct SharedMap {
    map: MmapMut,
}

// SAFETY: the mmap address space is process-wide and unaffected by moving this
// struct; all shared mutation goes through AtomicU64 fields.
unsafe impl Send for SharedMap {}
unsafe impl Sync for SharedMap {}

impl SharedMap {
    pub(crate) fn map(file: &std::fs::File, len: u64) -> Result<Self> {
        let map = unsafe {
            memmap2::MmapOptions::new()
                .len(len as usize)
                .map_mut(file)
                .map_err(|error| Error::StoreSetup {
                    message: format!("mmap of shared store failed: {error}"),
                })?
        };
        Ok(Self { map })
    }

    pub(crate) fn header(&self) -> &Header {
        // SAFETY: the header region is fully inside the mapping by construction.
        unsafe { &*(self.map.as_ptr() as *const Header) }
    }

    pub(crate) fn journal(&self) -> &JournalRecord {
        // SAFETY: the journal page is fully inside the mapping by construction.
        unsafe { &*(self.map.as_ptr().add(HEADER_BYTES) as *const JournalRecord) }
    }

    fn ref_at<T>(&self, offset: u64, size: usize) -> Result<&T> {
        let offset = offset as usize;
        if offset % std::mem::align_of::<T>() != 0 || offset + size > self.map.len() {
            return Err(Error::StoreFormat {
                message: format!(
                    "shared memory offset {offset:#x} (+{size}) is out of bounds or misaligned"
                ),
            });
        }
        // SAFETY: bounds and alignment checked immediately above; offset came
        // from the layout or from a validated block header.
        Ok(unsafe { &*(self.map.as_ptr().add(offset) as *const T) })
    }

    /// `slice_at::<T>(offset, count)` borrows `count` consecutive `T`s with the
    /// same bounds and alignment checks as [`Self::ref_at`].
    fn slice_at<T>(&self, offset: u64, count: usize) -> Result<&[T]> {
        let offset = offset as usize;
        let size = count * std::mem::size_of::<T>();
        if offset % std::mem::align_of::<T>() != 0 || offset + size > self.map.len() {
            return Err(Error::StoreFormat {
                message: format!(
                    "shared memory offset {offset:#x} (+{size}) is out of bounds or misaligned"
                ),
            });
        }
        // SAFETY: bounds and alignment checked immediately above.
        Ok(unsafe { std::slice::from_raw_parts(self.map.as_ptr().add(offset) as *const T, count) })
    }

    pub(crate) fn index_slot(
        &self,
        table: usize,
        index: u64,
        layout: &Layout,
    ) -> Result<&IndexSlot> {
        debug_assert!(table < 2);
        let offset = layout.off_index[table] + index * std::mem::size_of::<IndexSlot>() as u64;
        self.ref_at::<IndexSlot>(offset, std::mem::size_of::<IndexSlot>())
    }

    pub(crate) fn ring_slot(&self, index: u64, layout: &Layout) -> Result<&ChangeRingSlot> {
        let offset = layout.off_ring + index * std::mem::size_of::<ChangeRingSlot>() as u64;
        self.ref_at::<ChangeRingSlot>(offset, std::mem::size_of::<ChangeRingSlot>())
    }

    pub(crate) fn entry_block(&self, offset: u64) -> Result<&EntryBlockHeader> {
        self.ref_at::<EntryBlockHeader>(offset, BLOCK_HEADER_BYTES)
    }

    pub(crate) fn event_block(&self, offset: u64) -> Result<&ChangeEventBlock> {
        self.ref_at::<ChangeEventBlock>(offset, EVENT_HEADER_BYTES)
    }

    pub(crate) fn freelist_head(&self, class: usize, layout: &Layout) -> Result<&AtomicU64> {
        debug_assert!(class < FREE_CLASSES);
        let offset = layout.off_freelist + (class * 8) as u64;
        self.ref_at::<AtomicU64>(offset, 8)
    }

    /// Relaxed-load `words` consecutive `u64`s starting at `offset`.
    pub(crate) fn read_words(&self, offset: u64, words: usize) -> Result<Vec<u64>> {
        let slice: &[AtomicU64] = self.slice_at::<AtomicU64>(offset, words)?;
        Ok(slice.iter().map(|w| w.load(Ordering::Relaxed)).collect())
    }

    /// Relaxed-store `words` at `offset` (used while a block is unpublished).
    pub(crate) fn write_words(&self, offset: u64, words: &[u64]) -> Result<()> {
        let slice: &[AtomicU64] = self.slice_at::<AtomicU64>(offset, words.len())?;
        for (slot, word) in slice.iter().zip(words) {
            slot.store(*word, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Zero `len` bytes at `offset` (len must be a multiple of 8).
    pub(crate) fn zero_range(&self, offset: u64, len: u64) -> Result<()> {
        let words = (len / 8) as usize;
        let slice: &[AtomicU64] = self.slice_at::<AtomicU64>(offset, words)?;
        for slot in slice {
            slot.store(0, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// FNV-1a 64. Deliberately hand-rolled (not `DefaultHasher`) so the slot a key
/// hashes to is identical in every process regardless of compiler version.
pub(crate) fn slot_hash(kind: u64, collection: &str, key: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut mix = |byte: u8| {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    };
    for byte in kind.to_le_bytes() {
        mix(byte);
    }
    for byte in collection.as_bytes() {
        mix(*byte);
    }
    mix(0);
    for byte in key.as_bytes() {
        mix(*byte);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_structs_have_the_documented_sizes() {
        assert_eq!(std::mem::size_of::<Header>(), HEADER_BYTES);
        assert_eq!(std::mem::size_of::<JournalRecord>(), JOURNAL_BYTES);
        assert_eq!(std::mem::size_of::<IndexSlot>(), 16);
        assert_eq!(std::mem::size_of::<ChangeRingSlot>(), 16);
        assert_eq!(std::mem::size_of::<EntryBlockHeader>(), BLOCK_HEADER_BYTES);
        assert_eq!(std::mem::size_of::<ChangeEventBlock>(), EVENT_HEADER_BYTES);
    }

    #[test]
    fn layout_regions_do_not_overlap_and_leave_an_arena() {
        let layout = Layout::compute(65_536, 10_000, 256 * 1024 * 1024);
        assert_eq!(layout.off_freelist, 8192);
        let slot_bytes = 65_536 * 16;
        assert_eq!(
            layout.off_index[0],
            align8(layout.off_freelist + FREE_CLASSES as u64 * 8)
        );
        assert_eq!(layout.off_index[1], layout.off_index[0] + slot_bytes);
        assert_eq!(layout.off_ring, layout.off_index[1] + slot_bytes);
        assert!(layout.off_arena >= layout.off_ring + 10_000 * 16);
        assert_eq!(layout.arena_capacity, 256 * 1024 * 1024 - layout.off_arena);
    }

    #[test]
    fn layout_without_change_ring_still_leaves_an_arena() {
        let layout = Layout::compute(1_024, 0, 16 * 1024 * 1024);
        assert!(layout.arena_capacity > 0);
    }

    #[test]
    fn hash_is_stable_and_spreads() {
        let a = slot_hash(RECORD_ENTRY, "mcp:tools", "alpha");
        let b = slot_hash(RECORD_ENTRY, "mcp:tools", "beta");
        let c = slot_hash(RECORD_REGISTRY, "mcp:tools", "");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(a, slot_hash(RECORD_ENTRY, "mcp:tools", "alpha"));
    }
}
