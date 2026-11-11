//! Shared-memory store client: lifecycle, locking, crash recovery, and the
//! mutation pipeline.
//!
//! Concurrency model:
//! - Readers never take any lock. They walk the hash table with atomics and
//!   re-verify slot/generation after each snapshot copy.
//! - Writers serialize on one process-wide tokio mutex plus a cross-process
//!   `flock` on `<path>.write.lock`. The flock is only ever held inside a
//!   synchronous section (never across an `.await`), so a crashed writer's
//!   lock is released by the OS and never wedges a reader or another writer.
//! - Every journaled mutation follows `Prepared → Published → Recorded → Idle`
//!   in the single journal record; the next opener replays the record and
//!   finishes or rolls back the half-done work exactly once per operation id.

use super::arena;
use super::change::{self, now_unix_ms};
use super::config::ShmCreateConfig;
use super::entry::{self, EntrySnapshot};
use super::index::{self, Locate};
use super::notify;
use super::layout::{
    ChangeRingSlot, FORMAT_VERSION, FREE_CLASSES, HEADER_BYTES, IndexSlot, JOURNAL_BYTES,
    JOURNAL_IDLE, JOURNAL_PREPARED, JOURNAL_PUBLISHED, JOURNAL_RECORDED, JournalRecord, KIND_CULL,
    KIND_REMOVE, KIND_UPSERT, Layout, MAGIC, OP_DELETE, OP_PUT,
    REHASH_IDLE, RECORD_ENTRY, RECORD_REGISTRY, SLOT_EMPTY, SLOT_TOMBSTONE, STATE_DESTROYING,
    STATE_EMPTY, STATE_READY, SharedMap, slot_hash,
};
use crate::change::{ChangeCause, ChangeStart};
use crate::error::{Error, Result};
use crate::protocol::Revision;
use crate::value::{Value, ValueKind};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// Handle to a store's expiry sweeper thread. Dropping the handle stops the
/// thread AND joins it, so the sweeper can never outlive the store instance
/// that spawned it: it holds a client Arc (keeping the attach/write flocks
/// and fds alive) and its own sweep-lock fd — a recreate right after drop
/// would race those leftovers if teardown stayed detached.
pub(crate) struct ExpirySweeper {
    stop: Option<Arc<std::sync::atomic::AtomicBool>>,
    _worker: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ExpirySweeper {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.store(true, Ordering::Release);
        }
        // The thread exits within one 25ms tick (plus at most one bounded
        // in-progress sweep); join waits out that window so every fd and
        // flock of this store instance is closed by the time drop returns.
        if let Some(worker) = self._worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) struct ShmClient {
    pub(crate) map: SharedMap,
    pub(crate) layout: Layout,
    attach_lock: File,
    write_lock: File,
    path: PathBuf,
    /// Serializes tasks of *this* process; the cross-process order comes from
    /// the file lock. Both are required: two tasks sharing one open file
    /// description would not conflict on flock alone.
    pub(crate) local_write: AsyncMutex<()>,
}

// SAFETY: the client owns an mmap whose address space is process-wide; every
// byte shared across processes is read or written through AtomicU64 fields.
// The fds and the async mutex are plain owned state.
unsafe impl Send for ShmClient {}
unsafe impl Sync for ShmClient {}

fn open_lock_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
        .map_err(|error| Error::StoreSetup {
            message: format!("cannot open lock file {}: {error}", path.display()),
        })
}

fn check_private(path: &Path, meta: &std::fs::Metadata) -> Result<()> {
    #[cfg(unix)]
    {
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(Error::PathSecurity(format!(
                "{} must not be accessible by group or other (mode {:o})",
                path.display(),
                mode & 0o777
            )));
        }
    }
    Ok(())
}

/// The parent directory only has to keep other users from creating or
/// replacing the store and lock files, so its group/other *write* bits are
/// rejected. Read and search bits are harmless: every file we place there is
/// 0600, which is what makes a typical shared home directory (0755) a valid
/// store location.
fn check_parent_writable(path: &Path, meta: &std::fs::Metadata) -> Result<()> {
    #[cfg(unix)]
    {
        let mode = meta.permissions().mode();
        if mode & 0o022 != 0 {
            return Err(Error::PathSecurity(format!(
                "{} must not be group- or world-writable (mode {:o}); place the store in \
                 a directory only the owner can write",
                path.display(),
                mode & 0o777
            )));
        }
    }
    Ok(())
}

fn validate_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| Error::StoreSetup {
        message: format!("path {} has no parent directory", path.display()),
    })?;
    let meta = std::fs::metadata(parent).map_err(|_| Error::StoreSetup {
        message: format!(
            "parent directory {} does not exist; create it first",
            parent.display()
        ),
    })?;
    if !meta.is_dir() {
        return Err(Error::StoreSetup {
            message: format!("parent {} is not a directory", parent.display()),
        });
    }
    check_parent_writable(parent, &meta)
}

pub(crate) fn attach_lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".attach.lock");
    PathBuf::from(name)
}

pub(crate) fn write_lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".write.lock");
    PathBuf::from(name)
}

impl ShmClient {
    /// Create (or reset) the backing file. Exclusive while any other process
    /// still holds the attach lock.
    pub(crate) fn create(config: &ShmCreateConfig) -> Result<Self> {
        config.validate()?;
        validate_parent(&config.path)?;

        if let Ok(meta) = std::fs::metadata(&config.path) {
            check_private(&config.path, &meta)?;
        }
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&config.path)
            .map_err(|error| Error::StoreSetup {
                message: format!("cannot open {}: {error}", config.path.display()),
            })?;
        let attach_lock = open_lock_file(&attach_lock_path(&config.path))?;
        let write_lock = open_lock_file(&write_lock_path(&config.path))?;

        match FileExt::try_lock_exclusive(&attach_lock) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(Error::StoreInUse {
                    path: config.path.display().to_string(),
                });
            }
            Err(error) => {
                return Err(Error::StoreSetup {
                    message: format!("attach lock failed: {error}"),
                });
            }
        }

        data_file
            .set_len(config.capacity_bytes)
            .map_err(|error| Error::StoreSetup {
                message: format!("cannot size {}: {error}", config.path.display()),
            })?;
        let map = SharedMap::map(&data_file, config.capacity_bytes)?;
        let layout = Layout::plan(
            config.capacity_bytes,
            config.change_retention as u64,
            config.index_entries.map(u64::from),
        )?;

        let client = Self {
            map,
            layout,
            attach_lock,
            write_lock,
            path: config.path.clone(),
            local_write: AsyncMutex::new(()),
        };

        // A pre-existing valid store is reset from scratch; create is an
        // explicit destructive operation. The exclusive attach lock already
        // excludes every other process, so the writer lock is not needed.
        let header = client.map.header();
        let uninitialized = header.magic.load(Ordering::Acquire) != MAGIC
            || header.format_version.load(Ordering::Acquire) != FORMAT_VERSION
            || header.state.load(Ordering::Acquire) == STATE_EMPTY;
        if uninitialized {
            client.reset_with_new_id()?;
        } else {
            client.reset_excluding_id()?;
        }

        // Downgrade to a shared attach lock (flock converts atomically).
        FileExt::lock_shared(&client.attach_lock).map_err(|error| Error::StoreSetup {
            message: format!("attach downgrade failed: {error}"),
        })?;
        Ok(client)
    }

    /// Attach to an existing store and replay crash recovery.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        validate_parent(path)?;
        let meta = std::fs::metadata(path).map_err(|error| Error::StoreSetup {
            message: format!("shm store {} not found: {error}", path.display()),
        })?;
        check_private(path, &meta)?;
        if !meta.is_file() {
            return Err(Error::StoreSetup {
                message: format!("{} is not a file", path.display()),
            });
        }

        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| Error::StoreSetup {
                message: format!("cannot open {}: {error}", path.display()),
            })?;
        let attach_lock = open_lock_file(&attach_lock_path(path))?;
        let write_lock = open_lock_file(&write_lock_path(path))?;

        // Blocking shared attach: waits for any in-flight create/reset.
        FileExt::lock_shared(&attach_lock).map_err(|error| Error::StoreSetup {
            message: format!("attach lock failed: {error}"),
        })?;

        let file_len = data_file
            .metadata()
            .map_err(|error| Error::StoreSetup {
                message: format!("cannot stat {}: {error}", path.display()),
            })?
            .len();
        let map = SharedMap::map(&data_file, file_len)?;
        let header = map.header();
        if header.magic.load(Ordering::Acquire) != MAGIC {
            return Err(Error::StoreFormat {
                message: format!("{} is not an openkeyv shm store", path.display()),
            });
        }
        if header.format_version.load(Ordering::Acquire) != FORMAT_VERSION {
            return Err(Error::StoreFormat {
                message: format!(
                    "{} was written by format version {}, this build understands \
                     {FORMAT_VERSION}; recreate the store (create mode resets it) or use a \
                     build that matches version {}",
                    path.display(),
                    header.format_version.load(Ordering::Acquire),
                    header.format_version.load(Ordering::Acquire)
                ),
            });
        }
        if header.state.load(Ordering::Acquire) == STATE_EMPTY {
            return Err(Error::StoreFormat {
                message: format!("{} exists but was never initialized", path.display()),
            });
        }
        let layout = Layout::from_header(header, file_len)?;

        let client = Self {
            map,
            layout,
            attach_lock,
            write_lock,
            path: path.to_path_buf(),
            local_write: AsyncMutex::new(()),
        };

        // Cold-path recovery: a blocking flock is fine here because open is
        // synchronous and completes before any async work starts.
        FileExt::lock_exclusive(&client.write_lock).map_err(|error| Error::StoreSetup {
            message: format!("write lock failed: {error}"),
        })?;
        let recovery = client.recover();
        let _ = FileExt::unlock(&client.write_lock);
        recovery?;
        Ok(client)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn store_id(&self) -> [u8; 16] {
        let header = self.map.header();
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&header.store_id0.load(Ordering::Acquire).to_le_bytes());
        id[8..].copy_from_slice(&header.store_id1.load(Ordering::Acquire).to_le_bytes());
        id
    }

    // ----- lifecycle -------------------------------------------------------

    fn reset_with_new_id(&self) -> Result<()> {
        let mut id = [0u8; 16];
        getrandom::fill(&mut id).map_err(|error| Error::StoreSetup {
            message: format!("cannot generate store id: {error}"),
        })?;
        let id0 = u64::from_le_bytes(id[..8].try_into().unwrap());
        let id1 = u64::from_le_bytes(id[8..].try_into().unwrap());
        self.reset_common(id0, id1)
    }

    /// Reset all data while keeping the store id (crashed-destroy recovery).
    fn reset_excluding_id(&self) -> Result<()> {
        let header = self.map.header();
        self.reset_common(
            header.store_id0.load(Ordering::Acquire),
            header.store_id1.load(Ordering::Acquire),
        )
    }

    /// Full logical reset: zero every index/ring/freelist/journal word and
    /// start a fresh generation. Idempotent, so a crash mid-destroy is healed
    /// by the next open — the header state stays `STATE_DESTROYING` until the
    /// reset completes.
    fn reset_common(&self, id0: u64, id1: u64) -> Result<()> {
        let header = self.map.header();
        header.state.store(STATE_DESTROYING, Ordering::Release);

        // Every registration dies with the reset (generations zero below makes
        // each live Drop-CAS fail); remake the FIFOs so fresh registrations
        // get clean pipes instead of stale bytes.
        notify::reset_pipes(&self.path)?;

        self.map
            .zero_range(HEADER_BYTES as u64, JOURNAL_BYTES as u64)?;
        self.map
            .zero_range(self.layout.off_freelist, (FREE_CLASSES * 8) as u64)?;
        // Both table regions are reserved at the max stride; zero them whole.
        let table_bytes =
            self.layout.max_index_entries * std::mem::size_of::<IndexSlot>() as u64;
        self.map.zero_range(self.layout.off_index[0], table_bytes)?;
        self.map.zero_range(self.layout.off_index[1], table_bytes)?;
        let ring_bytes =
            self.layout.change_retention * std::mem::size_of::<ChangeRingSlot>() as u64;
        self.map.zero_range(self.layout.off_ring, ring_bytes)?;

        header.magic.store(MAGIC, Ordering::Relaxed);
        header
            .format_version
            .store(FORMAT_VERSION, Ordering::Relaxed);
        header
            .layout_bytes
            .store(self.layout.file_len, Ordering::Relaxed);
        header
            .header_bytes
            .store(HEADER_BYTES as u64, Ordering::Relaxed);
        header.store_id0.store(id0, Ordering::Relaxed);
        header.store_id1.store(id1, Ordering::Relaxed);
        header
            .arena_capacity
            .store(self.layout.arena_capacity, Ordering::Relaxed);
        header
            .index_entries
            .store(self.layout.index_entries, Ordering::Relaxed);
        header
            .max_index_entries
            .store(self.layout.max_index_entries, Ordering::Relaxed);
        header
            .index_entries_target
            .store(self.layout.index_entries, Ordering::Relaxed);
        header.rehash_cursor.store(REHASH_IDLE, Ordering::Relaxed);
        for slot in &header.notify {
            slot.pid.store(0, Ordering::Relaxed);
            slot.generation.store(0, Ordering::Relaxed);
        }
        header.expiry_head.store(0, Ordering::Relaxed);
        header.expiry_count.store(0, Ordering::Relaxed);
        header.expiry_cycle_due_ms.store(0, Ordering::Relaxed);
        for count in &header.free_counts {
            count.store(0, Ordering::Relaxed);
        }
        header.live_payload_bytes.store(0, Ordering::Relaxed);
        header.expired_total.store(0, Ordering::Relaxed);
        header
            .change_retention
            .store(self.layout.change_retention, Ordering::Relaxed);
        header.arena_high_water.store(0, Ordering::Relaxed);
        header.live_entries.store(0, Ordering::Relaxed);
        header.tombstones.store(0, Ordering::Relaxed);
        header.change_revision.store(0, Ordering::Relaxed);
        header.next_operation_id.store(1, Ordering::Relaxed);
        header.index_generation.store(0, Ordering::Relaxed);
        header.active_index.store(0, Ordering::Relaxed);
        header
            .creation_unix_ms
            .store(now_unix_ms(), Ordering::Relaxed);
        let generation = header.store_generation.load(Ordering::Acquire) + 1;
        header.store_generation.store(generation, Ordering::Release);
        header.state.store(STATE_READY, Ordering::Release);
        Ok(())
    }

    // ----- locking ---------------------------------------------------------

    /// Non-blocking attempt to take the cross-process writer lock. Only valid
    /// while holding `local_write`.
    pub(crate) fn try_write_lock(&self) -> Result<bool> {
        match FileExt::try_lock_exclusive(&self.write_lock) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(Error::StoreConnection {
                message: format!("shm write lock probe failed: {error}"),
            }),
        }
    }

    /// Blocking take of the writer lock. Only valid inside `spawn_blocking`.
    pub(crate) fn block_write_lock(&self) -> Result<()> {
        FileExt::lock_exclusive(&self.write_lock).map_err(|error| Error::StoreConnection {
            message: format!("shm write lock failed: {error}"),
        })
    }

    pub(crate) fn unlock_write(&self) {
        let _ = FileExt::unlock(&self.write_lock);
    }

    // ----- reads (lock-free) ----------------------------------------------

    const READ_ATTEMPTS: usize = 64;

    /// Lock-free lookup of one record. Retries while writers make the slot
    /// dance underneath; gives up loudly rather than reporting a false miss.
    pub(crate) fn find_snapshot(
        &self,
        record_kind: u64,
        collection: &str,
        key: &str,
    ) -> Result<Option<EntrySnapshot>> {
        for _ in 0..Self::READ_ATTEMPTS {
            match self.find_snapshot_once(record_kind, collection, key)? {
                Found::Matched(snapshot) => return Ok(Some(snapshot)),
                Found::Absent => return Ok(None),
                Found::Unstable => continue,
            }
        }
        Err(Error::StoreFormat {
            message: "entry read kept colliding with writers".to_string(),
        })
    }

    fn find_snapshot_once(&self, record_kind: u64, collection: &str, key: &str) -> Result<Found> {
        let header = self.map.header();
        let layout = &self.layout;
        let generation0 = header.index_generation.load(Ordering::Acquire);
        let source = header.active_index.load(Ordering::Acquire) as usize;
        let rehashing = header.rehash_cursor.load(Ordering::Acquire) != REHASH_IDLE;
        let source_count = header.index_entries.load(Ordering::Acquire);
        let target_count = if rehashing {
            header.index_entries_target.load(Ordering::Acquire)
        } else {
            0
        };
        let hash = slot_hash(record_kind, collection, key);
        let step = (hash >> 33) | 1;

        // Dual probe while a rehash is in flight: keys not yet moved live in
        // the source, everything else (and all new inserts) in the target.
        // A generation bump (rehash completion) invalidates the whole read
        // via the revalidation below; mid-rehash moves keep the generation
        // stable, so readers no longer restart on every mutation.
        for (table, count) in [
            (source, source_count),
            (1 - source, target_count),
        ] {
            if count == 0 {
                continue;
            }
            let mut slot_index = hash & (count - 1);
            for _ in 0..count {
                let slot = self.map.index_slot(table, slot_index, layout)?;
                let offset0 = slot.entry_offset.load(Ordering::Acquire);
                if offset0 == SLOT_EMPTY {
                    // Not in this table; a rehash may hold it in the other.
                    break;
                }
                if offset0 != SLOT_TOMBSTONE {
                    if let Some(snapshot) = entry::snapshot_once(&self.map, offset0)? {
                        if snapshot.record_kind == record_kind
                            && snapshot.collection == collection
                            && snapshot.key == key
                        {
                            let sequence0 = slot.sequence.load(Ordering::Acquire);
                            // Re-verify: the slot or table may have moved under us.
                            let offset1 = slot.entry_offset.load(Ordering::Acquire);
                            let sequence1 = slot.sequence.load(Ordering::Acquire);
                            let generation1 = header.index_generation.load(Ordering::Acquire);
                            if offset1 != offset0
                                || sequence1 != sequence0
                                || generation1 != generation0
                            {
                                return Ok(Found::Unstable);
                            }
                            if snapshot.is_expired(now_unix_ms()) {
                                // Expired entries stay in place until a writer
                                // removes them; readers report absence (memory
                                // parity).
                                return Ok(Found::Absent);
                            }
                            return Ok(Found::Matched(snapshot));
                        }
                    }
                }
                slot_index = (slot_index + step) & (count - 1);
            }
        }
        Ok(Found::Absent)
    }

    /// Weakly consistent scan proxy for enumeration paths.
    pub(crate) fn scan_hits(
        &self,
        record_kind: Option<u64>,
        collection: Option<&str>,
        expired_only: bool,
    ) -> Result<Vec<index::ScanHit>> {
        index::scan(
            &self.map,
            &self.layout,
            record_kind,
            collection,
            expired_only,
            now_unix_ms(),
        )
    }

    // ----- mutations (writer lock held) ------------------------------------

    fn next_operation_id(&self) -> u64 {
        self.map
            .header()
            .next_operation_id
            .fetch_add(1, Ordering::Relaxed)
    }

    fn ensure_registry(&self, collection: &str) -> Result<()> {
        for _ in 0..2 {
            match index::locate(&self.map, &self.layout, RECORD_REGISTRY, collection, "")? {
                Locate::Live { .. } => return Ok(()),
                Locate::Vacant { table, index } => {
                    let payload = entry::payload_len(collection, "", 0);
                    let handle = arena::alloc(&self.map, &self.layout, payload)?;
                    entry::write_entry_block(
                        &self.map,
                        &handle,
                        entry::EntryBlockFields {
                            record_kind: RECORD_REGISTRY,
                            value_kind: 0,
                            created_at_ms: now_unix_ms(),
                            expires_at_ms: 0,
                            revision: &[0u8; 16],
                            collection,
                            key: "",
                            value: &[],
                        },
                    )?;
                    index::publish_slot(&self.map, &self.layout, table, index, handle.offset)?;
                    let header = self.map.header();
                    header.live_entries.fetch_add(1, Ordering::Relaxed);
                    header
                        .live_payload_bytes
                        .fetch_add(payload as u64, Ordering::Relaxed);
                    return Ok(());
                }
                Locate::Full => {
                    index::maybe_start_rehash(&self.map, &self.layout)?;
                    index::rehash_step(&self.map, &self.layout)?;
                }
            }
        }
        Err(Error::StoreCapacity {
            requested: 1,
            available: 0,
        })
    }

    /// Upsert with an optional CAS requirement. Writer lock held.
    pub(crate) fn perform_upsert(
        &self,
        collection: &str,
        key: &str,
        value: &Value,
        ttl_secs: Option<f64>,
        require: Require,
    ) -> Result<UpsertOutcome> {
        self.ensure_registry(collection)?;
        let revision = Revision::fresh()?;
        let created_ms = now_unix_ms();
        let expires_ms = match ttl_secs {
            Some(secs) => created_ms + (secs * 1000.0) as u64,
            None => 0,
        };

        for _ in 0..2 {
            index::maybe_start_rehash(&self.map, &self.layout)?;
            index::rehash_step(&self.map, &self.layout)?;
            match index::locate(&self.map, &self.layout, RECORD_ENTRY, collection, key)? {
                Locate::Live {
                    table,
                    index,
                    block,
                    snapshot,
                } => {
                    let expired = snapshot.is_expired(now_unix_ms());
                    match (&require, expired) {
                        (Require::ExactRevision(expected), false)
                            if expected != &snapshot.revision =>
                        {
                            return Ok(UpsertOutcome::Conflict {
                                current: Some(snapshot),
                            });
                        }
                        (Require::ExactRevision(_), true) => {
                            // Expired entries are absent for CAS purposes.
                            // The corpse stays until the expiry sweep culls
                            // it with an Expired change event — memory keeps
                            // the corpse too, and expiry is a change.
                            return Ok(UpsertOutcome::Conflict { current: None });
                        }
                        (Require::Absent, false) => {
                            return Ok(UpsertOutcome::Conflict {
                                current: Some(snapshot),
                            });
                        }
                        _ => {}
                    }
                    // An update that hit the source mid-rehash relocates the
                    // slot to the target first (redis: all writes land in
                    // ht[1]); then the COW update proceeds normally.
                    let header = self.map.header();
                    let slot = if header.rehash_cursor.load(Ordering::Acquire) != REHASH_IDLE
                        && header.active_index.load(Ordering::Acquire) as usize == table
                    {
                        index::migrate_slot(
                            &self.map,
                            &self.layout,
                            RECORD_ENTRY,
                            collection,
                            key,
                            table,
                            index,
                            block,
                        )?
                    } else {
                        (table, index)
                    };
                    return self.commit_upsert(
                        collection,
                        key,
                        value,
                        revision.as_bytes(),
                        created_ms,
                        expires_ms,
                        slot,
                        block,
                        true,
                    );
                }
                Locate::Vacant { table, index } => {
                    if matches!(require, Require::ExactRevision(_)) {
                        return Ok(UpsertOutcome::Conflict { current: None });
                    }
                    // True exhaustion only: load pressure alone just triggers
                    // an expand via maybe_start_rehash above. The error is
                    // numeric-only (requested/available); stats summaries
                    // belong to the stats API.
                    let header = self.map.header();
                    let live = header.live_entries.load(Ordering::Relaxed);
                    let max = header.max_index_entries.load(Ordering::Relaxed);
                    if live + 1 > max * 9 / 10 {
                        return Err(Error::StoreCapacity {
                            requested: 1,
                            available: 0,
                        });
                    }
                    return self.commit_upsert(
                        collection,
                        key,
                        value,
                        revision.as_bytes(),
                        created_ms,
                        expires_ms,
                        (table, index),
                        0,
                        true,
                    );
                }
                Locate::Full => {
                    index::maybe_start_rehash(&self.map, &self.layout)?;
                    index::rehash_step(&self.map, &self.layout)?;
                }
            }
        }
        Err(Error::StoreCapacity {
            requested: 1,
            available: 0,
        })
    }

    /// One incremental compaction pass: rewrite entries parked in blocks
    /// larger than their payload's minimal class, freeing the oversized
    /// blocks for reuse. Stops after roughly `budget_bytes` of freed block
    /// bytes. Writer lock held. Rewrite = COW into the minimal class with the
    /// original revision and timestamps and NO change event — compaction is
    /// physical maintenance, not a store mutation.
    pub(crate) fn compact_pass(&self, budget_bytes: u64) -> Result<u64> {
        let mut rewritten = 0u64;
        for hit in self.scan_hits(Some(RECORD_ENTRY), None, false)? {
            if rewritten >= budget_bytes {
                break;
            }
            // Fresh snapshot per entry: the scan above is weakly consistent.
            let Some(snapshot) = self.find_snapshot(RECORD_ENTRY, &hit.collection, &hit.key)?
            else {
                continue;
            };
            if snapshot.is_expired(now_unix_ms()) {
                // The expiry sweep owns expired entries.
                continue;
            }
            let payload =
                entry::payload_len(&snapshot.collection, &snapshot.key, snapshot.value.len());
            let minimal_class = arena::minimal_class_for_payload(payload);
            match index::locate(
                &self.map,
                &self.layout,
                RECORD_ENTRY,
                &snapshot.collection,
                &snapshot.key,
            )? {
                index::Locate::Live {
                    table,
                    index,
                    block,
                    ..
                } => {
                    let current_class =
                        self.map.entry_block(block)?.size_class.load(Ordering::Relaxed) as usize;
                    if current_class <= minimal_class {
                        continue;
                    }
                    let value = Value::new_unchecked(
                        ValueKind::from_tag(snapshot.value_kind as u8).ok_or_else(|| {
                            Error::StoreFormat {
                                message: format!(
                                    "entry holds unknown value kind tag {}",
                                    snapshot.value_kind
                                ),
                            }
                        })?,
                        snapshot.value.clone(),
                    );
                    let old_bytes = arena::class_bytes(current_class) as u64;
                    // commit_upsert frees the oversized block via the normal
                    // journal path; the rewrite is silent and revision-stable.
                    self.commit_upsert(
                        &snapshot.collection,
                        &snapshot.key,
                        &value,
                        &snapshot.revision,
                        snapshot.created_at_ms,
                        snapshot.expires_at_ms,
                        (table, index),
                        block,
                        false,
                    )?;
                    rewritten += old_bytes;
                }
                index::Locate::Vacant { .. } | index::Locate::Full => {
                    // Vanished between scan and rewrite; nothing to compact.
                }
            }
        }
        Ok(rewritten)
    }

    /// The journaled write pipeline: publish a fresh block, flip the slot,
    /// record the change, then free the replaced block. Writer lock held.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn commit_upsert(
        &self,
        collection: &str,
        key: &str,
        value: &Value,
        revision: &[u8; 16],
        created_ms: u64,
        expires_ms: u64,
        target: (usize, u64),
        old_block: u64,
        emit: bool,
    ) -> Result<UpsertOutcome> {
        let operation_id = self.next_operation_id();
        let payload = entry::payload_len(collection, key, value.len());
        let handle = arena::alloc(&self.map, &self.layout, payload)?;
        entry::write_entry_block(
            &self.map,
            &handle,
            entry::EntryBlockFields {
                record_kind: RECORD_ENTRY,
                value_kind: u64::from(value.kind().tag()),
                created_at_ms: created_ms,
                expires_at_ms: expires_ms,
                revision,
                collection,
                key,
                value: value.bytes(),
            },
        )?;

        self.journal_prepare(
            KIND_UPSERT,
            operation_id,
            old_block,
            handle.offset,
            target.0,
            target.1,
        )?;

        if !emit {
            // Silent physical rewrite (compaction): no change event, and the
            // journal marks the record event-free so crash recovery replays
            // it silently too.
            self.map
                .journal()
                .emit_change
                .store(0, Ordering::Relaxed);
        }
        let slot = self.map.index_slot(target.0, target.1, &self.layout)?;
        let header = self.map.header();
        if old_block == 0 {
            header.live_entries.fetch_add(1, Ordering::Relaxed);
            header
                .live_payload_bytes
                .fetch_add(payload as u64, Ordering::Relaxed);
        } else {
            header
                .live_payload_bytes
                .fetch_sub(entry::block_payload_len(&self.map, old_block)? as u64, Ordering::Relaxed);
            header
                .live_payload_bytes
                .fetch_add(payload as u64, Ordering::Relaxed);
        }
        // TTL entries join the intrusive expiry chain (singly-linked,
        // insert-at-head) inside the journaled window, before the slot flip:
        // a crash leaves at most a stale node, which the sweep's identity
        // check drops lazily. There is no unlink anywhere — an overwritten
        // or removed entry's node simply fails validation on the next walk.
        if expires_ms != 0 {
            let header = self.map.header();
            let block_header = self.map.entry_block(handle.offset)?;
            block_header.reserved[0].store(
                header.expiry_head.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            header.expiry_head.store(handle.offset, Ordering::Relaxed);
            header.expiry_count.fetch_add(1, Ordering::Relaxed);
            // Wake the sweepers: a due of 0 means "run on the next tick".
            header.expiry_cycle_due_ms.store(0, Ordering::Relaxed);
        }
        // Release, not Relaxed: a reader that Acquire-loads this offset must
        // also see the entry block published above (sequence = 2). On a weakly
        // ordered CPU a Relaxed store can become visible before the block
        // publish, and the reader would treat the unpublished block as "no
        // snapshot", probe past the slot and report Absent for a key that
        // exists at every linearization point of the overlapping overwrite.
        slot.entry_offset.store(handle.offset, Ordering::Release);
        slot.sequence.fetch_add(1, Ordering::Release);
        abort_hook("published");
        self.journal_set(JOURNAL_PUBLISHED);

        if emit && self.layout.change_retention > 0 {
            change::append_event(
                &self.map,
                &self.layout,
                OP_PUT,
                ChangeCause::Explicit,
                collection,
                key,
                operation_id,
            )?;
        }
        abort_hook("appended");
        self.journal_set(JOURNAL_RECORDED);

        self.journal_free_old(old_block)?;
        Ok(UpsertOutcome::Applied {
            revision: *revision,
        })
    }

    /// Delete (or cull) a record with an optional CAS requirement. Writer
    /// lock held. `cull` only changes the journal kind; registry records
    /// (`record_kind = RECORD_REGISTRY`) are internal bookkeeping and never
    /// emit change events.
    pub(crate) fn perform_remove(
        &self,
        record_kind: u64,
        collection: &str,
        key: &str,
        require: Option<&Revision>,
        cull: bool,
    ) -> Result<RemoveOutcome> {
        for _ in 0..2 {
            index::maybe_start_rehash(&self.map, &self.layout)?;
            index::rehash_step(&self.map, &self.layout)?;
            match index::locate(&self.map, &self.layout, record_kind, collection, key)? {
                Locate::Live {
                    table,
                    index,
                    block,
                    snapshot,
                } => {
                    if snapshot.is_expired(now_unix_ms()) {
                        if require.is_some() {
                            // Mirror memory: report absence regardless of the
                            // expectation; the sweep reclaims the corpse with
                            // an Expired change event.
                            return Ok(RemoveOutcome::Conflict { current: None });
                        }
                    } else if let Some(expected) = require {
                        if expected.as_bytes() != &snapshot.revision {
                            return Ok(RemoveOutcome::Conflict {
                                current: Some(snapshot),
                            });
                        }
                    }
                    return self.commit_remove(
                        record_kind,
                        collection,
                        key,
                        block,
                        table,
                        index,
                        cull,
                    );
                }
                Locate::Vacant { .. } => return Ok(RemoveOutcome::Absent),
                Locate::Full => {
                    index::maybe_start_rehash(&self.map, &self.layout)?;
                    index::rehash_step(&self.map, &self.layout)?;
                }
            }
        }
        Err(Error::StoreCapacity {
            requested: 1,
            available: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_remove(
        &self,
        record_kind: u64,
        collection: &str,
        key: &str,
        old_block: u64,
        table: usize,
        index: u64,
        cull: bool,
    ) -> Result<RemoveOutcome> {
        let operation_id = self.next_operation_id();
        let kind = if cull { KIND_CULL } else { KIND_REMOVE };
        self.journal_prepare(kind, operation_id, old_block, 0, table, index)?;

        let old_payload = entry::block_payload_len(&self.map, old_block)?;
        let slot = self.map.index_slot(table, index, &self.layout)?;
        slot.entry_offset.store(SLOT_TOMBSTONE, Ordering::Relaxed);
        slot.sequence.fetch_add(1, Ordering::Release);
        let header = self.map.header();
        header.live_entries.fetch_sub(1, Ordering::Relaxed);
        header.tombstones.fetch_add(1, Ordering::Relaxed);
        header
            .live_payload_bytes
            .fetch_sub(old_payload as u64, Ordering::Relaxed);
        if cull {
            header.expired_total.fetch_add(1, Ordering::Relaxed);
        }
        abort_hook("published");
        self.journal_set(JOURNAL_PUBLISHED);

        if self.layout.change_retention > 0 && record_kind == RECORD_ENTRY {
            // Culls are store-driven removals (capacity or TTL sweep) and
            // carry the Expired cause; explicit deletes stay Explicit.
            let cause = if cull {
                ChangeCause::Expired
            } else {
                ChangeCause::Explicit
            };
            change::append_event(
                &self.map,
                &self.layout,
                OP_DELETE,
                cause,
                collection,
                key,
                operation_id,
            )?;
        }
        abort_hook("appended");
        self.journal_set(JOURNAL_RECORDED);

        self.journal_free_old(old_block)?;
        Ok(RemoveOutcome::Removed)
    }

    // ----- expiry sweep ----------------------------------------------------

    /// Run one bounded expiry cycle over the intrusive TTL chain. Writer lock
    /// held. A node counts only while its block still hash-locates back to
    /// it, so overwritten or removed entries drop out lazily and block reuse
    /// self-corrects the chain — there is no unlink anywhere on the write
    /// path. Expired nodes are culled through the normal journaled remove,
    /// which emits the `Expired` change event; the rebuilt chain stays in
    /// chain order and the un-walked tail is relinked, so nothing is ever
    /// lost, only deferred to a later cycle.
    pub(crate) fn expire_slice(&self) -> Result<()> {
        let header = self.map.header();
        if header.state.load(Ordering::Acquire) != STATE_READY {
            return Ok(());
        }
        let now = now_unix_ms();
        if now < header.expiry_cycle_due_ms.load(Ordering::Acquire) {
            return Ok(());
        }

        const REMOVE_BUDGET: u64 = 256;
        const WALK_CAP: u64 = 4096;
        const CYCLE_MS: u64 = 50;

        let total = header.expiry_count.load(Ordering::Acquire);
        let mut removed = 0u64;
        let mut walked = 0u64;
        let mut kept = 0u64;
        let mut new_head = 0u64;
        let mut last_kept = 0u64;
        let mut cursor = header.expiry_head.load(Ordering::Acquire);

        while cursor != 0 && removed < REMOVE_BUDGET && walked < WALK_CAP.min(total.max(1)) {
            walked += 1;
            let next = self
                .map
                .entry_block(cursor)?
                .reserved[0]
                .load(Ordering::Relaxed);
            // Validate the node: the block must be a live TTL entry the index
            // still points at; anything else is a stale leftover.
            let validated = match entry::snapshot_once(&self.map, cursor)? {
                Some(snapshot)
                    if snapshot.record_kind == RECORD_ENTRY && snapshot.expires_at_ms != 0 =>
                {
                    match index::locate(
                        &self.map,
                        &self.layout,
                        RECORD_ENTRY,
                        &snapshot.collection,
                        &snapshot.key,
                    )? {
                        Locate::Live { block, .. } if block == cursor => Some(snapshot),
                        _ => None,
                    }
                }
                _ => None,
            };
            match validated {
                Some(snapshot) if snapshot.expires_at_ms <= now => {
                    self.perform_remove(
                        RECORD_ENTRY,
                        &snapshot.collection,
                        &snapshot.key,
                        None,
                        true,
                    )?;
                    removed += 1;
                }
                Some(_) => {
                    if last_kept != 0 {
                        self.map
                            .entry_block(last_kept)?
                            .reserved[0]
                            .store(cursor, Ordering::Relaxed);
                    } else {
                        new_head = cursor;
                    }
                    last_kept = cursor;
                    kept += 1;
                }
                None => {}
            }
            cursor = next;
        }
        // Relink the un-walked tail so remaining nodes stay reachable.
        if last_kept != 0 {
            self.map
                .entry_block(last_kept)?
                .reserved[0]
                .store(cursor, Ordering::Relaxed);
        } else {
            new_head = cursor;
        }

        let remaining = kept + (total.saturating_sub(walked));
        header.expiry_head.store(new_head, Ordering::Relaxed);
        header.expiry_count.store(remaining, Ordering::Relaxed);
        // Idle stores park the sweepers entirely; a fresh TTL put re-arms by
        // storing a due of 0.
        header.expiry_cycle_due_ms.store(
            if remaining == 0 {
                u64::MAX
            } else {
                now + CYCLE_MS
            },
            Ordering::Release,
        );
        Ok(())
    }

    /// Dedicated sweeper thread: ticks twice per cycle, checks the due stamp
    /// lock-free, and runs [`ShmClient::expire_slice`] under the writer lock
    /// when due. Every process holding the store sweeps; flock serializes
    /// them, and idle stores cost only the due read. A plain OS thread, not
    /// a tokio task — stores are created from sync contexts too.
    ///
    /// The sweeper opens its *own* `.write.lock` file description. flock on
    /// the same description converts instead of blocking (one `open()` shared
    /// with the mutate paths would let the sweep interleave with writers);
    /// a second description on the same file genuinely conflicts, same
    /// process included.
    pub(crate) fn spawn_expiry_sweeper(
        client: &Arc<ShmClient>,
    ) -> Result<ExpirySweeper> {
        let sweep_lock = open_lock_file(&write_lock_path(client.path()))?;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let client = Arc::clone(client);
        let worker = std::thread::Builder::new()
            .name("openkeyv-shm-expiry".to_owned())
            .spawn(move || {
                while !flag.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(25));
                    let header = client.map.header();
                    if header.state.load(Ordering::Acquire) != STATE_READY {
                        continue;
                    }
                    if change::now_unix_ms()
                        < header.expiry_cycle_due_ms.load(Ordering::Acquire)
                    {
                        continue;
                    }
                    if FileExt::lock_exclusive(&sweep_lock).is_err() {
                        continue;
                    }
                    let _ = client.expire_slice();
                    let _ = FileExt::unlock(&sweep_lock);
                }
            })
            .expect("expiry sweeper thread must spawn");
        Ok(ExpirySweeper {
            stop: Some(stop),
            _worker: Some(worker),
        })
    }

    // ----- journal ---------------------------------------------------------

    /// Write the prepared record. Field stores precede the state store so a
    /// crash before `JOURNAL_PREPARED` leaves `Idle` (and at most a leaked
    /// fresh block) rather than a record with garbage fields.
    fn journal_prepare(
        &self,
        kind: u64,
        operation_id: u64,
        old_block: u64,
        new_block: u64,
        table: usize,
        index: u64,
    ) -> Result<()> {
        let journal: &JournalRecord = self.map.journal();
        journal.operation_id.store(operation_id, Ordering::Relaxed);
        journal.kind.store(kind, Ordering::Relaxed);
        journal.slot_table.store(table as u64, Ordering::Relaxed);
        journal.slot_index.store(index, Ordering::Relaxed);
        journal.old_block.store(old_block, Ordering::Relaxed);
        journal.new_block.store(new_block, Ordering::Relaxed);
        journal.freed.store(0, Ordering::Relaxed);
        journal.emit_change.store(
            u64::from(self.layout.change_retention > 0),
            Ordering::Relaxed,
        );
        journal.state.store(JOURNAL_PREPARED, Ordering::Release);
        abort_hook("prepared");
        Ok(())
    }

    fn journal_set(&self, state: u64) {
        self.map.journal().state.store(state, Ordering::Release);
    }

    /// Mark `old_block` freed in the journal *before* freeing it, then idle the
    /// journal. A crash between the two merely leaks the block; a double free
    /// is impossible.
    fn journal_free_old(&self, old_block: u64) -> Result<()> {
        let journal: &JournalRecord = self.map.journal();
        journal.freed.store(2, Ordering::Relaxed);
        if old_block != 0 {
            arena::free(&self.map, &self.layout, old_block)?;
        }
        journal.state.store(JOURNAL_IDLE, Ordering::Release);
        Ok(())
    }

    /// Replay a half-finished mutation after a crash. Writer lock held.
    pub(crate) fn recover(&self) -> Result<()> {
        if self.map.header().state.load(Ordering::Acquire) == STATE_DESTROYING {
            // A destroy (or create-reset) crashed halfway through the full
            // reset; redo it. Idempotent by construction.
            return self.reset_excluding_id();
        }

        let journal: &JournalRecord = self.map.journal();
        let state = journal.state.load(Ordering::Acquire);
        if state == JOURNAL_IDLE {
            return Ok(());
        }

        let kind = journal.kind.load(Ordering::Acquire);
        let operation_id = journal.operation_id.load(Ordering::Acquire);
        let slot_table = journal.slot_table.load(Ordering::Acquire) as usize;
        let slot_index = journal.slot_index.load(Ordering::Acquire);
        let old_block = journal.old_block.load(Ordering::Acquire);
        let new_block = journal.new_block.load(Ordering::Acquire);

        let slot = self.map.index_slot(slot_table, slot_index, &self.layout)?;
        let current = slot.entry_offset.load(Ordering::Acquire);
        let published = match kind {
            KIND_UPSERT => new_block != 0 && current == new_block,
            KIND_REMOVE | KIND_CULL => current == SLOT_TOMBSTONE,
            other => {
                return Err(Error::StoreFormat {
                    message: format!("journal carries unknown kind {other}"),
                });
            }
        };

        if published {
            // The mutation landed. Guarantee its change event exists exactly
            // once, then free the replaced block.
            if journal.emit_change.load(Ordering::Acquire) == 1
                && !change::ring_contains_operation(&self.map, &self.layout, operation_id)?
            {
                let source = if kind == KIND_UPSERT {
                    new_block
                } else {
                    old_block
                };
                let snapshot =
                    entry::snapshot_once(&self.map, source)?.ok_or_else(|| Error::StoreFormat {
                        message: "recovery could not read the journaled block".to_string(),
                    })?;
                let operation = if kind == KIND_UPSERT {
                    OP_PUT
                } else {
                    OP_DELETE
                };
                // The cause is derivable from the journaled kind: culls are
                // store-driven removals, everything else is explicit.
                let cause = if kind == KIND_CULL {
                    ChangeCause::Expired
                } else {
                    ChangeCause::Explicit
                };
                change::append_event(
                    &self.map,
                    &self.layout,
                    operation,
                    cause,
                    &snapshot.collection,
                    &snapshot.key,
                    operation_id,
                )?;
                // A subscriber attached to an earlier generation may already
                // be idle-waiting on the ring; wake it for the re-append.
                notify::notify_all(self);
            }
            if journal.freed.load(Ordering::Acquire) == 0 {
                journal.freed.store(2, Ordering::Relaxed);
                if old_block != 0 {
                    arena::free(&self.map, &self.layout, old_block)?;
                }
            }
        } else {
            // The mutation never became visible; drop the fresh block.
            if journal.freed.load(Ordering::Acquire) == 0 {
                journal.freed.store(1, Ordering::Relaxed);
                if new_block != 0 {
                    arena::free(&self.map, &self.layout, new_block)?;
                }
            }
        }
        journal.state.store(JOURNAL_IDLE, Ordering::Release);
        Ok(())
    }

    // ----- subscriptions ---------------------------------------------------

    /// Resolve a subscription start point. Writer lock held so the returned
    /// revision has no gap to the next mutation.
    pub(crate) fn subscribe_point(&self, start: &ChangeStart) -> Result<(u64, u64)> {
        if self.layout.change_retention == 0 {
            return Err(Error::InvalidOperation(
                "this shm store was created with change_retention = 0 and has no change feed"
                    .to_string(),
            ));
        }
        let header = self.map.header();
        let count = header.change_revision.load(Ordering::Acquire);
        let generation = header.store_generation.load(Ordering::Acquire);
        let next = match start {
            ChangeStart::Latest => count + 1,
            ChangeStart::Beginning => count.saturating_sub(self.layout.change_retention) + 1,
            ChangeStart::After(cursor) => {
                let requested = cursor
                    .as_str()
                    .parse::<u64>()
                    .map_err(|_| Error::InvalidChangeCursor(cursor.to_string()))?;
                if requested > count {
                    return Err(Error::InvalidChangeCursor(cursor.to_string()));
                }
                if count > 0 {
                    let oldest = count.saturating_sub(self.layout.change_retention) + 1;
                    if requested < oldest.saturating_sub(1) {
                        return Err(Error::ChangeCursorExpired {
                            requested: cursor.to_string(),
                            oldest: oldest.to_string(),
                        });
                    }
                }
                requested + 1
            }
        };
        Ok((next, generation))
    }

    // ----- destruction -----------------------------------------------------

    /// Destroy the store: reset all data, bump the generation so every live
    /// change stream ends. Crash-safe via the `STATE_DESTROYING` flag.
    pub(crate) fn destroy(&self) -> Result<bool> {
        self.reset_excluding_id()?;
        Ok(true)
    }
}

enum Found {
    Matched(EntrySnapshot),
    Absent,
    Unstable,
}

/// CAS requirement for upserts.
#[derive(Clone, Debug)]
pub(crate) enum Require {
    /// Unconditional overwrite (plain `put`).
    Any,
    /// Create-if-absent; expired entries count as absent.
    Absent,
    /// Overwrite only when the live entry carries this exact revision.
    ExactRevision([u8; 16]),
}

#[derive(Debug)]
pub(crate) enum UpsertOutcome {
    Applied { revision: [u8; 16] },
    Conflict { current: Option<EntrySnapshot> },
}

#[derive(Debug)]
pub(crate) enum RemoveOutcome {
    Removed,
    Absent,
    Conflict { current: Option<EntrySnapshot> },
}

#[cfg(feature = "shm-test-hooks")]
fn abort_hook(stage: &str) {
    if std::env::var("OKV_SHM_ABORT_AT").as_deref() == Ok(stage) {
        eprintln!("openkeyv shm test hook: aborting at {stage}");
        std::process::abort();
    }
}

#[cfg(not(feature = "shm-test-hooks"))]
fn abort_hook(_stage: &str) {}

impl Drop for ShmClient {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.attach_lock);
        let _ = FileExt::unlock(&self.write_lock);
    }
}
