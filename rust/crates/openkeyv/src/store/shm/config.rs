//! Configuration types for the SHM store backend.

use crate::error::{Error, Result};
use std::path::PathBuf;

pub(crate) const DEFAULT_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_INDEX_ENTRIES: u32 = 65_536;
pub(crate) const DEFAULT_CHANGE_RETENTION: u32 = 10_000;

const MIN_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;
const MIN_INDEX_ENTRIES: u32 = 1_024;
const MAX_CHANGE_RETENTION: u32 = 1 << 26;

/// Cross-process key/collection length limits. The SHM layout is fixed-size by
/// contract, so identifiers are bounded up front instead of failing at
/// allocation time deep inside a mutation.
pub(crate) const MAX_COLLECTION_BYTES: usize = 512;
pub(crate) const MAX_KEY_BYTES: usize = 1_024;

pub(crate) const DEFAULT_COLLECTION: &str = "default_collection";

/// How a store path should be opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShmOpenMode {
    /// Initialize (or reset) the backing file. Fails with
    /// [`Error::StoreInUse`] while any other process still has it attached.
    Create,
    /// Attach to an existing store. Fails if the file is missing or was
    /// written by an incompatible format.
    Open,
}

/// Parameters for creating a new backing file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShmCreateConfig {
    pub path: PathBuf,
    /// Total backing-file size, header/index/ring included.
    pub capacity_bytes: u64,
    /// Slots per hash table; must be a power of two.
    pub index_entries: u32,
    /// Change-feed ring capacity; 0 disables the change feed.
    pub change_retention: u32,
}

impl ShmCreateConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            capacity_bytes: DEFAULT_CAPACITY_BYTES,
            index_entries: DEFAULT_INDEX_ENTRIES,
            change_retention: DEFAULT_CHANGE_RETENTION,
        }
    }

    pub fn with_capacity_bytes(mut self, capacity_bytes: u64) -> Self {
        self.capacity_bytes = capacity_bytes;
        self
    }

    pub fn with_index_entries(mut self, index_entries: u32) -> Self {
        self.index_entries = index_entries;
        self
    }

    pub fn with_change_retention(mut self, change_retention: u32) -> Self {
        self.change_retention = change_retention;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.capacity_bytes < MIN_CAPACITY_BYTES {
            return Err(Error::StoreSetup {
                message: format!(
                    "shm capacity_bytes must be at least {MIN_CAPACITY_BYTES}, got {}",
                    self.capacity_bytes
                ),
            });
        }
        if self.index_entries < MIN_INDEX_ENTRIES || !self.index_entries.is_power_of_two() {
            return Err(Error::StoreSetup {
                message: format!(
                    "shm index_entries must be a power of two >= {MIN_INDEX_ENTRIES}, got {}",
                    self.index_entries
                ),
            });
        }
        if self.change_retention > MAX_CHANGE_RETENTION {
            return Err(Error::StoreSetup {
                message: format!(
                    "shm change_retention must be at most {MAX_CHANGE_RETENTION}, got {}",
                    self.change_retention
                ),
            });
        }
        Ok(())
    }
}

/// Full open description: path plus mode and (for create) layout hints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShmConfig {
    pub path: PathBuf,
    pub mode: ShmOpenMode,
    pub capacity_bytes: u64,
    pub index_entries: u32,
    pub change_retention: u32,
}

impl ShmConfig {
    pub fn create(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            mode: ShmOpenMode::Create,
            capacity_bytes: DEFAULT_CAPACITY_BYTES,
            index_entries: DEFAULT_INDEX_ENTRIES,
            change_retention: DEFAULT_CHANGE_RETENTION,
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            mode: ShmOpenMode::Open,
            capacity_bytes: DEFAULT_CAPACITY_BYTES,
            index_entries: DEFAULT_INDEX_ENTRIES,
            change_retention: DEFAULT_CHANGE_RETENTION,
        }
    }

    pub fn with_capacity_bytes(mut self, capacity_bytes: u64) -> Self {
        self.capacity_bytes = capacity_bytes;
        self
    }

    pub fn with_index_entries(mut self, index_entries: u32) -> Self {
        self.index_entries = index_entries;
        self
    }

    pub fn with_change_retention(mut self, change_retention: u32) -> Self {
        self.change_retention = change_retention;
        self
    }

    pub(crate) fn create_config(&self) -> ShmCreateConfig {
        ShmCreateConfig {
            path: self.path.clone(),
            capacity_bytes: self.capacity_bytes,
            index_entries: self.index_entries,
            change_retention: self.change_retention,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_small_or_unkempt_layouts() {
        let valid = ShmCreateConfig::new("/tmp/x");
        valid.validate().unwrap();

        let tiny = ShmCreateConfig::new("/tmp/x").with_capacity_bytes(1024);
        assert!(tiny.validate().is_err());

        let not_pow2 = ShmCreateConfig::new("/tmp/x").with_index_entries(1_000);
        assert!(not_pow2.validate().is_err());

        let too_small = ShmCreateConfig::new("/tmp/x").with_index_entries(512);
        assert!(too_small.validate().is_err());

        let huge_ring = ShmCreateConfig::new("/tmp/x").with_change_retention(u32::MAX);
        assert!(huge_ring.validate().is_err());
    }

    #[test]
    fn shm_config_defaults_match_create_config_defaults() {
        let create = ShmConfig::create("/tmp/x").create_config();
        assert_eq!(create, ShmCreateConfig::new("/tmp/x"));
        assert_eq!(ShmConfig::open("/tmp/x").mode, ShmOpenMode::Open);
    }
}
