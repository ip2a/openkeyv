//! Shared-memory store backend (`shm` feature, Unix only).
//!
//! One mmap'd file holds the header, journal, hash index, change ring, and
//! block arena. Multiple processes attach to the same file: reads are
//! lock-free seqlock snapshots, writes serialize through a cross-process
//! flock, and a journal makes every mutation crash-atomic.

pub(crate) mod arena;
pub(crate) mod change;
pub(crate) mod client;
pub mod config;
mod entry;
pub(crate) mod index;
pub(crate) mod layout;
pub(crate) mod notify;
mod store;

pub use config::{ShmConfig, ShmCreateConfig, ShmOpenMode};
pub use store::ShmStore;
