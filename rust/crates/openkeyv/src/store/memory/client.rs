use crate::entry::ManagedEntry;
use dashmap::DashMap;
use tokio::sync::RwLock;

pub type MemoryCollections = DashMap<String, DashMap<String, ManagedEntry>>;

#[derive(Default)]
pub struct MemoryClient {
    collections: MemoryCollections,
    setup_complete: RwLock<bool>,
}

impl MemoryClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn collections(&self) -> &MemoryCollections {
        &self.collections
    }

    pub(crate) fn setup_complete(&self) -> &RwLock<bool> {
        &self.setup_complete
    }
}
