use crate::entry::ManagedEntry;
use std::collections::HashMap;
use tokio::sync::RwLock;

pub type SimpleData = HashMap<String, HashMap<String, ManagedEntry>>;

#[derive(Default)]
pub struct SimpleClient {
    data: RwLock<SimpleData>,
}

impl SimpleClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn data(&self) -> &RwLock<SimpleData> {
        &self.data
    }
}
