use std::collections::HashSet;
use tokio::sync::Mutex;

pub struct MongoClient {
    db: mongodb::Database,
    initialized_collections: Mutex<HashSet<String>>,
}

impl MongoClient {
    pub fn new(db: mongodb::Database) -> Self {
        Self {
            db,
            initialized_collections: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn db(&self) -> &mongodb::Database {
        &self.db
    }

    pub(crate) fn initialized_collections(&self) -> &Mutex<HashSet<String>> {
        &self.initialized_collections
    }
}
