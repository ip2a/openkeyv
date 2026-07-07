#[derive(Clone)]
pub struct DiskClient {
    db: sled::Db,
}

impl DiskClient {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    pub(crate) fn db(&self) -> &sled::Db {
        &self.db
    }
}
