pub struct RocksDBClient {
    db: rocksdb::DB,
}

impl RocksDBClient {
    pub fn new(db: rocksdb::DB) -> Self {
        Self { db }
    }

    pub(crate) fn db(&self) -> &rocksdb::DB {
        &self.db
    }
}
