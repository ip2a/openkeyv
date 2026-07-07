pub struct MongoClient {
    db: mongodb::Database,
}

impl MongoClient {
    pub fn new(db: mongodb::Database) -> Self {
        Self { db }
    }

    pub(crate) fn db(&self) -> &mongodb::Database {
        &self.db
    }
}
