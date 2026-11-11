pub struct FirestoreClient {
    db: firestore::FirestoreDb,
}

impl FirestoreClient {
    pub fn new(db: firestore::FirestoreDb) -> Self {
        Self { db }
    }

    pub(crate) fn db(&self) -> &firestore::FirestoreDb {
        &self.db
    }
}
