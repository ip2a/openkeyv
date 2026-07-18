pub struct SqliteClient {
    conn: tokio::sync::Mutex<rusqlite::Connection>,
}

impl SqliteClient {
    pub fn new(conn: rusqlite::Connection) -> Self {
        Self {
            conn: tokio::sync::Mutex::new(conn),
        }
    }

    pub(crate) fn conn(&self) -> &tokio::sync::Mutex<rusqlite::Connection> {
        &self.conn
    }
}
