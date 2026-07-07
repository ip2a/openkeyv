pub struct DuckDBClient {
    conn: tokio::sync::Mutex<duckdb::Connection>,
}

impl DuckDBClient {
    pub fn new(conn: duckdb::Connection) -> Self {
        Self {
            conn: tokio::sync::Mutex::new(conn),
        }
    }

    pub(crate) fn conn(&self) -> &tokio::sync::Mutex<duckdb::Connection> {
        &self.conn
    }
}
