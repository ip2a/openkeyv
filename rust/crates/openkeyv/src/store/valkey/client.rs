#[derive(Clone)]
pub struct ValkeyClient {
    conn: redis::aio::ConnectionManager,
}

impl ValkeyClient {
    pub fn new(conn: redis::aio::ConnectionManager) -> Self {
        Self { conn }
    }

    pub(crate) fn connection(&self) -> redis::aio::ConnectionManager {
        self.conn.clone()
    }
}
