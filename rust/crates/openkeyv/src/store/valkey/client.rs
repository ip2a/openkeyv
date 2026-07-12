#[derive(Clone)]
pub struct ValkeyClient {
    conn: redis::aio::MultiplexedConnection,
}

impl ValkeyClient {
    pub fn new(conn: redis::aio::MultiplexedConnection) -> Self {
        Self { conn }
    }

    pub(crate) fn connection(&self) -> redis::aio::MultiplexedConnection {
        self.conn.clone()
    }
}
