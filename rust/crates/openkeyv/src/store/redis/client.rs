#[derive(Clone)]
pub struct RedisClient {
    conn: redis::aio::MultiplexedConnection,
}

impl RedisClient {
    pub fn new(conn: redis::aio::MultiplexedConnection) -> Self {
        Self { conn }
    }

    pub(crate) fn connection(&self) -> redis::aio::MultiplexedConnection {
        self.conn.clone()
    }
}
