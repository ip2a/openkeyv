use crate::error::{Error, Result};
use redis::aio::{ConnectionManager, MultiplexedConnection};

#[derive(Clone)]
pub struct RedisClient {
    conn: ConnectionManager,
    client: Option<redis::Client>,
}

impl RedisClient {
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn, client: None }
    }

    pub(crate) fn with_client(conn: ConnectionManager, client: redis::Client) -> Self {
        Self {
            conn,
            client: Some(client),
        }
    }

    pub(crate) fn connection(&self) -> ConnectionManager {
        self.conn.clone()
    }

    pub(crate) async fn subscription_connection(&self) -> Result<MultiplexedConnection> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::InvalidOperation(
                "Redis ChangeFeed requires RedisStore::new or RedisStore::from_client".to_string(),
            )
        })?;
        client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|error| Error::StoreConnection {
                message: error.to_string(),
            })
    }
}
