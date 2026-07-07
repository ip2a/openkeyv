pub struct PostgresClient {
    pool: sqlx::PgPool,
}

impl PostgresClient {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}
