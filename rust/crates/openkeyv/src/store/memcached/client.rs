pub struct MemcachedClient {
    client: tokio::sync::Mutex<memcache::Client>,
}

impl MemcachedClient {
    pub fn new(client: memcache::Client) -> Self {
        Self {
            client: tokio::sync::Mutex::new(client),
        }
    }

    pub(crate) fn client(&self) -> &tokio::sync::Mutex<memcache::Client> {
        &self.client
    }
}
