use super::error::{Result, map_keyring_err};

pub struct KeyringClient {
    service_name: String,
}

impl KeyringClient {
    pub fn new(service_name: String) -> Self {
        Self { service_name }
    }

    pub(crate) fn entry(&self, collection: &str, key: &str) -> Result<keyring::Entry> {
        let username = compound_key(collection, key);
        keyring::Entry::new(&self.service_name, &username).map_err(map_keyring_err)
    }
}

fn compound_key(collection: &str, key: &str) -> String {
    format!("{}:{}", collection, key)
}
