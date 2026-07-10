#[derive(Clone)]
pub struct KeyringClient {
    service_name: String,
}

impl KeyringClient {
    pub fn new(service_name: String) -> Self {
        Self { service_name }
    }

    pub(crate) fn entry(&self, collection: &str, key: &str) -> keyring::Result<keyring::Entry> {
        let username = format!("{}:{collection}{key}", collection.len());
        keyring::Entry::new(&self.service_name, &username)
    }
}
