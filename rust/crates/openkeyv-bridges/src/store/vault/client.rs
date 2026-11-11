pub struct VaultClient {
    client: vaultrs::client::VaultClient,
    mount_point: String,
}

impl VaultClient {
    pub fn new(client: vaultrs::client::VaultClient, mount_point: String) -> Self {
        Self {
            client,
            mount_point,
        }
    }

    pub(crate) fn client(&self) -> &vaultrs::client::VaultClient {
        &self.client
    }

    pub(crate) fn mount_point(&self) -> &str {
        &self.mount_point
    }
}
