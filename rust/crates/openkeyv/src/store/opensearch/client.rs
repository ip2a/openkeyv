pub struct OpenSearchClient {
    client: opensearch::OpenSearch,
}

impl OpenSearchClient {
    pub fn new(client: opensearch::OpenSearch) -> Self {
        Self { client }
    }

    pub(crate) fn client(&self) -> &opensearch::OpenSearch {
        &self.client
    }
}
