pub struct DynamoDBClient {
    client: aws_sdk_dynamodb::Client,
}

impl DynamoDBClient {
    pub fn new(client: aws_sdk_dynamodb::Client) -> Self {
        Self { client }
    }

    pub(crate) fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}
