pub struct S3Client {
    client: aws_sdk_s3::Client,
}

impl S3Client {
    pub fn new(client: aws_sdk_s3::Client) -> Self {
        Self { client }
    }

    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        &self.client
    }
}
