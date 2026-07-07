#[derive(Clone, Copy, Debug, Default)]
pub struct NullClient;

impl NullClient {
    pub fn new() -> Self {
        Self
    }
}
