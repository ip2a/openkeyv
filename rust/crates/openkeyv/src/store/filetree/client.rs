use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileTreeClient {
    base_path: PathBuf,
}

impl FileTreeClient {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    pub(crate) fn base_path(&self) -> &Path {
        &self.base_path
    }
}
