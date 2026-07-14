use super::client::FileTreeClient;
use super::config::FileTreeConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::fs;

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c => c,
        })
        .collect()
}

fn safe_path(base: &Path, collection: &str, key: &str) -> Result<PathBuf> {
    let collection = sanitize_filename(collection);
    let key = sanitize_filename(key);
    let path = base.join(&collection).join(&key);

    // Defense in depth: sanitize_filename already removes separators,
    // but we verify the joined path stays under base.
    if !path.starts_with(base) {
        return Err(Error::PathSecurity(format!(
            "path '{}' escapes base directory",
            path.display()
        )));
    }

    Ok(path)
}

fn collection_path(base: &Path, collection: &str) -> PathBuf {
    let collection = sanitize_filename(collection);
    base.join(&collection)
}

/// Async filesystem-based key-value store.
///
/// Each collection is a directory under `base_path`.
/// Each key is a file containing an `OKVE1`-encoded `ManagedEntry`.
pub struct FileTreeStore {
    client: FileTreeClient,
    config: FileTreeConfig,
}

impl FileTreeStore {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self::with_options(base_path, None)
    }

    pub fn with_options(base_path: impl Into<PathBuf>, default_collection: Option<String>) -> Self {
        Self::with_config(base_path, FileTreeConfig::new(default_collection))
    }

    pub fn with_config(base_path: impl Into<PathBuf>, config: FileTreeConfig) -> Self {
        Self {
            client: FileTreeClient::new(base_path),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn base_path(&self) -> &Path {
        self.client.base_path()
    }

    async fn ensure_collection_dir(&self, collection: &str) -> Result<()> {
        let path = collection_path(self.base_path(), collection);
        fs::create_dir_all(&path)
            .await
            .map_err(|e| Error::StoreSetup {
                message: format!("failed to create collection dir: {}", e),
            })?;
        Ok(())
    }

    async fn read_entry(&self, path: &Path) -> Result<Option<ManagedEntry>> {
        match fs::read(path).await {
            Ok(bytes) => Ok(Some(ManagedEntry::decode(Bytes::from(bytes))?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::StoreConnection {
                message: format!("failed to read file: {}", e),
            }),
        }
    }

    async fn write_entry(&self, path: &Path, entry: &ManagedEntry) -> Result<()> {
        fs::write(path, entry.encode())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to write file: {}", e),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for FileTreeStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let path = safe_path(self.base_path(), cname, key)?;
        match self.read_entry(&path).await? {
            Some(entry) if !entry.is_expired() => Ok(Some(entry.value)),
            _ => Ok(None),
        }
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let path = safe_path(self.base_path(), cname, key)?;
        match self.read_entry(&path).await? {
            Some(entry) if !entry.is_expired() => {
                let ttl = entry.ttl().unwrap_or(0.0);
                Ok(Some((entry.value, ttl)))
            }
            _ => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let path = safe_path(self.base_path(), cname, key)?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        self.write_entry(&path, &entry).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let path = safe_path(self.base_path(), cname, key)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::StoreConnection {
                message: format!("failed to delete file: {}", e),
            }),
        }
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let path = safe_path(self.base_path(), cname, key)?;
            match self.read_entry(&path).await? {
                Some(entry) if !entry.is_expired() => results.push(Some(entry.value)),
                _ => results.push(None),
            }
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let path = safe_path(self.base_path(), cname, key)?;
            match self.read_entry(&path).await? {
                Some(entry) if !entry.is_expired() => {
                    let ttl = entry.ttl().unwrap_or(0.0);
                    results.push(Some((entry.value, ttl)))
                }
                _ => results.push(None),
            }
        }
        Ok(results)
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        if keys.len() != values.len() {
            return Err(Error::BatchSizeMismatch {
                keys: keys.len(),
                values: values.len(),
            });
        }
        if let Some(seconds) = ttl {
            ManagedEntry::validate_ttl(seconds)?;
        }
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        for (key, value) in keys.iter().zip(values.iter()) {
            let path = safe_path(self.base_path(), cname, key)?;
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds)?,
                None => ManagedEntry::new(value.clone()),
            };
            self.write_entry(&path, &entry).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let mut count = 0;
        for key in keys {
            let path = safe_path(self.base_path(), cname, key)?;
            match fs::remove_file(&path).await {
                Ok(()) => count += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(Error::StoreConnection {
                        message: format!("failed to delete file: {}", e),
                    });
                }
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for FileTreeStore {
    async fn cull(&self) -> Result<()> {
        let mut entries =
            fs::read_dir(self.base_path())
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to read base dir: {}", e),
                })?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to read dir entry: {}", e),
            })?
        {
            let path = entry.path();
            if path.is_dir() {
                let mut files = fs::read_dir(&path)
                    .await
                    .map_err(|e| Error::StoreConnection {
                        message: format!("failed to read dir: {}", e),
                    })?;
                while let Some(file) =
                    files
                        .next_entry()
                        .await
                        .map_err(|e| Error::StoreConnection {
                            message: format!("failed to read dir entry: {}", e),
                        })?
                {
                    let file_path = file.path();
                    if let Some(content) = self.read_entry(&file_path).await? {
                        if content.is_expired() {
                            fs::remove_file(&file_path).await.map_err(|e| {
                                Error::StoreConnection {
                                    message: format!("failed to remove expired file: {}", e),
                                }
                            })?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for FileTreeStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let path = collection_path(self.base_path(), cname);
        let mut entries = fs::read_dir(&path)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to read dir: {}", e),
            })?;
        let mut keys = Vec::new();
        let limit = limit.unwrap_or(10_000).min(10_000);
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to read dir entry: {}", e),
            })?
        {
            if let Some(name) = entry.file_name().to_str() {
                keys.push(name.to_string());
            }
            if keys.len() >= limit {
                break;
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for FileTreeStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let mut entries =
            fs::read_dir(self.base_path())
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to read base dir: {}", e),
                })?;
        let mut collections = Vec::new();
        let limit = limit.unwrap_or(10_000).min(10_000);
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to read dir entry: {}", e),
            })?
        {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    collections.push(name.to_string());
                }
                if collections.len() >= limit {
                    break;
                }
            }
        }
        Ok(collections)
    }
}

#[async_trait]
impl AsyncDestroyCollection for FileTreeStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let path = collection_path(self.base_path(), collection);
        match fs::remove_dir_all(&path).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::StoreConnection {
                message: format!("failed to remove collection dir: {}", e),
            }),
        }
    }
}

#[async_trait]
impl AsyncDestroyStore for FileTreeStore {
    async fn destroy(&self) -> Result<bool> {
        match fs::remove_dir_all(self.base_path()).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(Error::StoreConnection {
                message: format!("failed to remove store dir: {}", e),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_filetree_store_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = Value::utf8("Alice");

        store.put("user1", value.clone(), None, None).await.unwrap();
        let got = store.get("user1", None).await.unwrap();
        assert_eq!(got, Some(value));

        let path = safe_path(store.base_path(), store.collection_name(None), "user1").unwrap();
        let bytes = fs::read(path).await.unwrap();
        assert!(bytes.starts_with(b"OKVE1"));
    }

    #[tokio::test]
    async fn test_filetree_store_rejects_json_entry_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let collection = store.collection_name(None);
        store.ensure_collection_dir(collection).await.unwrap();
        let path = safe_path(store.base_path(), collection, "legacy").unwrap();
        fs::write(path, br#"{"value":null}"#).await.unwrap();

        let err = store.get("legacy", None).await.unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[tokio::test]
    async fn test_filetree_cull_rejects_corrupt_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let collection = store.collection_name(None);
        store.ensure_collection_dir(collection).await.unwrap();
        let path = safe_path(store.base_path(), collection, "corrupt").unwrap();
        fs::write(path, b"corrupt").await.unwrap();

        let err = store.cull().await.unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[tokio::test]
    async fn test_filetree_store_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = Value::null();

        store.put("k", value, None, None).await.unwrap();
        assert!(store.delete("k", None).await.unwrap());
        assert!(!store.delete("k", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_filetree_store_collections() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = Value::null();

        store
            .put("k", value.clone(), Some("c1"), None)
            .await
            .unwrap();
        store.put("k", value, Some("c2"), None).await.unwrap();

        let cols = store.collections(None).await.unwrap();
        assert!(cols.contains(&"c1".to_string()));
        assert!(cols.contains(&"c2".to_string()));
    }

    #[tokio::test]
    async fn test_filetree_store_destroy_collection() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = Value::null();

        store.put("k", value, Some("c1"), None).await.unwrap();
        assert!(store.destroy_collection("c1").await.unwrap());
        assert!(!store.destroy_collection("c1").await.unwrap());
    }

    #[tokio::test]
    async fn test_filetree_store_path_security() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = Value::null();

        // Path traversal attempt should be sanitized or rejected
        store
            .put("../../../etc/passwd", value.clone(), None, None)
            .await
            .unwrap();
        // Should be stored under the sanitized name, not escape base
        let got = store.get("../../../etc/passwd", None).await.unwrap();
        assert_eq!(got, Some(value));
    }
}
