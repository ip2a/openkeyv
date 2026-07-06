use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

const DEFAULT_COLLECTION: &str = "default_collection";

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
/// Each key is a file containing a JSON-serialized `ManagedEntry`.
pub struct FileTreeStore {
    base_path: PathBuf,
    default_collection: String,
}

impl FileTreeStore {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self::with_options(base_path, None)
    }

    pub fn with_options(base_path: impl Into<PathBuf>, default_collection: Option<String>) -> Self {
        Self {
            base_path: base_path.into(),
            default_collection: default_collection
                .unwrap_or_else(|| DEFAULT_COLLECTION.to_string()),
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    async fn ensure_collection_dir(&self, collection: &str) -> Result<()> {
        let path = collection_path(&self.base_path, collection);
        fs::create_dir_all(&path)
            .await
            .map_err(|e| Error::StoreSetup {
                message: format!("failed to create collection dir: {}", e),
            })?;
        Ok(())
    }

    async fn read_entry(&self, path: &Path) -> Result<Option<ManagedEntry>> {
        match fs::read_to_string(path).await {
            Ok(contents) => {
                let entry: ManagedEntry = serde_json::from_str(&contents)
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                Ok(Some(entry))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::StoreConnection {
                message: format!("failed to read file: {}", e),
            }),
        }
    }

    async fn write_entry(&self, path: &Path, entry: &ManagedEntry) -> Result<()> {
        let json =
            serde_json::to_string_pretty(entry).map_err(|e| Error::Serialization(e.to_string()))?;
        let mut file = fs::File::create(path)
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to create file: {}", e),
            })?;
        file.write_all(json.as_bytes())
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to write file: {}", e),
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKeyValue for FileTreeStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let path = safe_path(&self.base_path, cname, key)?;
        match self.read_entry(&path).await? {
            Some(entry) if !entry.is_expired() => Ok(Some(entry.value)),
            _ => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let path = safe_path(&self.base_path, cname, key)?;
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
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let path = safe_path(&self.base_path, cname, key)?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        self.write_entry(&path, &entry).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let path = safe_path(&self.base_path, cname, key)?;
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
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let path = safe_path(&self.base_path, cname, key)?;
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
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let path = safe_path(&self.base_path, cname, key)?;
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
        values: &[HashMap<String, Value>],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        if keys.len() != values.len() {
            return Err(Error::BatchSizeMismatch {
                keys: keys.len(),
                values: values.len(),
            });
        }
        let cname = self.collection_name(collection);
        self.ensure_collection_dir(cname).await?;
        for (key, value) in keys.iter().zip(values.iter()) {
            let path = safe_path(&self.base_path, cname, key)?;
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
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
            let path = safe_path(&self.base_path, cname, key)?;
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
            fs::read_dir(&self.base_path)
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
                            fs::remove_file(&file_path).await.ok();
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
        let path = collection_path(&self.base_path, cname);
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
            fs::read_dir(&self.base_path)
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
        let path = collection_path(&self.base_path, collection);
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
        match fs::remove_dir_all(&self.base_path).await {
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
        let mut value = HashMap::new();
        value.insert("name".to_string(), Value::String("Alice".to_string()));

        store.put("user1", value.clone(), None, None).await.unwrap();
        let got = store.get("user1", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_filetree_store_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = HashMap::new();

        store.put("k", value, None, None).await.unwrap();
        assert!(store.delete("k", None).await.unwrap());
        assert!(!store.delete("k", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_filetree_store_collections() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = HashMap::new();

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
        let value = HashMap::new();

        store.put("k", value, Some("c1"), None).await.unwrap();
        assert!(store.destroy_collection("c1").await.unwrap());
        assert!(!store.destroy_collection("c1").await.unwrap());
    }

    #[tokio::test]
    async fn test_filetree_store_path_security() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let value = HashMap::new();

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
