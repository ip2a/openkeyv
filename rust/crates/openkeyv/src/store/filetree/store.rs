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
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use tokio::fs;

const COMPONENT_PREFIX: &str = "okv1-";
const MAX_COMPONENT_BYTES: usize = 255;

#[cfg(windows)]
const MAX_PATH_UNITS: usize = 259;
#[cfg(target_os = "macos")]
const MAX_PATH_UNITS: usize = 1023;
#[cfg(all(unix, not(target_os = "macos")))]
const MAX_PATH_UNITS: usize = 4095;
#[cfg(not(any(unix, windows)))]
const MAX_PATH_UNITS: usize = 4095;

fn encode_component(value: &str, kind: &str) -> Result<String> {
    let encoded_len =
        COMPONENT_PREFIX
            .len()
            .checked_add(value.len().checked_mul(2).ok_or_else(|| {
                Error::InvalidKey(format!("FileTree {kind} is too large to encode"))
            })?)
            .ok_or_else(|| Error::InvalidKey(format!("FileTree {kind} is too large to encode")))?;
    if encoded_len > MAX_COMPONENT_BYTES {
        return Err(Error::InvalidKey(format!(
            "FileTree {kind} encodes to {encoded_len} bytes (max {MAX_COMPONENT_BYTES})"
        )));
    }

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(encoded_len);
    encoded.push_str(COMPONENT_PREFIX);
    for byte in value.as_bytes() {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

fn decode_component(component: &OsStr, kind: &str) -> Result<String> {
    let component = component
        .to_str()
        .ok_or_else(|| Error::InvalidKey(format!("FileTree physical {kind} is not valid UTF-8")))?;
    if component.len() > MAX_COMPONENT_BYTES {
        return Err(Error::InvalidKey(format!(
            "FileTree physical {kind} is {} bytes (max {MAX_COMPONENT_BYTES})",
            component.len()
        )));
    }
    let hex = component.strip_prefix(COMPONENT_PREFIX).ok_or_else(|| {
        Error::InvalidKey(format!(
            "FileTree physical {kind} is missing the {COMPONENT_PREFIX} prefix"
        ))
    })?;
    if hex.len() % 2 != 0 {
        return Err(Error::InvalidKey(format!(
            "FileTree physical {kind} has an odd hexadecimal length"
        )));
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(Error::InvalidKey(format!(
                "FileTree physical {kind} is not canonical lowercase hexadecimal"
            ))),
        };
        bytes.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }

    let decoded = String::from_utf8(bytes)
        .map_err(|_| Error::InvalidKey(format!("FileTree physical {kind} is not valid UTF-8")))?;
    if encode_component(&decoded, kind)? != component {
        return Err(Error::InvalidKey(format!(
            "FileTree physical {kind} is not canonical"
        )));
    }
    Ok(decoded)
}

#[cfg(windows)]
fn path_units(path: &Path) -> usize {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str().encode_wide().count()
}

#[cfg(unix)]
fn path_units(path: &Path) -> usize {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().len()
}

#[cfg(not(any(unix, windows)))]
fn path_units(path: &Path) -> usize {
    path.as_os_str().to_string_lossy().len()
}

fn validate_path_length(path: &Path) -> Result<()> {
    let units = path_units(path);
    if units > MAX_PATH_UNITS {
        return Err(Error::InvalidKey(format!(
            "FileTree path is {units} platform path units (max {MAX_PATH_UNITS})"
        )));
    }
    Ok(())
}

fn collection_path(base: &Path, collection: &str) -> Result<PathBuf> {
    let path = base.join(encode_component(collection, "collection")?);
    validate_path_length(&path)?;
    if !path.starts_with(base) {
        return Err(Error::PathSecurity(format!(
            "path '{}' escapes base directory",
            path.display()
        )));
    }
    Ok(path)
}

fn entry_path(collection_path: &Path, key: &str) -> Result<PathBuf> {
    let path = collection_path.join(encode_component(key, "key")?);
    validate_path_length(&path)?;
    if !path.starts_with(collection_path) {
        return Err(Error::PathSecurity(format!(
            "path '{}' escapes collection directory",
            path.display()
        )));
    }
    Ok(path)
}

async fn decode_physical_entry(
    entry: &fs::DirEntry,
    kind: &str,
    expected_directory: bool,
) -> Result<String> {
    let file_type = entry
        .file_type()
        .await
        .map_err(|error| Error::StoreConnection {
            message: format!("failed to inspect {kind} entry: {error}"),
        })?;
    if file_type.is_symlink() {
        return Err(Error::PathSecurity(format!(
            "{kind} path '{}' is a symbolic link",
            entry.path().display()
        )));
    }
    if expected_directory && !file_type.is_dir() {
        return Err(Error::InvalidKey(format!(
            "FileTree physical {kind} '{}' is not a directory",
            entry.path().display()
        )));
    }
    if !expected_directory && !file_type.is_file() {
        return Err(Error::InvalidKey(format!(
            "FileTree physical {kind} '{}' is not a regular file",
            entry.path().display()
        )));
    }
    decode_component(&entry.file_name(), kind)
}

/// Async filesystem-based key-value store.
///
/// Each collection is a canonically encoded directory under `base_path`.
/// Each key is a canonically encoded file containing an `OKVE1`-encoded `ManagedEntry`.
/// Logical collection and key names are limited to 125 UTF-8 bytes so their encoded
/// components fit the portable 255-byte filesystem component limit.
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

    async fn collection_dir_exists(&self, path: &Path) -> Result<bool> {
        match fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(Error::PathSecurity(format!(
                "collection path '{}' is a symbolic link",
                path.display()
            ))),
            Ok(metadata) if !metadata.is_dir() => Err(Error::InvalidKey(format!(
                "FileTree collection path '{}' is not a directory",
                path.display()
            ))),
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::StoreConnection {
                message: format!("failed to inspect collection directory: {error}"),
            }),
        }
    }

    async fn ensure_collection_dir(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path)
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!("failed to create collection directory: {error}"),
            })?;
        if !self.collection_dir_exists(path).await? {
            return Err(Error::StoreSetup {
                message: "collection directory disappeared after creation".to_string(),
            });
        }
        Ok(())
    }

    async fn entry_file_exists(&self, path: &Path) -> Result<bool> {
        match fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(Error::PathSecurity(format!(
                "key path '{}' is a symbolic link",
                path.display()
            ))),
            Ok(metadata) if !metadata.is_file() => Err(Error::InvalidKey(format!(
                "FileTree key path '{}' is not a regular file",
                path.display()
            ))),
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::StoreConnection {
                message: format!("failed to inspect key file: {error}"),
            }),
        }
    }

    async fn read_entry(&self, path: &Path) -> Result<Option<ManagedEntry>> {
        if !self.entry_file_exists(path).await? {
            return Ok(None);
        }
        match fs::read(path).await {
            Ok(bytes) => Ok(Some(ManagedEntry::decode(Bytes::from(bytes))?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::StoreConnection {
                message: format!("failed to read file: {error}"),
            }),
        }
    }

    async fn write_entry(&self, path: &Path, entry: &ManagedEntry) -> Result<()> {
        self.entry_file_exists(path).await?;
        fs::write(path, entry.encode())
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to write file: {error}"),
            })?;
        Ok(())
    }

    async fn remove_entry(&self, path: &Path) -> Result<bool> {
        if !self.entry_file_exists(path).await? {
            return Ok(false);
        }
        match fs::remove_file(path).await {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::StoreConnection {
                message: format!("failed to delete file: {error}"),
            }),
        }
    }
}

#[async_trait]
impl AsyncKeyValue for FileTreeStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let path = entry_path(&collection, key)?;
        self.ensure_collection_dir(&collection).await?;
        match self.read_entry(&path).await? {
            Some(entry) if !entry.is_expired() => Ok(Some(entry.value)),
            _ => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let path = entry_path(&collection, key)?;
        self.ensure_collection_dir(&collection).await?;
        match self.read_entry(&path).await? {
            Some(entry) if !entry.is_expired() => {
                let ttl = entry.ttl();
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
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let path = entry_path(&collection, key)?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        self.ensure_collection_dir(&collection).await?;
        self.write_entry(&path, &entry).await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let path = entry_path(&collection, key)?;
        if !self.collection_dir_exists(&collection).await? {
            return Ok(false);
        }
        self.remove_entry(&path).await
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let paths = keys
            .iter()
            .map(|key| entry_path(&collection, key))
            .collect::<Result<Vec<_>>>()?;
        self.ensure_collection_dir(&collection).await?;

        let mut results = Vec::with_capacity(paths.len());
        for path in paths {
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
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let paths = keys
            .iter()
            .map(|key| entry_path(&collection, key))
            .collect::<Result<Vec<_>>>()?;
        self.ensure_collection_dir(&collection).await?;

        let mut results = Vec::with_capacity(paths.len());
        for path in paths {
            match self.read_entry(&path).await? {
                Some(entry) if !entry.is_expired() => {
                    let ttl = entry.ttl();
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

        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let paths = keys
            .iter()
            .map(|key| entry_path(&collection, key))
            .collect::<Result<Vec<_>>>()?;
        let entries = values
            .iter()
            .map(|value| match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => Ok(ManagedEntry::new(value.clone())),
            })
            .collect::<Result<Vec<_>>>()?;
        self.ensure_collection_dir(&collection).await?;

        for (path, entry) in paths.iter().zip(entries.iter()) {
            self.write_entry(path, entry).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        let paths = keys
            .iter()
            .map(|key| entry_path(&collection, key))
            .collect::<Result<Vec<_>>>()?;
        if !self.collection_dir_exists(&collection).await? {
            return Ok(0);
        }

        let mut count = 0;
        for path in paths {
            if self.remove_entry(&path).await? {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for FileTreeStore {
    async fn cull(&self) -> Result<()> {
        let mut collections =
            fs::read_dir(self.base_path())
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read base directory: {error}"),
                })?;
        while let Some(collection) =
            collections
                .next_entry()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read collection entry: {error}"),
                })?
        {
            decode_physical_entry(&collection, "collection", true).await?;

            let mut files =
                fs::read_dir(collection.path())
                    .await
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to read collection directory: {error}"),
                    })?;
            while let Some(file) =
                files
                    .next_entry()
                    .await
                    .map_err(|error| Error::StoreConnection {
                        message: format!("failed to read key entry: {error}"),
                    })?
            {
                decode_physical_entry(&file, "key", false).await?;

                let file_path = file.path();
                if self
                    .read_entry(&file_path)
                    .await?
                    .is_some_and(|entry| entry.is_expired())
                {
                    self.remove_entry(&file_path).await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for FileTreeStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let collection = collection_path(self.base_path(), self.collection_name(collection))?;
        self.ensure_collection_dir(&collection).await?;
        let mut entries =
            fs::read_dir(&collection)
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read collection directory: {error}"),
                })?;
        let mut keys = Vec::new();
        let limit = limit.unwrap_or(10_000).min(10_000);
        while let Some(entry) =
            entries
                .next_entry()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read key entry: {error}"),
                })?
        {
            let key = decode_physical_entry(&entry, "key", false).await?;
            if keys.len() < limit {
                keys.push(key);
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
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read base directory: {error}"),
                })?;
        let mut collections = Vec::new();
        let limit = limit.unwrap_or(10_000).min(10_000);
        while let Some(entry) =
            entries
                .next_entry()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read collection entry: {error}"),
                })?
        {
            let collection = decode_physical_entry(&entry, "collection", true).await?;
            if collections.len() < limit {
                collections.push(collection);
            }
        }
        Ok(collections)
    }
}

#[async_trait]
impl AsyncDestroyCollection for FileTreeStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let collection = collection_path(self.base_path(), collection)?;
        if !self.collection_dir_exists(&collection).await? {
            return Ok(false);
        }

        let mut entries =
            fs::read_dir(&collection)
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read collection directory: {error}"),
                })?;
        while let Some(entry) =
            entries
                .next_entry()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to read key entry: {error}"),
                })?
        {
            decode_physical_entry(&entry, "key", false).await?;
        }

        match fs::remove_dir_all(&collection).await {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::StoreConnection {
                message: format!("failed to remove collection directory: {error}"),
            }),
        }
    }
}

#[async_trait]
impl AsyncDestroyStore for FileTreeStore {
    async fn destroy(&self) -> Result<bool> {
        match fs::remove_dir_all(self.base_path()).await {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(Error::StoreConnection {
                message: format!("failed to remove store directory: {error}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn component_transport_roundtrips_exact_names() {
        let names = [
            "", "Key", "key", "é", "e\u{301}", "a/b", "a:b", "a_b", "../", "\0\u{1f}",
        ];
        let mut physical = HashSet::new();
        for name in names {
            let encoded = encode_component(name, "key").unwrap();
            assert!(encoded.starts_with(COMPONENT_PREFIX));
            assert!(
                encoded
                    .bytes()
                    .all(|byte| byte == b'-' || byte.is_ascii_lowercase() || byte.is_ascii_digit())
            );
            assert!(physical.insert(encoded.clone()));
            assert_eq!(decode_component(OsStr::new(&encoded), "key").unwrap(), name);
        }

        let accepted = "x".repeat(125);
        assert_eq!(encode_component(&accepted, "key").unwrap().len(), 255);
        assert!(matches!(
            encode_component(&"x".repeat(126), "key"),
            Err(Error::InvalidKey(_))
        ));
    }

    #[test]
    fn component_parser_rejects_malformed_names() {
        for component in [
            "collection",
            "okv2-00",
            "okv1-0",
            "okv1-AA",
            "okv1-gg",
            "okv1-ff",
        ] {
            assert!(matches!(
                decode_component(OsStr::new(component), "key"),
                Err(Error::InvalidKey(_))
            ));
        }
        assert!(matches!(
            decode_component(OsStr::new(&format!("okv1-{}", "00".repeat(126))), "key"),
            Err(Error::InvalidKey(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn component_parser_rejects_non_utf8_names() {
        use std::os::unix::ffi::OsStringExt;
        let component = std::ffi::OsString::from_vec(vec![0xff]);
        assert!(matches!(
            decode_component(&component, "key"),
            Err(Error::InvalidKey(_))
        ));
    }

    #[test]
    fn path_length_boundary_is_explicit() {
        assert!(validate_path_length(Path::new(&"x".repeat(MAX_PATH_UNITS))).is_ok());
        assert!(matches!(
            validate_path_length(Path::new(&"x".repeat(MAX_PATH_UNITS + 1))),
            Err(Error::InvalidKey(_))
        ));
    }

    #[tokio::test]
    async fn filetree_roundtrips_distinct_logical_identities() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let identities = [
            ("", ""),
            ("Users", "Key"),
            ("users", "key"),
            ("é", "e\u{301}"),
            ("a/b", "a:b"),
            ("a:b", "a_b"),
            ("../", "../../../etc/passwd"),
            ("control\0", "line\nkey"),
        ];

        for (index, (collection, key)) in identities.iter().enumerate() {
            store
                .put(key, Value::utf8(index.to_string()), Some(collection), None)
                .await
                .unwrap();
        }
        for (index, (collection, key)) in identities.iter().enumerate() {
            assert_eq!(
                store.get(key, Some(collection)).await.unwrap(),
                Some(Value::utf8(index.to_string()))
            );
        }

        let collections = store.collections(None).await.unwrap();
        for (collection, _) in identities {
            assert!(collections.contains(&collection.to_string()));
        }
        let keys = store.keys(Some("a/b"), None).await.unwrap();
        assert_eq!(keys, vec!["a:b".to_string()]);

        for entry in std::fs::read_dir(tmp.path()).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            assert!(name.starts_with(COMPONENT_PREFIX));
            assert!(!name.contains('/'));
            assert!(!name.contains('\\'));
        }
    }

    #[tokio::test]
    async fn filetree_crud_ttl_and_batch_use_the_same_transport() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let collection = "batch/Collection";
        let keys = vec!["a:b".to_string(), "a/b".to_string(), "Key".to_string()];
        let values = vec![Value::from(1_i64), Value::from(2_i64), Value::from(3_i64)];

        store
            .put_many(&keys, &values, Some(collection), Some(60.0))
            .await
            .unwrap();
        assert_eq!(
            store.get_many(&keys, Some(collection)).await.unwrap(),
            values.iter().cloned().map(Some).collect::<Vec<_>>()
        );
        let ttls = store.ttl_many(&keys, Some(collection)).await.unwrap();
        assert!(
            ttls.iter()
                .all(|item| item.as_ref().is_some_and(|(_, ttl)| ttl.is_some()))
        );
        assert_eq!(store.delete_many(&keys, Some(collection)).await.unwrap(), 3);
        assert_eq!(store.delete_many(&keys, Some(collection)).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn filetree_batch_prevalidates_every_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let oversized = "x".repeat(126);
        let keys = vec!["first".to_string(), oversized.clone()];

        assert!(matches!(
            store
                .put_many(
                    &keys,
                    &[Value::from(1_i64), Value::from(2_i64)],
                    Some("batch"),
                    None,
                )
                .await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(store.get("first", Some("batch")).await.unwrap(), None);

        store
            .put("first", Value::from(1_i64), Some("batch"), None)
            .await
            .unwrap();
        assert!(matches!(
            store.delete_many(&keys, Some("batch")).await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(
            store.get("first", Some("batch")).await.unwrap(),
            Some(Value::from(1_i64))
        );

        let collection = collection_path(store.base_path(), "batch").unwrap();
        fs::write(entry_path(&collection, "first").unwrap(), b"corrupt")
            .await
            .unwrap();
        assert!(matches!(
            store.get_many(&keys, Some("batch")).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            store.ttl_many(&keys, Some("batch")).await,
            Err(Error::InvalidKey(_))
        ));
    }

    #[tokio::test]
    async fn filetree_rejects_malformed_physical_identities() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        fs::create_dir(tmp.path().join("legacy")).await.unwrap();

        assert!(matches!(
            store.collections(None).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(store.cull().await, Err(Error::InvalidKey(_))));

        fs::remove_dir(tmp.path().join("legacy")).await.unwrap();
        store
            .put("valid", Value::null(), Some("canonical"), None)
            .await
            .unwrap();
        let collection = collection_path(store.base_path(), "canonical").unwrap();
        fs::write(
            collection.join("legacy"),
            ManagedEntry::new(Value::null()).encode(),
        )
        .await
        .unwrap();

        assert!(matches!(
            store.keys(Some("canonical"), None).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(store.cull().await, Err(Error::InvalidKey(_))));
        assert!(matches!(
            store.destroy_collection("canonical").await,
            Err(Error::InvalidKey(_))
        ));
        assert!(collection.exists());
        assert!(store.destroy().await.unwrap());
        assert!(!tmp.path().exists());
    }

    #[tokio::test]
    async fn filetree_does_not_read_old_sanitized_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let old_collection = tmp.path().join("default_collection");
        fs::create_dir(&old_collection).await.unwrap();
        fs::write(
            old_collection.join(".._.._secret"),
            ManagedEntry::new(Value::utf8("old")).encode(),
        )
        .await
        .unwrap();

        assert_eq!(store.get("../../secret", None).await.unwrap(), None);
    }

    #[tokio::test]
    async fn filetree_rejects_non_okve1_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        store
            .put("corrupt", Value::null(), Some("entries"), None)
            .await
            .unwrap();
        let collection = collection_path(store.base_path(), "entries").unwrap();
        fs::write(
            entry_path(&collection, "corrupt").unwrap(),
            br#"{"value":null}"#,
        )
        .await
        .unwrap();

        let error = store.get("corrupt", Some("entries")).await.unwrap_err();
        assert!(error.to_string().contains("invalid OpenKeyV entry magic"));
        let error = store.cull().await.unwrap_err();
        assert!(error.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[tokio::test]
    async fn filetree_destroy_collection_is_isolated() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        store
            .put("key", Value::from(1_i64), Some("a/b"), None)
            .await
            .unwrap();
        store
            .put("key", Value::from(2_i64), Some("a:b"), None)
            .await
            .unwrap();

        assert!(store.destroy_collection("a/b").await.unwrap());
        assert!(!store.destroy_collection("a/b").await.unwrap());
        assert_eq!(store.get("key", Some("a/b")).await.unwrap(), None);
        assert_eq!(
            store.get("key", Some("a:b")).await.unwrap(),
            Some(Value::from(2_i64))
        );
    }

    #[tokio::test]
    async fn filetree_rejects_oversized_identity_before_filesystem_access() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        assert!(matches!(
            store.put(&"x".repeat(126), Value::null(), None, None).await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);

        assert!(matches!(
            store
                .put("key", Value::null(), Some(&"x".repeat(126)), None)
                .await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filetree_rejects_collection_symlink_escape() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let store = FileTreeStore::new(tmp.path());
        let collection = collection_path(store.base_path(), "escape").unwrap();
        symlink(outside.path(), &collection).unwrap();

        assert!(matches!(
            store.put("key", Value::null(), Some("escape"), None).await,
            Err(Error::PathSecurity(_))
        ));
        assert!(
            !outside
                .path()
                .join(encode_component("key", "key").unwrap())
                .exists()
        );
    }
}
