use super::client::DiskClient;
use super::config::DiskConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;
use std::str;

const TREE_PREFIX: &str = "okv1-";

fn encode_tree_name(collection: &str) -> Result<String> {
    let encoded_len = TREE_PREFIX
        .len()
        .checked_add(collection.len().checked_mul(2).ok_or_else(|| {
            Error::InvalidKey("Disk collection is too large to encode".to_string())
        })?)
        .ok_or_else(|| Error::InvalidKey("Disk collection is too large to encode".to_string()))?;

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(encoded_len);
    encoded.push_str(TREE_PREFIX);
    for byte in collection.as_bytes() {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

fn decode_tree_name(name: &[u8]) -> Result<Option<(Vec<u8>, String)>> {
    if !name.starts_with(TREE_PREFIX.as_bytes()) {
        return Ok(None);
    }

    let physical = str::from_utf8(name)
        .map_err(|_| Error::InvalidKey("Disk physical tree name is not valid UTF-8".to_string()))?;
    let encoded = &physical[TREE_PREFIX.len()..];
    if encoded.len() % 2 != 0 {
        return Err(Error::InvalidKey(
            "Disk physical tree name has an odd hexadecimal length".to_string(),
        ));
    }

    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(Error::InvalidKey(
                "Disk physical tree name is not canonical lowercase hexadecimal".to_string(),
            )),
        };
        bytes.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }

    let collection = String::from_utf8(bytes)
        .map_err(|_| Error::InvalidKey("Disk physical tree name is not valid UTF-8".to_string()))?;
    if encode_tree_name(&collection)? != physical {
        return Err(Error::InvalidKey(
            "Disk physical tree name is not canonical".to_string(),
        ));
    }

    Ok(Some((name.to_vec(), collection)))
}

fn entry_to_ivec(entry: &ManagedEntry) -> Result<sled::IVec> {
    Ok(entry.encode().into())
}

fn ivec_to_entry(iv: sled::IVec) -> Result<ManagedEntry> {
    ManagedEntry::decode(Bytes::from_owner(iv))
}

/// Disk-backed store using [sled], an embedded key-value database.
///
/// Each collection maps to a separate sled `Tree`.
pub struct DiskStore {
    client: DiskClient,
    config: DiskConfig,
}

impl DiskStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(path).map_err(|e| Error::StoreSetup {
            message: format!("failed to open sled database: {}", e),
        })?;
        Ok(Self::with_config(db, DiskConfig::default()))
    }

    pub fn with_config(db: sled::Db, config: DiskConfig) -> Self {
        Self {
            client: DiskClient::new(db),
            config,
        }
    }

    fn db(&self) -> &sled::Db {
        self.client.db()
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn get_tree(&self, collection: &str) -> Result<sled::Tree> {
        let tree_name = encode_tree_name(collection)?;
        self.db()
            .open_tree(tree_name)
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to open tree: {}", e),
            })
    }

    fn owned_tree_names(&self) -> Result<Vec<(Vec<u8>, String)>> {
        self.db()
            .tree_names()
            .into_iter()
            .filter_map(|name| match decode_tree_name(&name) {
                Ok(Some(tree)) => Some(Ok(tree)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    fn open_physical_tree(&self, tree_name: &[u8]) -> Result<sled::Tree> {
        self.db()
            .open_tree(tree_name)
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to open tree: {}", e),
            })
    }

    fn validate_tree_entries(tree: &sled::Tree) -> Result<Vec<sled::IVec>> {
        let mut expired_keys = Vec::new();
        for result in tree.iter() {
            let (key, value) = result.map_err(|e| Error::StoreConnection {
                message: format!("failed to iterate: {}", e),
            })?;
            str::from_utf8(&key).map_err(|_| {
                Error::InvalidKey("Disk physical key is not valid UTF-8".to_string())
            })?;
            let entry = ivec_to_entry(value)?;
            if entry.is_expired() {
                expired_keys.push(key);
            }
        }
        Ok(expired_keys)
    }

    fn validate_tree(tree: &sled::Tree) -> Result<()> {
        Self::validate_tree_entries(tree).map(|_| ())
    }
}

#[async_trait]
impl AsyncKeyValue for DiskStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let key = key.as_bytes();
        let res = tree.get(key).map_err(|e| Error::StoreConnection {
            message: format!("failed to get: {}", e),
        })?;
        match res {
            Some(iv) => {
                let entry = ivec_to_entry(iv)?;
                if entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry.value))
                }
            }
            None => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let res = tree.get(key).map_err(|e| Error::StoreConnection {
            message: format!("failed to get: {}", e),
        })?;
        match res {
            Some(iv) => {
                let entry = ivec_to_entry(iv)?;
                if entry.is_expired() {
                    Ok(None)
                } else {
                    let ttl = entry.ttl();
                    Ok(Some((entry.value, ttl)))
                }
            }
            None => Ok(None),
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
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        let iv = entry_to_ivec(&entry)?;
        let tree = self.get_tree(cname)?;
        tree.insert(key, iv).map_err(|e| Error::StoreConnection {
            message: format!("failed to insert: {}", e),
        })?;
        tree.flush_async()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to flush: {}", e),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let res = tree.remove(key).map_err(|e| Error::StoreConnection {
            message: format!("failed to remove: {}", e),
        })?;
        Ok(res.is_some())
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let res = tree
                .get(key.as_bytes())
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to get: {}", e),
                })?;
            match res {
                Some(iv) => {
                    let entry = ivec_to_entry(iv)?;
                    if entry.is_expired() {
                        results.push(None);
                    } else {
                        results.push(Some(entry.value));
                    }
                }
                None => results.push(None),
            }
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let res = tree
                .get(key.as_bytes())
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to get: {}", e),
                })?;
            match res {
                Some(iv) => {
                    let entry = ivec_to_entry(iv)?;
                    if entry.is_expired() {
                        results.push(None);
                    } else {
                        let ttl = entry.ttl();
                        results.push(Some((entry.value, ttl)));
                    }
                }
                None => results.push(None),
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
        let tree_name = encode_tree_name(cname)?;
        let entries: Vec<_> = keys
            .iter()
            .zip(values.iter())
            .map(|(key, value)| {
                let entry = match ttl {
                    Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds)?,
                    None => ManagedEntry::new(value.clone()),
                };
                Ok((key.as_bytes(), entry_to_ivec(&entry)?))
            })
            .collect::<Result<_>>()?;
        let tree = self.open_physical_tree(tree_name.as_bytes())?;
        for (key, iv) in entries {
            tree.insert(key, iv).map_err(|e| Error::StoreConnection {
                message: format!("failed to insert: {}", e),
            })?;
        }
        tree.flush_async()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to flush: {}", e),
            })?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut count = 0;
        for key in keys {
            let res = tree.remove(key).map_err(|e| Error::StoreConnection {
                message: format!("failed to remove: {}", e),
            })?;
            if res.is_some() {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for DiskStore {
    async fn cull(&self) -> Result<()> {
        let owned_trees = self.owned_tree_names()?;
        let mut expired = Vec::with_capacity(owned_trees.len());
        for (tree_name, _) in owned_trees {
            let tree = self.open_physical_tree(&tree_name)?;
            let expired_keys = Self::validate_tree_entries(&tree)?;
            expired.push((tree, expired_keys));
        }

        for (tree, keys) in expired {
            for key in keys {
                tree.remove(key).map_err(|e| Error::StoreConnection {
                    message: format!("failed to remove expired entry: {}", e),
                })?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for DiskStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let tree = self.get_tree(cname)?;
        let mut keys = Vec::new();
        let limit = limit.unwrap_or(10_000).min(10_000);
        for res in tree.iter() {
            let (k, _) = res.map_err(|e| Error::StoreConnection {
                message: format!("failed to iterate: {}", e),
            })?;
            let key = str::from_utf8(&k).map_err(|_| {
                Error::InvalidKey("Disk physical key is not valid UTF-8".to_string())
            })?;
            if keys.len() < limit {
                keys.push(key.to_string());
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for DiskStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let owned_trees = self.owned_tree_names()?;
        let limit = limit.unwrap_or(10_000).min(10_000);
        Ok(owned_trees
            .into_iter()
            .take(limit)
            .map(|(_, collection)| collection)
            .collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for DiskStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let cname = self.collection_name(Some(collection));
        let tree_name = encode_tree_name(cname)?;
        let exists = self
            .db()
            .tree_names()
            .iter()
            .any(|name| name.as_ref() == tree_name.as_bytes());
        if !exists {
            return Ok(false);
        }

        let tree = self.open_physical_tree(tree_name.as_bytes())?;
        Self::validate_tree(&tree)?;
        self.db()
            .drop_tree(tree_name)
            .map_err(|e| Error::StoreConnection {
                message: format!("failed to drop tree: {}", e),
            })
    }
}

#[async_trait]
impl AsyncDestroyStore for DiskStore {
    async fn destroy(&self) -> Result<bool> {
        let owned_trees = self.owned_tree_names()?;
        for (tree_name, _) in &owned_trees {
            let tree = self.open_physical_tree(tree_name)?;
            Self::validate_tree(&tree)?;
        }

        for (tree_name, _) in owned_trees {
            self.db()
                .drop_tree(tree_name)
                .map_err(|e| Error::StoreConnection {
                    message: format!("failed to drop tree: {}", e),
                })?;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeDelta, Utc};

    fn encoded_entry(value: Value) -> sled::IVec {
        entry_to_ivec(&ManagedEntry::new(value)).unwrap()
    }

    fn expired_entry() -> sled::IVec {
        let mut entry = ManagedEntry::new(Value::utf8("expired"));
        entry.expires_at = Some(Utc::now() - TimeDelta::seconds(1));
        entry_to_ivec(&entry).unwrap()
    }

    fn tree_exists(db: &sled::Db, name: &str) -> bool {
        db.tree_names()
            .iter()
            .any(|tree_name| tree_name.as_ref() == name.as_bytes())
    }

    #[tokio::test]
    async fn test_disk_store_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let value = Value::utf8("Alice");

        store.put("user1", value.clone(), None, None).await.unwrap();
        let got = store.get("user1", None).await.unwrap();
        assert_eq!(got, Some(value));
    }

    #[tokio::test]
    async fn test_disk_store_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let value = Value::null();

        store.put("k", value, None, None).await.unwrap();
        assert!(store.delete("k", None).await.unwrap());
        assert!(!store.delete("k", None).await.unwrap());
    }

    #[tokio::test]
    async fn test_disk_store_collections() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
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
    async fn test_disk_store_destroy_collection() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let value = Value::null();

        store.put("k", value, Some("c1"), None).await.unwrap();
        assert!(store.destroy_collection("c1").await.unwrap());
    }

    #[tokio::test]
    async fn test_disk_store_rejects_json_entry_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let tree = store.get_tree(store.collection_name(None)).unwrap();
        tree.insert("k", br#"{"value":null}"#).unwrap();

        let err = store.get("k", None).await.unwrap_err();

        assert!(err.to_string().contains("invalid OpenKeyV entry magic"));
    }

    #[test]
    fn disk_tree_transport_roundtrips_exact_collection_names() {
        let collections = [
            "", "Users", "users", "é", "e\u{301}", "/", ":", "\u{0001}", "\0",
        ];

        for collection in collections {
            let physical = encode_tree_name(collection).unwrap();
            assert!(physical.starts_with(TREE_PREFIX));
            assert!(
                physical[TREE_PREFIX.len()..]
                    .chars()
                    .all(|character| character.is_ascii_hexdigit()
                        && !character.is_ascii_uppercase())
            );

            let (encoded, decoded) = decode_tree_name(physical.as_bytes()).unwrap().unwrap();
            assert_eq!(encoded, physical.as_bytes());
            assert_eq!(decoded, collection);
        }

        assert_ne!(encode_tree_name("Users"), encode_tree_name("users"));
        assert_ne!(encode_tree_name("é"), encode_tree_name("e\u{301}"));
    }

    #[test]
    fn disk_tree_transport_rejects_malformed_owned_names() {
        for name in ["okv1-0", "okv1-C3a9", "okv1-c3g9"] {
            assert!(matches!(
                decode_tree_name(name.as_bytes()),
                Err(Error::InvalidKey(_))
            ));
        }

        assert!(matches!(
            decode_tree_name(b"okv1-\xff"),
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(decode_tree_name(b"external").unwrap(), None);
    }

    #[tokio::test]
    async fn disk_collection_identities_are_isolated() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let collections = ["", "Users", "users", "é", "e\u{301}", "/", ":", "\0"];

        for (index, collection) in collections.iter().enumerate() {
            store
                .put(
                    &format!("key-{index}"),
                    Value::utf8(*collection),
                    Some(collection),
                    None,
                )
                .await
                .unwrap();
        }

        for (index, collection) in collections.iter().enumerate() {
            assert_eq!(
                store
                    .get(&format!("key-{index}"), Some(collection))
                    .await
                    .unwrap(),
                Some(Value::utf8(*collection))
            );
        }

        let listed = store.collections(None).await.unwrap();
        for collection in collections {
            assert!(listed.contains(&collection.to_string()));
        }
    }

    #[tokio::test]
    async fn disk_owned_namespace_isolated_from_default_and_external_trees() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let store = DiskStore::with_config(db.clone(), DiskConfig::default());
        let default_tree = &db;
        let external_tree = db.open_tree("external").unwrap();
        let raw_collection_tree = db.open_tree("Users").unwrap();

        let default_value = encoded_entry(Value::utf8("default"));
        let external_value = encoded_entry(Value::utf8("external"));
        let raw_value = encoded_entry(Value::utf8("raw"));
        default_tree
            .insert(b"default-key", default_value.clone())
            .unwrap();
        external_tree
            .insert(b"external-key", external_value.clone())
            .unwrap();
        raw_collection_tree
            .insert(b"raw-key", raw_value.clone())
            .unwrap();

        store
            .put("owned-key", Value::utf8("owned"), Some("Users"), None)
            .await
            .unwrap();
        store
            .put("owned-key", Value::utf8("owned-users"), Some("users"), None)
            .await
            .unwrap();

        let collections = store.collections(None).await.unwrap();
        assert!(collections.contains(&"Users".to_string()));
        assert!(collections.contains(&"users".to_string()));
        assert!(!collections.contains(&"external".to_string()));
        assert!(tree_exists(&db, "Users"));

        assert!(store.destroy_collection("Users").await.unwrap());
        assert!(!tree_exists(&db, &encode_tree_name("Users").unwrap()));
        assert!(tree_exists(&db, "Users"));
        assert!(tree_exists(&db, "external"));
        assert_eq!(
            default_tree.get(b"default-key").unwrap(),
            Some(default_value.clone())
        );
        assert_eq!(
            external_tree.get(b"external-key").unwrap(),
            Some(external_value.clone())
        );
        assert_eq!(
            raw_collection_tree.get(b"raw-key").unwrap(),
            Some(raw_value.clone())
        );

        assert!(store.destroy().await.unwrap());
        assert!(!tree_exists(&db, &encode_tree_name("users").unwrap()));
        assert!(tree_exists(&db, "Users"));
        assert!(tree_exists(&db, "external"));
        assert_eq!(
            default_tree.get(b"default-key").unwrap(),
            Some(default_value)
        );
    }

    #[tokio::test]
    async fn disk_cull_ignores_external_trees() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let store = DiskStore::with_config(db.clone(), DiskConfig::default());
        let owned_tree = store.get_tree("owned").unwrap();
        owned_tree.insert(b"expired", expired_entry()).unwrap();
        let external_tree = db.open_tree("external").unwrap();
        external_tree.insert(b"corrupt", b"not-an-entry").unwrap();

        store.cull().await.unwrap();

        assert!(owned_tree.get(b"expired").unwrap().is_none());
        assert_eq!(
            external_tree.get(b"corrupt").unwrap(),
            Some(sled::IVec::from(b"not-an-entry"))
        );
    }

    #[tokio::test]
    async fn disk_cull_validates_all_owned_trees_before_mutation() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let store = DiskStore::with_config(db.clone(), DiskConfig::default());
        let owned_tree = store.get_tree("owned").unwrap();
        owned_tree.insert(b"expired", expired_entry()).unwrap();
        let malformed_tree = db.open_tree("okv1-0").unwrap();
        malformed_tree.insert(b"corrupt", b"not-an-entry").unwrap();

        let err = store.cull().await.unwrap_err();

        assert!(matches!(err, Error::InvalidKey(_)));
        assert!(owned_tree.get(b"expired").unwrap().is_some());
    }

    #[tokio::test]
    async fn disk_keys_validate_entries_after_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let tree = store.get_tree("keys").unwrap();
        tree.insert(b"z", b"not-an-entry").unwrap();
        tree.insert(vec![0xff], b"not-an-entry").unwrap();

        for limit in [0, 1] {
            let err = store.keys(Some("keys"), Some(limit)).await.unwrap_err();
            assert!(matches!(err, Error::InvalidKey(_)));
        }
    }

    #[tokio::test]
    async fn disk_put_many_prevalidates_before_opening_a_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskStore::new(tmp.path()).unwrap();
        let collection = "not-created";
        let keys = vec!["key".to_string()];
        let values = vec![Value::null()];

        let err = store
            .put_many(&keys, &values, Some(collection), Some(0.0))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::InvalidTtl(_)));
        assert!(!tree_exists(
            store.db(),
            &encode_tree_name(collection).unwrap()
        ));
    }

    #[tokio::test]
    async fn disk_destroy_collection_validates_before_dropping() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let store = DiskStore::with_config(db.clone(), DiskConfig::default());
        let tree_name = encode_tree_name("corrupt").unwrap();
        let tree = db.open_tree(&tree_name).unwrap();
        tree.insert(b"corrupt", b"not-an-entry").unwrap();

        let err = store.destroy_collection("corrupt").await.unwrap_err();

        assert!(matches!(err, Error::Deserialization(_)));
        assert!(tree_exists(&db, &tree_name));
    }

    #[tokio::test]
    async fn disk_destroy_validates_all_owned_trees_before_dropping() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let store = DiskStore::with_config(db.clone(), DiskConfig::default());
        let valid_tree_name = encode_tree_name("valid").unwrap();
        let valid_tree = db.open_tree(&valid_tree_name).unwrap();
        valid_tree.insert(b"expired", expired_entry()).unwrap();
        let malformed_tree_name = encode_tree_name("corrupt").unwrap();
        let malformed_tree = db.open_tree(&malformed_tree_name).unwrap();
        malformed_tree.insert(b"corrupt", b"not-an-entry").unwrap();

        let err = store.destroy().await.unwrap_err();

        assert!(matches!(err, Error::Deserialization(_)));
        assert!(tree_exists(&db, &valid_tree_name));
        assert!(tree_exists(&db, &malformed_tree_name));
    }

    #[tokio::test]
    async fn disk_collections_validate_all_owned_names_before_limit() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let store = DiskStore::with_config(db.clone(), DiskConfig::default());
        store.get_tree("valid").unwrap();
        db.open_tree("okv1-0").unwrap();

        let err = store.collections(Some(1)).await.unwrap_err();

        assert!(matches!(err, Error::InvalidKey(_)));
    }
}
