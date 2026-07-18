use openkeyv::protocol::{
    AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections, AsyncEnumerateKeys,
    AsyncKeyValue,
};
use openkeyv::value::Value;

async fn assert_exact_identity<S: AsyncKeyValue>(store: &S) {
    let cases = [
        ("", ""),
        ("Users", "same"),
        ("users", "same"),
        ("e\u{301}", "unicode"),
        ("é", "unicode"),
        ("*?[\\]", "line\nnull\0/:*?[]\\"),
        ("a:b", "c"),
        ("a", "b:c"),
    ];

    for (index, (collection, key)) in cases.iter().enumerate() {
        store
            .put(
                key,
                Value::utf8(format!("value-{index}")),
                Some(collection),
                None,
            )
            .await
            .unwrap();
    }

    for (index, (collection, key)) in cases.iter().enumerate() {
        assert_eq!(
            store.get(key, Some(collection)).await.unwrap(),
            Some(Value::utf8(format!("value-{index}"))),
            "identity case {index} must roundtrip exactly",
        );
    }

    let batch_collection = "batch:*?[\\]";
    let batch_keys = vec![
        "".to_string(),
        "line\nnull\0/:*?[]\\".to_string(),
        "Users".to_string(),
        "users".to_string(),
    ];
    let batch_values = vec![
        Value::utf8("empty"),
        Value::utf8("special"),
        Value::utf8("upper"),
        Value::utf8("lower"),
    ];
    store
        .put_many(&batch_keys, &batch_values, Some(batch_collection), None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_many(&batch_keys, Some(batch_collection))
            .await
            .unwrap(),
        batch_values.into_iter().map(Some).collect::<Vec<_>>(),
    );

    assert_eq!(
        store
            .ttl("line\nnull\0/:*?[]\\", Some(batch_collection))
            .await
            .unwrap(),
        Some((Value::utf8("special"), None)),
    );
}

async fn assert_collection_identity_and_destruction<S>(store: &S)
where
    S: AsyncKeyValue
        + AsyncEnumerateKeys
        + AsyncEnumerateCollections
        + AsyncDestroyCollection
        + AsyncDestroyStore,
{
    store
        .put("same", Value::utf8("upper"), Some("Users"), None)
        .await
        .unwrap();
    store
        .put("same", Value::utf8("lower"), Some("users"), None)
        .await
        .unwrap();

    assert_eq!(
        store.keys(Some("Users"), None).await.unwrap(),
        vec!["same".to_string()]
    );
    assert_eq!(
        store.keys(Some("users"), None).await.unwrap(),
        vec!["same".to_string()]
    );
    let collections = store.collections(None).await.unwrap();
    assert!(collections.iter().any(|name| name == "Users"));
    assert!(collections.iter().any(|name| name == "users"));

    assert!(store.destroy_collection("Users").await.unwrap());
    assert_eq!(store.get("same", Some("Users")).await.unwrap(), None);
    assert_eq!(
        store.get("same", Some("users")).await.unwrap(),
        Some(Value::utf8("lower"))
    );

    assert!(store.destroy_collection("users").await.unwrap());
    assert_eq!(store.get("same", Some("users")).await.unwrap(), None);
    assert!(store.destroy().await.unwrap());
}

#[tokio::test]
async fn memory_identity_regression() {
    let store = openkeyv::store::memory::MemoryStore::new();
    assert_exact_identity(&store).await;
    assert_collection_identity_and_destruction(&store).await;
}

#[tokio::test]
async fn simple_identity_regression() {
    let store = openkeyv::store::simple::SimpleStore::new();
    assert_exact_identity(&store).await;
    assert_collection_identity_and_destruction(&store).await;
}

#[tokio::test]
async fn filetree_identity_regression() {
    let directory = tempfile::tempdir().unwrap();
    let store = openkeyv::store::filetree::FileTreeStore::new(directory.path());
    assert_exact_identity(&store).await;
    assert_collection_identity_and_destruction(&store).await;
}

#[cfg(feature = "disk")]
#[tokio::test]
async fn disk_identity_regression() {
    let directory = tempfile::tempdir().unwrap();
    let store = openkeyv::store::disk::DiskStore::new(directory.path()).unwrap();
    assert_exact_identity(&store).await;
    assert_collection_identity_and_destruction(&store).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_identity_regression() {
    let directory = tempfile::tempdir().unwrap();
    let store = openkeyv::store::rocksdb::RocksDBStore::new(directory.path()).unwrap();
    assert_exact_identity(&store).await;
    assert_collection_identity_and_destruction(&store).await;
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_identity_regression() {
    let store = openkeyv::store::duckdb::DuckDBStore::new(None, None)
        .await
        .unwrap();
    assert_exact_identity(&store).await;
    assert_collection_identity_and_destruction(&store).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_identity_regression() {
    let store = openkeyv::store::sqlite::SqliteStore::new(None, None)
        .await
        .unwrap();
    assert_exact_identity(&store).await;
    assert_collection_identity_and_destruction(&store).await;
}
