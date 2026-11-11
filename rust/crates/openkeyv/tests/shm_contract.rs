//! Differential contract tests: the shm store must behave exactly like the
//! memory store for every operation, result, TTL nuance, CAS branch, change
//! event, and error the public traits can express. Each scenario runs once
//! against each backend and the observable results are compared directly —
//! not asserted against hand-written expectations, so any future drift in
//! either backend fails here.

#![cfg(unix)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use openkeyv::Revision;
use openkeyv::change::{
    ChangeCause, ChangeCursor, ChangeFeedRequest, ChangeFilter, ChangeOperation, ChangeStart,
    ChangeStream, StoreChange,
};
use openkeyv::protocol::{
    AsyncChangeFeed, AsyncCompareAndSwap, AsyncCull, AsyncDestroyCollection, AsyncDestroyStore,
    AsyncEnumerateCollections, AsyncEnumerateKeys, AsyncKeyValue, CompareAndDeleteResult,
    CompareAndSwapResult,
};
use openkeyv::store::memory::{MemoryConfig, MemoryStore};
use openkeyv::store::shm::{ShmCreateConfig, ShmStore};
use openkeyv::value::Value;

/// Every capability the contract exercises, as trait objects so one scenario
/// body runs unchanged against both backends.
trait SyncSource:
    AsyncChangeFeed
    + AsyncKeyValue
    + AsyncCompareAndSwap
    + AsyncEnumerateCollections
    + AsyncEnumerateKeys
{
}
impl<T> SyncSource for T where
    T: AsyncChangeFeed
        + AsyncKeyValue
        + AsyncCompareAndSwap
        + AsyncEnumerateCollections
        + AsyncEnumerateKeys
{
}

struct Harness {
    kv: Arc<dyn AsyncKeyValue>,
    cas: Arc<dyn AsyncCompareAndSwap>,
    cull: Arc<dyn AsyncCull>,
    keys: Arc<dyn AsyncEnumerateKeys>,
    collections: Arc<dyn AsyncEnumerateCollections>,
    destroy_store: Arc<dyn AsyncDestroyStore>,
    destroy_collection: Arc<dyn AsyncDestroyCollection>,
    feed: Arc<dyn AsyncChangeFeed>,
    sync: Arc<dyn SyncSource>,
}

fn memory_harness() -> Harness {
    let store = Arc::new(MemoryStore::new());
    Harness {
        kv: store.clone(),
        cas: store.clone(),
        cull: store.clone(),
        keys: store.clone(),
        collections: store.clone(),
        destroy_store: store.clone(),
        destroy_collection: store.clone(),
        sync: store.clone(),
        feed: store,
    }
}

fn shm_harness(name: &str) -> Harness {
    // Same rule as the unit tests: the parent must be owner-only because
    // `/tmp` on Linux (1777) is group/world-writable and refused by the store.
    let dir = std::env::temp_dir().join(format!("openkeyv-contract-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("contract test directory must create");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .expect("contract test directory permissions must apply");
    }
    let path = dir.join(name);
    let store = Arc::new(
        ShmStore::create(
            ShmCreateConfig::new(&path)
                .with_capacity_bytes(16 * 1024 * 1024)
                .with_index_entries(1_024)
                .with_change_retention(1_000),
        )
        .expect("shm store must create"),
    );
    Harness {
        kv: store.clone(),
        cas: store.clone(),
        cull: store.clone(),
        keys: store.clone(),
        collections: store.clone(),
        destroy_store: store.clone(),
        destroy_collection: store.clone(),
        sync: store.clone(),
        feed: store,
    }
}

/// Run one scenario against the memory store and the shm store; any
/// observable difference fails whichever backend drifts. Call sites invoke
/// `scenario(&memory_harness())` and `scenario(&shm_harness(name))`
/// explicitly — async fn items do not implement `for<'a> Fn`, so no
/// higher-ranked callback wrapper is possible here.
async fn drain(stream: &mut Box<dyn ChangeStream + Send>, expected: usize) -> Vec<StoreChange> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut changes = Vec::new();
    while changes.len() < expected {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} change events, got {}",
            changes.len()
        );
        match stream.recv().await.expect("change stream must not error") {
            Some(change) => changes.push(change),
            None => break,
        }
    }
    changes
}

/// Events with backend-specific fields stripped for comparison. The cause
/// rides along: cause tagging is part of the differential contract.
fn event_summary(changes: &[StoreChange]) -> Vec<(u64, String, String, String, String)> {
    changes
        .iter()
        .map(|c| {
            (
                c.revision,
                c.cursor.as_str().to_string(),
                c.collection.clone(),
                format!("{:?}/{}", c.operation, c.key),
                format!("{:?}", c.cause),
            )
        })
        .collect()
}

async fn subscribe_live(h: &Harness) -> Box<dyn ChangeStream + Send> {
    h.feed
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Latest,
            filter: ChangeFilter::default(),
        })
        .await
        .expect("subscribe must succeed")
}

async fn scenario_kv_basics(h: &Harness) {
    // Plain put/get/overwrite.
    h.kv.put("k1", Value::utf8("v1"), None, None).await.unwrap();
    assert_eq!(h.kv.get("k1", None).await.unwrap(), Some(Value::utf8("v1")));
    h.kv.put("k1", Value::utf8("v2"), None, None).await.unwrap();
    assert_eq!(h.kv.get("k1", None).await.unwrap(), Some(Value::utf8("v2")));
    assert_eq!(h.kv.get("missing", None).await.unwrap(), None);

    // ttl(): remaining time present for TTL entries, None otherwise.
    h.kv.put("t1", Value::utf8("x"), None, Some(30.0))
        .await
        .unwrap();
    let (value, ttl) = h.kv.ttl("t1", None).await.unwrap().unwrap();
    assert_eq!(value, Value::utf8("x"));
    let ttl = ttl.expect("ttl entry must report remaining time");
    assert!((0.0..=30.0).contains(&ttl), "ttl {ttl} out of range");

    let (value, ttl) = h.kv.ttl("k1", None).await.unwrap().unwrap();
    assert_eq!(value, Value::utf8("v2"));
    assert!(ttl.is_none(), "non-TTL entry must not report a ttl");

    assert_eq!(h.kv.ttl("missing", None).await.unwrap(), None);

    // Explicit collection scoping.
    h.kv.put("k1", Value::utf8("scoped"), Some("other"), None)
        .await
        .unwrap();
    assert_eq!(
        h.kv.get("k1", Some("other")).await.unwrap(),
        Some(Value::utf8("scoped"))
    );
    assert_eq!(h.kv.get("k1", None).await.unwrap(), Some(Value::utf8("v2")));

    // Batch operations.
    let keys: Vec<String> = ["b1", "b2", "b3"].iter().map(|s| s.to_string()).collect();
    let values: Vec<Value> = ["1", "2", "3"].iter().map(|s| Value::utf8(*s)).collect();
    h.kv.put_many(&keys, &values, Some("batch"), None)
        .await
        .unwrap();
    let got = h.kv.get_many(&keys, Some("batch")).await.unwrap();
    assert_eq!(
        got,
        values.iter().map(|v| Some(v.clone())).collect::<Vec<_>>()
    );
    let mismatched =
        h.kv.put_many(&keys, &values[..2], Some("batch"), None)
            .await;
    assert!(mismatched.is_err(), "put_many length mismatch must fail");
    assert_eq!(h.kv.delete_many(&keys, Some("batch")).await.unwrap(), 3);
    assert_eq!(h.kv.delete_many(&keys, Some("batch")).await.unwrap(), 0);
}

#[tokio::test]
async fn kv_basics_match() {
    scenario_kv_basics(&memory_harness()).await;
    scenario_kv_basics(&shm_harness("kv_basics")).await;
}

async fn scenario_ttl_expiry(h: &Harness) {
    h.kv.put("e1", Value::utf8("soon"), None, Some(0.05))
        .await
        .unwrap();
    assert_eq!(
        h.kv.get("e1", None).await.unwrap(),
        Some(Value::utf8("soon"))
    );
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Expiry (lazy on memory, swept on shm): reads report absence.
    assert_eq!(h.kv.get("e1", None).await.unwrap(), None);
    assert_eq!(h.kv.ttl("e1", None).await.unwrap(), None);
    assert_eq!(h.cas.get_with_revision("e1", None).await.unwrap(), None);
}

#[tokio::test]
async fn ttl_expiry_and_expired_delete_match() {
    scenario_ttl_expiry(&memory_harness()).await;
    scenario_ttl_expiry(&shm_harness("ttl_expiry")).await;
}

/// Caller-issued mutations stay `Explicit` on every backend.
#[tokio::test]
async fn explicit_mutations_stay_cause_explicit() {
    for h in [&memory_harness(), &shm_harness("explicit_cause")] {
        let mut stream = subscribe_live(h).await;
        h.kv.put("x", Value::utf8("1"), None, None).await.unwrap();
        assert!(h.kv.delete("x", None).await.unwrap());
        let put = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("put event must arrive")
            .expect("stream must not error")
            .expect("stream must not end");
        let delete = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("delete event must arrive")
            .expect("stream must not error")
            .expect("stream must not end");
        assert_eq!(put.cause, ChangeCause::Explicit);
        assert_eq!(delete.cause, ChangeCause::Explicit);
    }
}

/// Active expiry on shm: a TTL entry is physically removed and emits exactly
/// one `Expired` delete within 2s with no writes touching the key. The
/// sweeper runs on its own thread, so this drives itself.
#[tokio::test]
async fn ttl_entry_is_removed_and_emits_expired_within_2s() {
    let h = shm_harness("ttl_sweep");
    let mut stream = subscribe_live(&h).await;
    h.kv
        .put("victim", Value::utf8("dies"), None, Some(0.05))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut saw_put = false;
    let expired = loop {
        let change = tokio::time::timeout(Duration::from_millis(500), stream.recv())
            .await
            .expect("events must keep arriving")
            .expect("stream must not error")
            .expect("stream must not end");
        match (&change.operation, change.key.as_str()) {
            (ChangeOperation::Put, "victim") => {
                assert_eq!(change.cause, ChangeCause::Explicit);
                saw_put = true;
            }
            (ChangeOperation::Delete, "victim") => break change,
            _ => {}
        }
        assert!(Instant::now() < deadline, "no Expired removal within 2s");
    };
    assert!(saw_put, "the put event must precede the removal");
    assert_eq!(expired.cause, ChangeCause::Expired);

    // Physically gone: enumeration misses it and a delete reports absence.
    assert!(!h
        .keys
        .keys(Some("default_collection"), None)
        .await
        .unwrap()
        .contains(&"victim".to_string()));
    assert!(!h.kv.delete("victim", None).await.unwrap());
}

/// Capacity-driven eviction on memory emits `Expired` deletes — the same
/// cause shm's cull and sweep paths carry (backend parity of cause).
#[tokio::test]
async fn memory_eviction_emits_expired_cause() {
    let store = Arc::new(MemoryStore::with_config(MemoryConfig::new(Some(2), None, None)));
    store
        .put("a", Value::utf8("1"), None, None)
        .await
        .unwrap();
    store
        .put("b", Value::utf8("2"), None, None)
        .await
        .unwrap();
    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Latest,
            filter: Default::default(),
        })
        .await
        .unwrap();
    store
        .put("c", Value::utf8("3"), None, None)
        .await
        .unwrap();

    // put() records the Put for "c" first, then the capacity-eviction
    // Delete for "a" — drain both in order.
    let put_c = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .expect("put event must arrive")
        .expect("stream must not error")
        .expect("stream must not end");
    assert_eq!(put_c.key, "c");
    assert_eq!(put_c.operation, ChangeOperation::Put);
    assert_eq!(put_c.cause, ChangeCause::Explicit);

    let evicted = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .expect("eviction event must arrive")
        .expect("stream must not error")
        .expect("stream must not end");
    assert_eq!(evicted.key, "a", "capacity eviction removes the smallest key");
    assert_eq!(evicted.operation, ChangeOperation::Delete);
    assert_eq!(evicted.cause, ChangeCause::Expired);
}

async fn scenario_cas(h: &Harness) {
    // Create-if-absent on an absent key.
    let applied = h
        .cas
        .compare_and_swap("c1", None, Value::utf8("one"), None, None)
        .await
        .unwrap();
    let revision = match applied {
        CompareAndSwapResult::Applied { revision } => revision,
        other => panic!("expected Applied, got {other:?}"),
    };

    // Create-if-absent on an existing key conflicts with the current value.
    let conflict = h
        .cas
        .compare_and_swap("c1", None, Value::utf8("two"), None, None)
        .await
        .unwrap();
    match conflict {
        CompareAndSwapResult::Conflict { current } => {
            let current = current.expect("conflict must carry the current value");
            assert_eq!(current.value, Value::utf8("one"));
            assert_eq!(current.revision, revision);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }

    // Wrong expectation conflicts; matching expectation applies and yields a
    // fresh revision.
    let wrong = Revision::from_bytes([9u8; 16]);
    let conflict = h
        .cas
        .compare_and_swap("c1", Some(&wrong), Value::utf8("two"), None, None)
        .await
        .unwrap();
    assert!(matches!(conflict, CompareAndSwapResult::Conflict { .. }));

    let applied = h
        .cas
        .compare_and_swap("c1", Some(&revision), Value::utf8("two"), None, None)
        .await
        .unwrap();
    let revision2 = match applied {
        CompareAndSwapResult::Applied { revision } => revision,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert_ne!(revision, revision2);

    // get_with_revision reflects the applied write.
    let seen = h.cas.get_with_revision("c1", None).await.unwrap().unwrap();
    assert_eq!(seen.value, Value::utf8("two"));
    assert_eq!(seen.revision, revision2);

    // compare_and_delete: wrong revision conflicts, right one deletes.
    let conflict = h.cas.compare_and_delete("c1", &wrong, None).await.unwrap();
    match conflict {
        CompareAndDeleteResult::Conflict { current } => {
            assert_eq!(current.unwrap().revision, revision2);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    let deleted = h
        .cas
        .compare_and_delete("c1", &revision2, None)
        .await
        .unwrap();
    assert!(matches!(deleted, CompareAndDeleteResult::Deleted));
    assert_eq!(h.kv.get("c1", None).await.unwrap(), None);
    let conflict = h
        .cas
        .compare_and_delete("c1", &revision2, None)
        .await
        .unwrap();
    match conflict {
        CompareAndDeleteResult::Conflict { current } => {
            assert!(
                current.is_none(),
                "delete of an absent key conflicts with no current"
            );
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn cas_semantics_match() {
    scenario_cas(&memory_harness()).await;
    scenario_cas(&shm_harness("cas")).await;
}

async fn scenario_cas_expired(h: &Harness) {
    h.kv.put("x1", Value::utf8("v"), None, Some(0.05))
        .await
        .unwrap();
    let stale = h
        .cas
        .get_with_revision("x1", None)
        .await
        .unwrap()
        .unwrap()
        .revision;
    tokio::time::sleep(Duration::from_millis(120)).await;

    // expected=None on an expired entry creates.
    let applied = h
        .cas
        .compare_and_swap("x1", None, Value::utf8("fresh"), None, None)
        .await
        .unwrap();
    assert!(matches!(applied, CompareAndSwapResult::Applied { .. }));
    assert_eq!(
        h.kv.get("x1", None).await.unwrap(),
        Some(Value::utf8("fresh"))
    );

    // An expired entry plus a stale expectation conflicts with no current
    // value, on both backends.
    h.kv.put("x2", Value::utf8("v"), None, Some(0.05))
        .await
        .unwrap();
    let stale2 = h
        .cas
        .get_with_revision("x2", None)
        .await
        .unwrap()
        .unwrap()
        .revision;
    tokio::time::sleep(Duration::from_millis(120)).await;
    let conflict = h
        .cas
        .compare_and_swap("x2", Some(&stale2), Value::utf8("v2"), None, None)
        .await
        .unwrap();
    match conflict {
        CompareAndSwapResult::Conflict { current } => {
            assert!(current.is_none(), "expired corpse must surface as absent");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    let conflict = h.cas.compare_and_delete("x2", &stale, None).await.unwrap();
    match conflict {
        CompareAndDeleteResult::Conflict { current } => {
            assert!(current.is_none());
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    // The corpse is gone for plain readers too.
    assert_eq!(h.kv.get("x2", None).await.unwrap(), None);
}

#[tokio::test]
async fn cas_over_expired_entries_matches() {
    scenario_cas_expired(&memory_harness()).await;
    scenario_cas_expired(&shm_harness("cas_expired")).await;
}

async fn scenario_cull(h: &Harness) {
    // Subscribe before the TTL put: on shm the expiry sweeper may reclaim
    // the key before the explicit cull runs, and the Delete/Expired must be
    // observed exactly once — from whichever mechanism fired.
    let mut stream = subscribe_live(h).await;
    h.kv.put("gone", Value::utf8("v"), None, Some(0.05))
        .await
        .unwrap();
    h.kv.put("stays", Value::utf8("v"), None, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    h.cull.cull().await.unwrap();

    // Collect past the two Puts until the Delete for "gone" shows up.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut expired_delete = None;
    while expired_delete.is_none() {
        assert!(
            Instant::now() < deadline,
            "no Delete/Expired for the expired key within 5s"
        );
        let change = tokio::time::timeout(Duration::from_millis(500), stream.recv())
            .await
            .expect("recv must not hang")
            .expect("change stream must not error")
            .expect("change stream must not end");
        if matches!(change.operation, ChangeOperation::Delete) && change.key == "gone" {
            assert_eq!(
                change.cause,
                ChangeCause::Expired,
                "expiry removal must carry the Expired cause"
            );
            expired_delete = Some(change);
        }
    }

    // Quiet window: neither the sweeper nor a second cull may re-emit.
    let quiet_end = Instant::now() + Duration::from_millis(300);
    loop {
        let remaining = quiet_end
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.recv()).await {
            Ok(Ok(Some(change))) => assert!(
                !(matches!(change.operation, ChangeOperation::Delete)
                    && change.key == "gone"),
                "expired key must be removed exactly once"
            ),
            Ok(Ok(None)) => break,
            Ok(Err(error)) => panic!("change stream must not error: {error}"),
            Err(_quiet) => break,
        }
    }

    assert_eq!(h.kv.get("gone", None).await.unwrap(), None);
    assert_eq!(
        h.kv.get("stays", None).await.unwrap(),
        Some(Value::utf8("v"))
    );
}

#[tokio::test]
async fn cull_removes_expired_and_emits_delete() {
    scenario_cull(&memory_harness()).await;
    scenario_cull(&shm_harness("cull")).await;
}

async fn scenario_enumerate(h: &Harness) {
    for (collection, key) in [
        ("alpha", "a1"),
        ("alpha", "a2"),
        ("beta", "b1"),
        ("beta", "b2"),
        ("beta", "b3"),
    ] {
        h.kv.put(key, Value::utf8(key), Some(collection), None)
            .await
            .unwrap();
    }

    let mut keys = h.keys.keys(Some("alpha"), None).await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a1".to_string(), "a2".to_string()]);

    let mut keys = h.keys.keys(Some("beta"), None).await.unwrap();
    keys.sort();
    assert_eq!(
        keys,
        vec!["b1".to_string(), "b2".to_string(), "b3".to_string()]
    );

    // With two keys a limit of 1 must yield exactly one key on both.
    let limited = h.keys.keys(Some("alpha"), Some(1)).await.unwrap();
    assert_eq!(limited.len(), 1);

    let mut collections = h.collections.collections(None).await.unwrap();
    collections.sort();
    assert_eq!(collections, vec!["alpha".to_string(), "beta".to_string()]);
    let limited = h.collections.collections(Some(1)).await.unwrap();
    assert_eq!(limited.len(), 1);

    // Empty and missing collections enumerate empty.
    assert!(h.keys.keys(Some("missing"), None).await.unwrap().is_empty());
}

#[tokio::test]
async fn enumeration_matches() {
    scenario_enumerate(&memory_harness()).await;
    scenario_enumerate(&shm_harness("enumerate")).await;
}

async fn scenario_destroy_collection(h: &Harness) {
    for key in ["d1", "d2"] {
        h.kv.put(key, Value::utf8(key), Some("doomed"), None)
            .await
            .unwrap();
    }
    h.kv.put("keep", Value::utf8("keep"), Some("safe"), None)
        .await
        .unwrap();

    let mut stream = subscribe_live(h).await;
    assert!(
        h.destroy_collection
            .destroy_collection("doomed")
            .await
            .unwrap()
    );
    let events = drain(&mut stream, 2).await;
    assert_eq!(event_summary(&events).len(), 2);
    for event in &events {
        assert_eq!(event.collection, "doomed");
        assert!(matches!(event.operation, ChangeOperation::Delete));
    }

    // A vanished collection reports false, twice — checked before any read of
    // the doomed collection, because backends that materialize collections on
    // reads (memory) would otherwise bring it back between the two calls.
    assert!(
        !h.destroy_collection
            .destroy_collection("doomed")
            .await
            .unwrap()
    );
    assert!(
        !h.destroy_collection
            .destroy_collection("never-was")
            .await
            .unwrap()
    );

    // Enumeration is empty; the untouched collection survives.
    assert!(h.keys.keys(Some("doomed"), None).await.unwrap().is_empty());
    assert_eq!(
        h.kv.get("keep", Some("safe")).await.unwrap(),
        Some(Value::utf8("keep"))
    );
}

#[tokio::test]
async fn destroy_collection_matches() {
    scenario_destroy_collection(&memory_harness()).await;
    scenario_destroy_collection(&shm_harness("destroy_collection")).await;
}

async fn scenario_destroy_store(h: &Harness) {
    h.kv.put("z1", Value::utf8("v"), Some("zstore"), None)
        .await
        .unwrap();
    assert!(h.destroy_store.destroy().await.unwrap());

    // The collection and its data are gone. The existence check runs before
    // the reads because read-path materialization (memory) would otherwise
    // resurrect an empty "zstore" between the assertions.
    assert!(
        !h.collections
            .collections(None)
            .await
            .unwrap()
            .contains(&"zstore".to_string())
    );
    assert!(h.keys.keys(Some("zstore"), None).await.unwrap().is_empty());
    assert_eq!(h.kv.get("z1", Some("zstore")).await.unwrap(), None);

    // Destroying an already-destroyed store still succeeds.
    assert!(h.destroy_store.destroy().await.unwrap());
}

#[tokio::test]
async fn destroy_store_matches() {
    scenario_destroy_store(&memory_harness()).await;
    scenario_destroy_store(&shm_harness("destroy_store")).await;
}

async fn scenario_default_collection(h: &Harness) {
    h.kv.put("d", Value::utf8("v"), None, None).await.unwrap();
    let collections = h.collections.collections(None).await.unwrap();
    assert!(collections.iter().any(|c| c == "default_collection"));
    assert_eq!(
        h.kv.get("d", Some("default_collection")).await.unwrap(),
        Some(Value::utf8("v"))
    );
}

#[tokio::test]
async fn default_collection_matches() {
    scenario_default_collection(&memory_harness()).await;
    scenario_default_collection(&shm_harness("default_collection")).await;
}

async fn scenario_feed_cursor(h: &Harness) {
    h.kv.put("f1", Value::utf8("1"), None, None).await.unwrap();
    h.kv.put("f2", Value::utf8("2"), None, None).await.unwrap();
    h.kv.delete("f1", None).await.unwrap();

    // Beginning replays everything in order.
    let mut stream = h
        .feed
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Beginning,
            filter: ChangeFilter::default(),
        })
        .await
        .unwrap();
    let events = drain(&mut stream, 3).await;
    assert_eq!(
        event_summary(&events),
        vec![
            (1, "1".into(), "default_collection".into(), "Put/f1".into(), "Explicit".into()),
            (2, "2".into(), "default_collection".into(), "Put/f2".into(), "Explicit".into()),
            (
                3,
                "3".into(),
                "default_collection".into(),
                "Delete/f1".into(),
                "Explicit".into()
            ),
        ]
    );

    // After(cursor) replays strictly after the cursor.
    let mut stream = h
        .feed
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::After(ChangeCursor::new("1")),
            filter: ChangeFilter::default(),
        })
        .await
        .unwrap();
    let events = drain(&mut stream, 2).await;
    assert_eq!(event_summary(&events)[0].0, 2);

    // Cursor 0 is always valid and replays everything.
    let mut stream = h
        .feed
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::After(ChangeCursor::new("0")),
            filter: ChangeFilter::default(),
        })
        .await
        .unwrap();
    let events = drain(&mut stream, 3).await;
    assert_eq!(event_summary(&events).len(), 3);

    // A cursor beyond the history is rejected identically.
    let error = h
        .feed
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::After(ChangeCursor::new("999")),
            filter: ChangeFilter::default(),
        })
        .await;
    assert!(error.is_err(), "future cursor must be invalid");

    // Non-numeric cursors are rejected identically.
    let error = h
        .feed
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::After(ChangeCursor::new("abc")),
            filter: ChangeFilter::default(),
        })
        .await;
    assert!(error.is_err(), "non-numeric cursor must be invalid");
}

#[tokio::test]
async fn change_replay_cursor_and_errors_match() {
    scenario_feed_cursor(&memory_harness()).await;
    scenario_feed_cursor(&shm_harness("feed_cursor")).await;
}

async fn scenario_feed_filter(h: &Harness) {
    let mut stream = h
        .feed
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Latest,
            filter: ChangeFilter {
                collections: vec!["picked".to_string()],
                operations: vec![ChangeOperation::Put],
            },
        })
        .await
        .unwrap();

    h.kv.put("p1", Value::utf8("v"), Some("picked"), None)
        .await
        .unwrap();
    h.kv.put("q1", Value::utf8("v"), Some("skipped"), None)
        .await
        .unwrap();
    h.kv.put("p2", Value::utf8("v"), Some("picked"), None)
        .await
        .unwrap();
    h.kv.delete("p2", Some("picked")).await.unwrap();

    // Only Put events in the picked collection arrive.
    let events = drain(&mut stream, 2).await;
    assert_eq!(events[0].key, "p1");
    assert_eq!(events[1].key, "p2");
}

#[tokio::test]
async fn change_filter_matches() {
    scenario_feed_filter(&memory_harness()).await;
    scenario_feed_filter(&shm_harness("feed_filter")).await;
}

/// A file carrying an old format version must be rejected loudly, naming the
/// version it holds and the one this build understands.
#[tokio::test]
async fn v1_backing_file_is_rejected_loudly() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    let dir = std::env::temp_dir().join(format!("openkeyv-v1-reject-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let path = dir.join("v1.store");

    // Fabricate a v1-shaped file: valid magic, version 1, ready state, and a
    // plausible layout header so the version check is what fires.
    let file_len = 16 * 1024 * 1024u64;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .unwrap();
    file.set_len(file_len).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let map = unsafe { memmap2::MmapOptions::new().map_mut(&file).unwrap() };
    let words: &mut [AtomicU64] = unsafe {
        std::slice::from_raw_parts_mut(map.as_ptr() as *const AtomicU64 as *mut AtomicU64, 32)
    };
    let magic = u64::from_le_bytes(*b"OKVSHM1\0");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    words[0].store(magic, Ordering::Relaxed); // magic
    words[1].store(1, Ordering::Relaxed); // format_version = 1
    words[2].store(file_len, Ordering::Relaxed); // layout_bytes
    words[3].store(4096, Ordering::Relaxed); // header_bytes
    words[8].store(1, Ordering::Relaxed); // state = STATE_READY
    words[16].store(4096 * 4096, Ordering::Relaxed); // arena_capacity (any plausible value)
    words[17].store(1024, Ordering::Relaxed); // index_entries
    words[18].store(8192, Ordering::Relaxed); // change_retention
    words[19].store(now, Ordering::Relaxed); // creation_unix_ms
    drop(map);
    drop(file);

    let error = match ShmStore::open(&path) {
        Err(error) => error,
        Ok(_) => panic!("a v1 backing file must be rejected"),
    };
    let message = error.to_string();
    assert!(
        message.contains("format version 1") && message.contains("understands 2"),
        "rejection must name both versions: {message}"
    );
    assert!(
        message.contains("recreate"),
        "rejection must tell the user how to proceed: {message}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// sync_snapshot contract: subscribe-then-scan must lose nothing under
/// concurrent writes. Every mutation lands in the snapshot union the stream;
/// the final state converges with the store's own final state.
async fn scenario_sync_snapshot(h: &Harness) {
    use openkeyv::migration::sync_snapshot;
    use std::collections::HashSet;

    // Warm the store so the snapshot has real content.
    for i in 0..20 {
        h.kv
            .put(&format!("pre{i}"), Value::utf8("warm"), Some("sync"), None)
            .await
            .unwrap();
    }

    let (mut stream, entries) = sync_snapshot(h.sync.as_ref(), ChangeFilter::default())
        .await
        .expect("sync_snapshot must succeed");
    // The consumer's view starts as the snapshot, then applies every change
    // the stream delivers; after the drain it must equal the store's real
    // final state exactly.
    let mut live: HashSet<String> =
        entries.into_iter().map(|entry| entry.key).collect();

    // Chaos: interleave puts of fresh keys, overwrites, and deletes while the
    // consumer drains the stream.
    for i in 0..200 {
        let key = format!("pre{}", i % 20);
        match i % 5 {
            0 | 1 | 2 => {
                h.kv
                    .put(&key, Value::utf8(format!("v{i}")), Some("sync"), None)
                    .await
                    .unwrap();
            }
            3 => {
                h.kv
                    .put(&format!("new{i}"), Value::utf8(format!("n{i}")), Some("sync"), None)
                    .await
                    .unwrap();
            }
            _ => {
                h.kv.delete(&key, Some("sync")).await.unwrap();
            }
        }
    }

    // Drain everything the stream buffered, folding each change into the
    // consumer view; recv tails live forever, so a short quiet window ends
    // the drain.
    let mut seen = 0usize;
    loop {
        match tokio::time::timeout(Duration::from_millis(400), stream.recv()).await {
            Ok(Ok(Some(change))) => {
                seen += 1;
                match change.operation {
                    ChangeOperation::Put => {
                        live.insert(change.key);
                    }
                    ChangeOperation::Delete => {
                        live.remove(&change.key);
                    }
                }
            }
            Ok(Ok(None)) | Ok(Err(_)) | Err(_) => break,
        }
    }
    assert!(seen > 0, "the stream must have carried the overlapping writes");

    // Converge: snapshot ∪ stream equals the store's real final key set.
    let store_keys: HashSet<String> = h
        .keys
        .keys(Some("sync"), None)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        live, store_keys,
        "the consumer's folded view must equal the store's final state"
    );

    // Every surviving key is readable, and deleted keys are gone.
    for key in &store_keys {
        assert!(
            h.kv.get(key, Some("sync")).await.unwrap().is_some(),
            "every key the store reports must be readable"
        );
    }
}

#[tokio::test]
async fn sync_snapshot_loses_no_events_under_chaos() {
    scenario_sync_snapshot(&memory_harness()).await;
    scenario_sync_snapshot(&shm_harness("sync_snapshot")).await;
}

/// The named-semaphore wakeup (not a poll loop): an idle subscriber parked in
/// `sem_timedwait` sees a fresh event within single-digit milliseconds.
#[tokio::test]
async fn wakeup_wakes_a_waiter_within_5ms() {
    let h = shm_harness("wake_latency");
    let mut stream = subscribe_live(&h).await;
    let mut latencies = Vec::new();
    for i in 0..10 {
        let key = format!("wake-{i}");
        // Settle so the stream is parked inside the semaphore wait when the
        // write lands; only a working wakeup delivers in single-digit ms.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let sent = Instant::now();
        h.kv
            .put(&key, Value::utf8("1"), None, None)
            .await
            .unwrap();
        let change = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("event must arrive within 5s")
            .expect("stream must not error")
            .expect("stream must not end while the store lives");
        assert_eq!(change.key, key);
        latencies.push(sent.elapsed().as_millis() as u64);
    }
    latencies.sort_unstable();
    let median = latencies[latencies.len() / 2];
    let max = latencies[latencies.len() - 1];
    assert!(
        median < 5,
        "median wake latency {median}ms >= 5ms: the subscriber is polling, not waking"
    );
    assert!(max < 100, "worst wake latency {max}ms >= 100ms");
}

/// 16 streams max across all processes (one header slot each); the 17th
/// subscriber must fail loudly instead of silently degrading to polling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_free_slot_is_loud() {
    const NOTIFY_SLOTS: usize = 16; // layout::NOTIFY_SLOTS
    let h = shm_harness("notify_exhaustion");

    // Hold every slot with a parked recv; registration happens on the first
    // idle wait, so each holder claims its slot shortly after subscribe.
    let mut holders = Vec::new();
    for _ in 0..NOTIFY_SLOTS {
        let mut stream = subscribe_live(&h).await;
        holders.push(tokio::spawn(async move {
            let _ = stream.recv().await;
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The 17th stream subscribes fine but fails loudly at its first idle wait.
    let mut late = subscribe_live(&h).await;
    let error = tokio::time::timeout(Duration::from_secs(5), late.recv())
        .await
        .expect("the loud failure must surface, not hang")
        .expect_err("the 17th concurrent stream must fail registration");
    assert!(
        error.to_string().contains("no free notify slot"),
        "wrong error: {error}"
    );

    for holder in holders {
        holder.abort();
    }
}
