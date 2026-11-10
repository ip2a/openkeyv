//! Cross-process tests with real child processes. `current_exe()` re-invokes
//! this test binary filtered to `child_entry`, with every parameter passed
//! through environment variables. Threads never stand in for processes: each
//! scenario below has at least two separate OS processes touching one backing
//! file.
//!
//! The crash scenarios live here too (`shm-test-hooks` gated): a child dies
//! by SIGABRT mid-write, and recovery is driven by a *fresh* `open` — the
//! pre-crash parent handle shares the mmap but never runs recovery.
#![cfg(all(feature = "shm", unix))]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

use openkeyv::change::{ChangeCursor, ChangeFeedRequest, ChangeOperation, ChangeStart};
use openkeyv::error::Error;
use openkeyv::protocol::{AsyncChangeFeed, AsyncCompareAndSwap, AsyncDestroyStore, AsyncKeyValue};
use openkeyv::store::shm::{ShmCreateConfig, ShmStore};
use openkeyv::value::Value;

#[cfg(feature = "shm-test-hooks")]
use std::os::unix::process::ExitStatusExt;

/// SIGABRT on macOS and Linux alike; children that hit a test-hook abort die
/// with this signal.
#[cfg(feature = "shm-test-hooks")]
const SIGABRT: i32 = 6;

/// A store path inside a per-process 0700 directory: the store refuses
/// group/world-writable parents, and `/tmp` on Linux is 1777.
fn store_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("openkeyv-process-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("process test directory must create");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .expect("process test directory permissions must apply");
    }
    dir.join(name)
}

fn child_command(store: &Path, mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "child_entry", "--test-threads=1"])
        .env("OKV_CHILD_MODE", mode)
        .env("OKV_CHILD_STORE", store);
    command
}

fn run_child(store: &Path, mode: &str, extra_env: &[(&str, &str)]) -> ExitStatus {
    let mut command = child_command(store, mode);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("child process must spawn");
    wait_with_timeout(&mut child, 30)
}

fn run_child_quiet(store: &Path, mode: &str, extra_env: &[(&str, &str)]) {
    let status = run_child(store, mode, extra_env);
    assert!(status.success(), "child {mode} failed: {status}");
}

/// Bounded wait so a wedged child fails the test instead of hanging the suite.
fn wait_with_timeout(child: &mut Child, secs: u64) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        match child.try_wait().expect("child status must be readable") {
            Some(status) => return status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child process did not exit within {secs}s");
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn small_config(path: &Path) -> ShmCreateConfig {
    ShmCreateConfig::new(path)
        .with_capacity_bytes(16 * 1024 * 1024)
        .with_index_entries(1_024)
}

/// The child-side entry point. Without `OKV_CHILD_MODE` (i.e. in the parent
/// run) it is a no-op that simply passes.
#[test]
fn child_entry() {
    let Ok(mode) = std::env::var("OKV_CHILD_MODE") else {
        return;
    };
    let store_path = std::env::var("OKV_CHILD_STORE").expect("child needs a store path");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime must build");

    match mode.as_str() {
        "put" => {
            let key = std::env::var("OKV_CHILD_KEY").unwrap();
            let value = std::env::var("OKV_CHILD_VALUE").unwrap();
            let store = ShmStore::open(&store_path).expect("child must open the store");
            runtime
                .block_on(store.put(&key, Value::utf8(&value), None, None))
                .expect("child put must succeed");
        }
        "delete" => {
            let key = std::env::var("OKV_CHILD_KEY").unwrap();
            let store = ShmStore::open(&store_path).expect("child must open the store");
            runtime
                .block_on(store.delete(&key, None))
                .expect("child delete must succeed");
        }
        "put_many" => {
            let prefix = std::env::var("OKV_CHILD_PREFIX").unwrap();
            let count: usize = std::env::var("OKV_CHILD_COUNT").unwrap().parse().unwrap();
            let store = ShmStore::open(&store_path).expect("child must open the store");
            runtime.block_on(async {
                for i in 0..count {
                    store
                        .put(&format!("{prefix}{i}"), Value::utf8(&prefix), None, None)
                        .await
                        .expect("child put_many must succeed");
                }
            });
        }
        "get" => {
            let key = std::env::var("OKV_CHILD_KEY").unwrap();
            let expected = std::env::var("OKV_CHILD_VALUE").unwrap();
            let store = ShmStore::open(&store_path).expect("child must open the store");
            let value = runtime
                .block_on(store.get(&key, None))
                .expect("child get must succeed");
            assert_eq!(
                value,
                Some(Value::utf8(&expected)),
                "child read wrong value"
            );
        }
        "open_only" => {
            let _store = ShmStore::open(&store_path).expect("child must open the store");
        }
        "create_should_fail" => {
            let error = match ShmStore::create(small_config(Path::new(&store_path))) {
                Err(error) => error,
                Ok(_) => panic!("create must fail while another process holds the store"),
            };
            assert!(
                matches!(error, Error::StoreInUse { .. }),
                "expected StoreInUse, got: {error}"
            );
        }
        "cas" => {
            let value = std::env::var("OKV_CHILD_VALUE").unwrap();
            let store = ShmStore::open(&store_path).expect("child must open the store");
            let outcome = runtime
                .block_on(store.compare_and_swap("race", None, Value::utf8(&value), None, None))
                .expect("child cas must succeed");
            println!("VALUE={value}");
            println!("CAS={outcome:?}");
        }
        "hold_attach" => {
            let store = ShmStore::open(&store_path).expect("child must open the store");
            runtime
                .block_on(store.put("attached", Value::utf8("1"), None, None))
                .expect("child marker put must succeed");
            std::thread::sleep(Duration::from_secs(60));
        }
        "watch_destroy" => {
            let store = ShmStore::open(&store_path).expect("child must open the store");
            runtime
                .block_on(store.put("watching", Value::utf8("1"), None, None))
                .expect("child marker put must succeed");
            let mut stream = runtime
                .block_on(store.subscribe(ChangeFeedRequest {
                    start: ChangeStart::Latest,
                    filter: Default::default(),
                }))
                .expect("child subscribe must succeed");
            runtime.block_on(async {
                let ended = tokio::time::timeout(Duration::from_secs(10), stream.recv())
                    .await
                    .expect("child stream recv timed out")
                    .expect("stream must not error");
                assert!(
                    ended.is_none(),
                    "destroy must end the stream, got {ended:?}"
                );
            });
            println!("WATCH_ENDED");
        }
        other => panic!("unknown child mode: {other}"),
    }
}

#[tokio::test]
async fn cross_process_read_write_and_feed() {
    let path = store_path("rw_feed");
    let store = ShmStore::create(small_config(&path)).expect("store must create");

    store
        .put("hello", Value::utf8("parent"), None, None)
        .await
        .unwrap();

    // A fresh process reads the parent's write.
    run_child_quiet(
        &path,
        "get",
        &[("OKV_CHILD_KEY", "hello"), ("OKV_CHILD_VALUE", "parent")],
    );

    // A fresh process writes; the parent reads it back.
    run_child_quiet(
        &path,
        "put",
        &[
            ("OKV_CHILD_KEY", "from-child"),
            ("OKV_CHILD_VALUE", "child"),
        ],
    );
    assert_eq!(
        store.get("from-child", None).await.unwrap(),
        Some(Value::utf8("child"))
    );

    // Events produced by the child arrive on the parent's subscription.
    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Latest,
            filter: Default::default(),
        })
        .await
        .unwrap();
    run_child_quiet(
        &path,
        "put",
        &[("OKV_CHILD_KEY", "fed"), ("OKV_CHILD_VALUE", "1")],
    );
    let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .expect("cross-process event must arrive within 5s")
        .expect("stream must not error")
        .expect("stream must not end while the store lives");
    assert_eq!(event.key, "fed");
    assert!(matches!(event.operation, ChangeOperation::Put));

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn cross_process_feed_replay_lag_and_destroy() {
    // Replay across processes: writes from children, read back in order by a
    // Beginning subscription.
    let path = store_path("feed_replay");
    let store = ShmStore::create(small_config(&path).with_change_retention(1_000))
        .expect("store must create");
    run_child_quiet(
        &path,
        "put",
        &[("OKV_CHILD_KEY", "k1"), ("OKV_CHILD_VALUE", "1")],
    );
    run_child_quiet(
        &path,
        "put",
        &[("OKV_CHILD_KEY", "k2"), ("OKV_CHILD_VALUE", "2")],
    );
    run_child_quiet(
        &path,
        "put",
        &[("OKV_CHILD_KEY", "k3"), ("OKV_CHILD_VALUE", "3")],
    );

    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Beginning,
            filter: Default::default(),
        })
        .await
        .unwrap();
    let mut keys = Vec::new();
    for _ in 0..3 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("replay event must arrive within 5s")
            .expect("stream must not error")
            .expect("stream must not end");
        keys.push(event.key);
    }
    assert_eq!(keys, vec!["k1", "k2", "k3"], "cross-process replay order");
    drop(store);
    let _ = std::fs::remove_file(&path);

    // Lag: a retention smaller than the written history expires old cursors,
    // and Beginning replays only what is retained.
    let path = store_path("feed_lag");
    let store =
        ShmStore::create(small_config(&path).with_change_retention(4)).expect("store must create");
    run_child_quiet(
        &path,
        "put_many",
        &[("OKV_CHILD_PREFIX", "m"), ("OKV_CHILD_COUNT", "6")],
    );
    let expired = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::After(ChangeCursor::new("1")),
            filter: Default::default(),
        })
        .await;
    assert!(
        expired.is_err(),
        "cursor below the retention window must fail"
    );

    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Beginning,
            filter: Default::default(),
        })
        .await
        .unwrap();
    let mut keys = Vec::new();
    for _ in 0..4 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("retained event must arrive within 5s")
            .expect("stream must not error")
            .expect("stream must not end");
        keys.push(event.key);
    }
    assert_eq!(
        keys,
        vec!["m2", "m3", "m4", "m5"],
        "Beginning replays the retained window"
    );

    // Destroy ends streams in other processes too: a child watches, the
    // parent destroys, the child's stream ends.
    let mut child = child_command(&path, "watch_destroy")
        .spawn()
        .expect("watch child must spawn");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if store.get("watching", None).await.unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "child never attached its watch");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(store.destroy().await.unwrap(), "destroy must succeed");
    let status = wait_with_timeout(&mut child, 15);
    assert!(status.success(), "watch child failed: {status}");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn cross_process_cas_has_exactly_one_winner() {
    let path = store_path("cas_race");
    let store = ShmStore::create(small_config(&path)).expect("store must create");

    let mut children = Vec::new();
    for i in 0..4 {
        let mut command = child_command(&path, "cas");
        command.env("OKV_CHILD_VALUE", format!("w{i}"));
        children.push(command.spawn().expect("cas child must spawn"));
    }
    for child in &mut children {
        let status = wait_with_timeout(child, 30);
        assert!(status.success(), "cas child failed: {status}");
    }

    // Four racing create-if-absent CAS calls: exactly one child's value won.
    let final_value = store
        .get("race", None)
        .await
        .unwrap()
        .expect("the race key must exist");
    let winners = (0..4)
        .filter(|i| final_value == Value::utf8(format!("w{i}")))
        .count();
    assert_eq!(winners, 1, "exactly one child's value must have won");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn store_in_use_and_recreate_after_kill() {
    let path = store_path("in_use");
    let store = ShmStore::create(small_config(&path)).expect("store must create");
    drop(store);

    // A child holds the store attached (its marker put proves it is in).
    let mut child = child_command(&path, "hold_attach")
        .spawn()
        .expect("hold child must spawn");
    let deadline = Instant::now() + Duration::from_secs(10);
    let store = loop {
        match ShmStore::open(&path) {
            Ok(store) => break store,
            Err(error) => {
                assert!(Instant::now() < deadline, "open kept failing: {error}");
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };
    loop {
        if store.get("attached", None).await.unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "child never attached");
        std::thread::sleep(Duration::from_millis(20));
    }

    // Another process may open, but must not create/reset.
    run_child_quiet(&path, "open_only", &[]);
    run_child_quiet(&path, "create_should_fail", &[]);

    // The OS frees the locks on process death: after a kill, create works.
    let _ = child.kill();
    let _ = child.wait();
    drop(store); // release this process's own shared attach before asking for exclusive
    let recreated =
        ShmStore::create(small_config(&path)).expect("create must succeed after the holder dies");
    recreated
        .put("after-kill", Value::utf8("1"), None, None)
        .await
        .unwrap();
    assert_eq!(
        recreated.get("after-kill", None).await.unwrap(),
        Some(Value::utf8("1"))
    );
    let _ = std::fs::remove_file(&path);
}

/// After a crash the pre-crash parent handle still sees the mmap, but only a
/// fresh `open` runs recovery; every crash assertion below reads through the
/// recovered handle.
async fn recovered(path: &Path) -> ShmStore {
    ShmStore::open(path).expect("reopen after crash must succeed")
}

#[tokio::test]
#[cfg(feature = "shm-test-hooks")]
async fn writer_abort_at_prepared_rolls_back() {
    let path = store_path("crash_prepared");
    let store = ShmStore::create(small_config(&path).with_change_retention(1_000))
        .expect("store must create");
    store
        .put("seed", Value::utf8("0"), None, None)
        .await
        .unwrap();

    let status = run_child(
        &path,
        "put",
        &[
            ("OKV_CHILD_KEY", "victim"),
            ("OKV_CHILD_VALUE", "v"),
            ("OKV_SHM_ABORT_AT", "prepared"),
        ],
    );
    assert_eq!(status.signal(), Some(SIGABRT), "child must die by SIGABRT");

    let store = recovered(&path).await;
    // Rollback: the write never became visible and emitted no event.
    assert_eq!(store.get("victim", None).await.unwrap(), None);
    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Beginning,
            filter: Default::default(),
        })
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .expect("seed event must replay")
        .expect("stream must not error")
        .expect("stream must not end");
    assert_eq!(event.key, "seed");

    // The recovered store accepts fresh writes.
    store
        .put("after", Value::utf8("1"), None, None)
        .await
        .unwrap();
    assert_eq!(
        store.get("after", None).await.unwrap(),
        Some(Value::utf8("1"))
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(feature = "shm-test-hooks")]
async fn writer_abort_at_published_keeps_write_and_backfills_one_event() {
    let path = store_path("crash_published");
    let store = ShmStore::create(small_config(&path).with_change_retention(1_000))
        .expect("store must create");
    store
        .put("seed", Value::utf8("0"), None, None)
        .await
        .unwrap();

    let status = run_child(
        &path,
        "put",
        &[
            ("OKV_CHILD_KEY", "victim"),
            ("OKV_CHILD_VALUE", "v"),
            ("OKV_SHM_ABORT_AT", "published"),
        ],
    );
    assert_eq!(status.signal(), Some(SIGABRT), "child must die by SIGABRT");

    let store = recovered(&path).await;
    // The published write survives recovery exactly once, with one event.
    assert_eq!(
        store.get("victim", None).await.unwrap(),
        Some(Value::utf8("v"))
    );
    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Beginning,
            filter: Default::default(),
        })
        .await
        .unwrap();
    let mut keys = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("events must replay")
            .expect("stream must not error")
            .expect("stream must not end");
        keys.push(event.key);
    }
    assert_eq!(keys, vec!["seed", "victim"], "exactly one event per write");

    store
        .put("after", Value::utf8("1"), None, None)
        .await
        .unwrap();
    assert_eq!(
        store.get("after", None).await.unwrap(),
        Some(Value::utf8("1"))
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(feature = "shm-test-hooks")]
async fn writer_abort_at_appended_does_not_duplicate_event() {
    let path = store_path("crash_appended");
    let store = ShmStore::create(small_config(&path).with_change_retention(1_000))
        .expect("store must create");
    store
        .put("seed", Value::utf8("0"), None, None)
        .await
        .unwrap();

    let status = run_child(
        &path,
        "put",
        &[
            ("OKV_CHILD_KEY", "victim"),
            ("OKV_CHILD_VALUE", "v"),
            ("OKV_SHM_ABORT_AT", "appended"),
        ],
    );
    assert_eq!(status.signal(), Some(SIGABRT), "child must die by SIGABRT");

    let store = recovered(&path).await;
    assert_eq!(
        store.get("victim", None).await.unwrap(),
        Some(Value::utf8("v"))
    );
    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Beginning,
            filter: Default::default(),
        })
        .await
        .unwrap();
    let mut keys = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("events must replay")
            .expect("stream must not error")
            .expect("stream must not end");
        keys.push(event.key);
    }
    assert_eq!(
        keys,
        vec!["seed", "victim"],
        "the appended event must not duplicate"
    );

    store
        .put("after", Value::utf8("1"), None, None)
        .await
        .unwrap();
    assert_eq!(
        store.get("after", None).await.unwrap(),
        Some(Value::utf8("1"))
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(feature = "shm-test-hooks")]
async fn writer_abort_at_published_delete_is_completed_once() {
    let path = store_path("crash_delete");
    let store = ShmStore::create(small_config(&path).with_change_retention(1_000))
        .expect("store must create");
    store
        .put("victim", Value::utf8("v"), None, None)
        .await
        .unwrap();

    let status = run_child(
        &path,
        "delete",
        &[
            ("OKV_CHILD_KEY", "victim"),
            ("OKV_SHM_ABORT_AT", "published"),
        ],
    );
    assert_eq!(status.signal(), Some(SIGABRT), "child must die by SIGABRT");

    let store = recovered(&path).await;
    // The delete survives: the key is gone and exactly one Delete event exists.
    assert_eq!(store.get("victim", None).await.unwrap(), None);
    let mut stream = store
        .subscribe(ChangeFeedRequest {
            start: ChangeStart::Beginning,
            filter: Default::default(),
        })
        .await
        .unwrap();
    let mut events = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("events must replay")
            .expect("stream must not error")
            .expect("stream must not end");
        events.push(format!("{:?}/{}", event.operation, event.key));
    }
    assert_eq!(
        events,
        vec!["Put/victim", "Delete/victim"],
        "delete recovery must emit exactly one Delete"
    );
    let _ = std::fs::remove_file(&path);
}
