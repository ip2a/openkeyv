//! Online index growth contract: the incremental rehash must never show a
//! reader a false absence, the file must never grow, and the only capacity
//! boundary a client can hit must fail loud. These tests are shm-only (they
//! assert rehash state, not differential behavior) and deliberately churn far
//! harder than the differential suite: the false-absent class of bug needs
//! live probe chains crossing a mid-flight cursor.

#![cfg(unix)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openkeyv::error::Error;
use openkeyv::migration::{copy_snapshot_with_feed, MigrationOptions};
use openkeyv::protocol::{AsyncDestroyStore, AsyncEnumerateKeys, AsyncKeyValue};
use openkeyv::store::shm::{ShmCreateConfig, ShmStore};
use openkeyv::value::Value;

/// A store path inside a per-process 0700 directory: the store refuses
/// group/world-writable parents, and `/tmp` on Linux is 1777.
fn store_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("openkeyv-rehash-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("rehash test directory must create");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .expect("rehash test directory permissions must apply");
    }
    dir
}

/// The reader keeps polling a hot key set while the writer floods enough
/// entries to drive several expansions through the incremental rehash. Every
/// read of a live key must return Some; no read may ever error (a reader
/// that cannot stabilize sees Unstable surfaced as an error after retries).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rehash_reader_never_sees_false_absent() {
    let path = store_dir().join("flood.shm");
    let store = Arc::new(
        ShmStore::create(
            ShmCreateConfig::new(&path)
                .with_capacity_bytes(64 * 1024 * 1024)
                .with_index_entries(1_024)
                .with_change_retention(4_096),
        )
        .expect("store must create"),
    );

    // Hot set lives before the flood so the reader races the rehash, not
    // the insert.
    for i in 0..100u32 {
        store
            .put(&format!("hot{i}"), Value::utf8("h"), Some("c"), None)
            .await
            .expect("hot put must succeed");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let store = store.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut rounds = 0u64;
            let mut failures = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                for i in 0..100u32 {
                    match store.get(&format!("hot{i}"), Some("c")).await {
                        Ok(Some(_)) => {}
                        Ok(None) => failures.push(format!("hot{i} absent")),
                        Err(e) => failures.push(format!("hot{i} error {e:?}")),
                    }
                }
                rounds += 1;
            }
            (rounds, failures)
        })
    };

    // 50k entries across an index that starts at 1024 slots: the flood must
    // cross several expand thresholds (1024 → … → 65536+).
    let flooded = 50_000u32;
    for i in 0..flooded {
        store
            .put(&format!("k{i}"), Value::utf8("v"), Some("c"), None)
            .await
            .expect("flood put must succeed");
    }

    stop.store(true, Ordering::Relaxed);
    let (rounds, failures) = reader.await.expect("reader task must join");
    assert!(
        rounds >= 100,
        "reader must have actually polled during the flood, got {rounds} rounds"
    );
    assert!(
        failures.is_empty(),
        "reader saw {} false absences/errors: {:?}",
        failures.len(),
        &failures[..failures.len().min(8)]
    );
}

/// Expansion and shrink both happen in place: the file length never moves,
/// the index walks up under load and back down under churn, and the data
/// survives both directions. Needs the debug state accessor (`shm-test-hooks`).
#[cfg(feature = "shm-test-hooks")]
#[tokio::test]
async fn index_expands_and_shrinks_online() {
    let path = store_dir().join("grow-shrink.shm");
    let store = Arc::new(
        ShmStore::create(
            ShmCreateConfig::new(&path)
                .with_capacity_bytes(64 * 1024 * 1024)
                .with_index_entries(1_024)
                .with_change_retention(4_096),
        )
        .expect("store must create"),
    );
    let file_len = std::fs::metadata(&path).expect("backing file must exist").len();

    let count = 40_000u32;
    let mut max_index = 0u64;
    for i in 0..count {
        store
            .put(&format!("k{i}"), Value::utf8("v"), Some("c"), None)
            .await
            .expect("put must succeed");
        if i % 500 == 0 {
            let (index, _, _, _) = store.shm_debug_index_state();
            max_index = max_index.max(index);
        }
    }
    let (_, live_after_put, tombstones_after_put, _) = store.shm_debug_index_state();
    assert_eq!(live_after_put, count as u64 + 1, "40k puts plus the registry");
    assert_eq!(tombstones_after_put, 0, "pure growth leaves no tombstones");
    assert!(
        max_index >= 32_768,
        "40k entries must have expanded the index well past 1024, peak sample {max_index}"
    );

    let peak_index = store.shm_debug_index_state().0;
    for i in 0..(count - 500) {
        let removed = store
            .delete(&format!("k{i}"), Some("c"))
            .await
            .expect("delete must apply");
        assert!(removed, "remove {i} must land");
        if i % 500 == 0 {
            std::hint::black_box(store.shm_debug_index_state());
        }
    }
    // Flights advance only through mutations (64 buckets each): drive the
    // trailing shrink flights to completion with throwaway churn.
    // Delete-then-put: the loop ends on an insert (the insert reuses the
    // tombstone slot), so settling leaves no dangling tombstone behind.
    for _ in 0..5_000 {
        store.delete("probe", Some("c")).await.expect("probe delete");
        store
            .put("probe", Value::utf8("p"), Some("c"), None)
            .await
            .expect("probe put must succeed");
        if store.shm_debug_index_state().3 == u64::MAX {
            break;
        }
    }
    let (final_index, _, _, final_cursor) = store.shm_debug_index_state();
    assert_eq!(
        final_cursor,
        u64::MAX,
        "rehash must be idle after the churn settles"
    );
    assert_eq!(
        store.shm_debug_index_state().2,
        0,
        "slot reuse must keep the tombstone counter exact"
    );
    store.delete("probe", Some("c")).await.expect("final delete");
    let (final_index, final_live, final_tombstones, _) = store.shm_debug_index_state();
    assert_eq!(final_live, 501, "500 survivors plus the registry");
    assert_eq!(final_tombstones, 1, "one delete leaves exactly one tombstone");
    assert!(
        final_index < peak_index,
        "index must shrink online: peak {peak_index}, final {final_index}"
    );
    assert!(
        std::fs::metadata(&path).expect("backing file must exist").len() == file_len,
        "file length must never change"
    );

    // Every survivor still hash-locates through the shrunk table.
    for i in (count - 500)..count {
        assert!(
            store
                .get(&format!("k{i}"), Some("c"))
                .await
                .expect("get must work")
                .is_some(),
            "k{i} must survive the churn"
        );
    }
}

/// The only capacity boundary a client can hit is true exhaustion: the arena
/// runs out and the next insert fails loud with a numeric StoreCapacity.
/// (The index-side 90% guard cannot fire first: minimum blocks are 256B, so
/// the arena always saturates long before 90% of the index ceiling — the
/// guard stays as belt-and-braces behind the same loud error.)
#[tokio::test]
async fn capacity_error_is_loud_at_true_exhaustion() {
    let path = store_dir().join("cap.shm");
    let store = Arc::new(
        ShmStore::create(
            ShmCreateConfig::new(&path)
                .with_capacity_bytes(16 * 1024 * 1024)
                .with_index_entries(1_024)
                .with_change_retention(1_024),
        )
        .expect("store must create"),
    );

    let mut inserts = 0u64;
    let first_rejection = loop {
        match store
            .put(&format!("k{inserts}"), Value::utf8("v"), Some("c"), None)
            .await
        {
            Ok(_) => inserts += 1,
            Err(e) => break e,
        }
    };
    assert!(
        matches!(first_rejection, Error::StoreCapacity { .. }),
        "exhaustion must fail loud with StoreCapacity, got {first_rejection:?}"
    );
    // 16MiB reserves both tables at the 131072-slot ceiling (4MiB), leaving
    // ~12MiB of arena; 256B minimum blocks put the wall around 48k entries.
    assert!(
        (30_000..60_000).contains(&inserts),
        "the wall must sit at the arena boundary, got {inserts} inserts"
    );
    // Data before the wall is intact and readable after the loud failure.
    assert!(
        store
            .get("k0", Some("c"))
            .await
            .expect("get must work")
            .is_some(),
        "entries below the wall must survive the failed insert"
    );
}

/// The documented grow recipe: a store too small for its workload migrates
/// into a bigger file via copy_snapshot_with_feed, then reconciles. The
/// target's index grows online during the bulk load — no presized index
/// needed.
#[tokio::test]
async fn grow_via_snapshot_migration() {
    let dir = store_dir();
    let source_path = dir.join("small.shm");
    let target_path = dir.join("big.shm");
    let source = Arc::new(
        ShmStore::create(
            ShmCreateConfig::new(&source_path)
                .with_capacity_bytes(16 * 1024 * 1024)
                .with_index_entries(1_024)
                .with_change_retention(4_096),
        )
        .expect("source store must create"),
    );

    let count = 5_000u32;
    for i in 0..count {
        source
            .put(&format!("k{i}"), Value::utf8(format!("val{i}")), Some("c"), None)
            .await
            .expect("seed put must succeed");
    }

    let target = Arc::new(
        ShmStore::create(
            ShmCreateConfig::new(&target_path)
                .with_capacity_bytes(64 * 1024 * 1024)
                .with_index_entries(1_024)
                .with_change_retention(4_096),
        )
        .expect("target store must create"),
    );
    let (report, mut subscription) =
        copy_snapshot_with_feed(source.as_ref(), target.as_ref(), &MigrationOptions::default())
            .await
            .expect("migration must succeed");
    assert_eq!(report.copied, count as u64, "every entry must migrate");

    // Spot-check values survived byte-identical, then count keys.
    for i in (0..count).step_by(97) {
        let got = target
            .get(&format!("k{i}"), Some("c"))
            .await
            .expect("target get must work");
        assert_eq!(
            got.expect("migrated key must exist"),
            Value::utf8(format!("val{i}")),
            "value for k{i} must match"
        );
    }
    let keys = target
        .keys(Some("c"), None)
        .await
        .expect("enumerate must work");
    assert_eq!(keys.len(), count as usize, "target must hold every key");

    // The live feed stayed open across the copy: a post-migration write
    // surfaces on the subscription exactly once.
    source
        .put("late", Value::utf8("l"), Some("c"), None)
        .await
        .expect("late put must succeed");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_late = false;
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the post-migration change"
        );
        match tokio::time::timeout(Duration::from_millis(500), subscription.recv()).await {
            Ok(Ok(Some(change))) if change.key == "late" => {
                saw_late = true;
                break;
            }
            Ok(Ok(Some(_))) => continue,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("feed error {e:?}"),
            Err(_) => break,
        }
    }
    assert!(saw_late, "post-migration write must surface on the live feed");

    source.destroy().await.expect("source destroy must succeed");
    target.destroy().await.expect("target destroy must succeed");
}
