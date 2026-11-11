//! Engine statistics and compaction contract for the shm store.
//!
//! Structural note baked into these tests: the arena right-sizes every write
//! (alloc always picks the minimal class for the payload, splitting larger
//! free blocks downward), so a live entry never sits in an oversized block.
//! Compaction therefore finds nothing to rewrite on a healthy arena — the
//! tests pin that invariant instead of assuming the oversized case exists.

#![cfg(unix)]

use std::time::Duration;

use openkeyv::protocol::{AsyncCull, AsyncKeyValue, AsyncStats};
use openkeyv::store::memory::MemoryStore;
use openkeyv::store::shm::{ShmCreateConfig, ShmStore};
use openkeyv::value::Value;

fn shm_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("openkeyv-stats-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("stats test directory must create");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .expect("stats test directory permissions must apply");
    }
    dir.join(name)
}

/// Counters must reconcile with hand math: live entries, payload bytes,
/// tombstones, expiry totals, and the change-revision stream.
#[tokio::test]
async fn stats_reconciles_with_header() {
    let store = ShmStore::create(
        ShmCreateConfig::new(shm_dir("reconcile.shm"))
            .with_capacity_bytes(16 * 1024 * 1024)
            .with_index_entries(1_024)
            .with_change_retention(4_096),
    )
    .expect("store must create");

    let count = 100u32;
    for i in 0..count {
        store
            .put(
                &format!("k{i}"),
                Value::utf8(format!("val{i}")),
                Some("c"),
                // TTL on the tail keys only: they must survive the explicit
                // delete range below so the sweeper actually culls them.
                if i >= 90 { Some(0.01) } else { None },
            )
            .await
            .expect("put must succeed");
    }
    for i in 0..30u32 {
        assert!(
            store.delete(&format!("k{i}"), Some("c")).await.expect("delete"),
            "delete {i} must land"
        );
    }

    // The 10 TTL entries expire and the sweeper culls them (25ms tick).
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stats = store.stats().await.expect("stats must work");
    assert!(stats.available, "shm stats must be available");

    // 100 puts, 30 explicit deletes, 10 sweeper culls, +1 registry entry.
    assert_eq!(
        stats.live_entries, 61,
        "100 - 30 deleted - 10 expired + 1 registry"
    );
    // Every dead entry left exactly one tombstone except those the purge
    // flights already reclaimed; the counter is live-table truth.
    assert_eq!(
        stats.tombstones, 40,
        "30 deletes + 10 culls, no purge flight at this load"
    );
    // Free-list reconciliation: every freed block sits in exactly one size
    // class. All entries in this test are class 0 (256B), so the free list
    // must hold exactly the 40 dead blocks.
    assert_eq!(
        stats.free_by_class,
        vec![(0u8, 40u64)],
        "30 deletes + 10 culls of 256B blocks"
    );
    assert!(
        stats.expired_total >= 10,
        "the sweeper culls must count, got {}",
        stats.expired_total
    );

    // live_payload_bytes must equal the sum of payload lengths over the live
    // set (collection "c" + key + value bytes), registry included.
    let mut expected_payload = "c".len() as u64; // registry entry payload
    for i in 30..90 {
        expected_payload += ("c".len() + format!("k{i}").len() + format!("val{i}").len()) as u64;
    }
    assert_eq!(
        stats.live_payload_bytes, expected_payload,
        "payload accounting must be exact"
    );

    // One change event per visible mutation: 100 puts + 30 deletes + 10
    // Expired culls. The registry does not emit events.
    assert_eq!(
        stats.change_revision, 140,
        "every mutation appends exactly one change event"
    );

    assert!(stats.arena_capacity > 0, "arena must report its capacity");
    assert!(
        stats.arena_high_water >= stats.live_payload_bytes,
        "high water bounds the live payload"
    );
    assert!(
        stats.index_entries >= 1_024 && stats.index_entries.is_power_of_two(),
        "index size is a power of two at or above the configured floor, got {}",
        stats.index_entries
    );
}

/// The arena self-compacts on write: overwriting big values with small ones
/// right-sizes the new blocks (split-on-reuse), so a follow-up compaction
/// pass has nothing oversized to rewrite, returns 0, and the data survives.
#[tokio::test]
async fn compact_moves_oversized_blocks() {
    let store = ShmStore::create(
        ShmCreateConfig::new(shm_dir("compact.shm"))
            .with_capacity_bytes(16 * 1024 * 1024)
            .with_index_entries(1_024)
            .with_change_retention(4_096),
    )
    .expect("store must create");

    let count = 50u32;
    let big = "x".repeat(8 * 1024);
    for i in 0..count {
        store
            .put(&format!("k{i}"), Value::utf8(big.as_str()), Some("c"), None)
            .await
            .expect("big put must succeed");
    }
    let after_big = store.stats().await.expect("stats must work");
    let big_class = after_big
        .free_by_class
        .iter()
        .find(|(class, _)| 1u64 << (class + 8) == 8 * 1024 + 256)
        .copied();
    assert!(
        after_big.arena_high_water >= count as u64 * 8 * 1024,
        "big writes must lift the high water"
    );

    for i in 0..count {
        store
            .put(&format!("k{i}"), Value::utf8("s"), Some("c"), None)
            .await
            .expect("small overwrite must succeed");
    }
    let after_small = store.stats().await.expect("stats must work");
    assert!(
        after_small.arena_high_water >= after_big.arena_high_water,
        "high water never shrinks (documented ceiling)"
    );
    assert!(
        after_small.arena_high_water - after_big.arena_high_water <= 256,
        "the only lift is the first overwrite bumping one small block before          the first big block is freed for reuse"
    );

    let rewritten = store.compact(u64::MAX).await.expect("compact must work");
    assert_eq!(
        rewritten, 0,
        "every live entry is already in its minimal class"
    );

    // The freed big blocks came back as split-down buddies, not as their
    // original class.
    if let Some((class, _)) = big_class {
        let after_compact = store.stats().await.expect("stats must work");
        let remaining = after_compact
            .free_by_class
            .iter()
            .find(|(c, _)| *c == class)
            .map(|(_, n)| *n)
            .unwrap_or(0);
        assert!(
            remaining < count as u64,
            "the big class must not re-accumulate wholesale"
        );
    }

    // Data intact after the pass.
    for i in 0..count {
        assert_eq!(
            store
                .get(&format!("k{i}"), Some("c"))
                .await
                .expect("get must work"),
            Some(Value::utf8("s")),
            "k{i} must survive compaction unchanged"
        );
    }
}

/// Engines without block-level allocation keep the trait default: a no-op.
#[tokio::test]
async fn compact_default_is_noop() {
    let memory = MemoryStore::new();
    let rewritten = memory.compact(1_000_000).await.expect("compact must work");
    assert_eq!(rewritten, 0, "memory store has nothing to compact");

    let stats = memory.stats().await.expect("stats must work");
    assert!(!stats.available, "memory stats are explicitly unavailable");
}
