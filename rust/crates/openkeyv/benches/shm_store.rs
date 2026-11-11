//! Benchmarks for the SHM store: warmed-up single-process read/write/CAS/
//! batch, plus two-process workloads where a real helper process reads the
//! same store concurrently.
#![cfg(unix)]

//!
//! Run with: cargo bench --bench shm_store --features shm
//!
//! The two-process benchmarks re-invoke this binary with `OKV_BENCH_CHILD=
//! reader`; the child hammers reads on the same key pool until the parent
//! kills it. Stores are created and seeded outside the measured iterations.
//!
//! Redis comparison evidence (acceptance thresholds) is collected separately
//! with redis-benchmark over a Unix socket and stored next to this crate's
//! benchmark reports.

use std::cell::Cell;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput};
use openkeyv::protocol::{AsyncCompareAndSwap, AsyncKeyValue};
use openkeyv::store::shm::{ShmCreateConfig, ShmStore};
use openkeyv::value::Value;

/// Key pool per value size; large enough that two processes interleave, small
/// enough to stay hot in every cache that matters.
const POOL: usize = 64;

/// (label, value) pairs covering the acceptance sizes.
fn workloads() -> Vec<(&'static str, Value)> {
    vec![
        ("64b", Value::utf8("x".repeat(64))),
        ("1k", Value::utf8("x".repeat(1024))),
        ("64k", Value::binary(vec![0xa5u8; 64 * 1024])),
        ("1m", Value::binary(vec![0xa5u8; 1024 * 1024])),
    ]
}

/// A store path inside a per-process 0700 directory: the store refuses
/// group/world-writable parents, and `/tmp` on Linux is 1777.
fn bench_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("openkeyv-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("bench directory must create");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .expect("bench directory permissions must apply");
    }
    dir.join(name)
}

fn default_config(path: &PathBuf) -> ShmCreateConfig {
    ShmCreateConfig::new(path)
}

fn seed_pool(store: &ShmStore, prefix: &str, value: &Value) -> tokio::runtime::Runtime {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        for i in 0..POOL {
            store
                .put(&format!("{prefix}-{i}"), value.clone(), None, None)
                .await
                .unwrap();
        }
    });
    rt
}

fn spawn_reader(path: &PathBuf, prefix: &str) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .env("OKV_BENCH_CHILD", "reader")
        .env("OKV_BENCH_STORE", path)
        .env("OKV_BENCH_PREFIX", prefix)
        .env("OKV_BENCH_POOL", POOL.to_string())
        .env("OKV_BENCH_SECONDS", "600")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("reader child must spawn")
}

fn stop_reader(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// The helper process body: open the store and read the key pool in a tight
/// loop until the deadline.
fn reader_child() {
    let store_path = std::env::var("OKV_BENCH_STORE").expect("child needs a store path");
    let prefix = std::env::var("OKV_BENCH_PREFIX").expect("child needs a key prefix");
    let pool: usize = std::env::var("OKV_BENCH_POOL").unwrap().parse().unwrap();
    let seconds: u64 = std::env::var("OKV_BENCH_SECONDS").unwrap().parse().unwrap();

    let store = ShmStore::open(&store_path).expect("reader must open the store");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut i = 0usize;
    rt.block_on(async {
        while Instant::now() < deadline {
            let key = format!("{prefix}-{}", i % pool);
            let _ = store.get(&key, None).await.unwrap();
            i += 1;
        }
    });
}

fn bench_single(c: &mut Criterion) {
    for (label, value) in workloads() {
        let path = bench_path(&format!("single-{label}"));
        let store = ShmStore::create(default_config(&path)).expect("store must create");
        let rt = seed_pool(&store, label, &value);

        let mut group = c.benchmark_group("shm_read");
        group.throughput(Throughput::Elements(1));
        let cursor = Cell::new(0usize);
        group.bench_with_input(BenchmarkId::new("get_1p", label), &(), |b, &_| {
            b.to_async(&rt).iter(|| async {
                let key = format!("{label}-{}", cursor.get() % POOL);
                cursor.set(cursor.get() + 1);
                let _ = store.get(&key, None).await.unwrap();
            });
        });
        group.finish();

        let mut group = c.benchmark_group("shm_write");
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("put_1p", label), &(), |b, &_| {
            b.to_async(&rt).iter(|| async {
                store
                    .put(&format!("{label}-0"), value.clone(), None, None)
                    .await
                    .unwrap();
            });
        });
        group.finish();

        // Feed-off reference point: isolates the change-ring cost of a write.
        if label == "64b" {
            let path = bench_path("single-nofeed");
            let store = ShmStore::create(ShmCreateConfig::new(&path).with_change_retention(0))
                .expect("store must create");
            let mut group = c.benchmark_group("shm_write");
            group.throughput(Throughput::Elements(1));
            group.bench_function("put_1p_nofeed", |b| {
                b.to_async(&rt).iter(|| async {
                    store
                        .put("bench-64b-0", value.clone(), None, None)
                        .await
                        .unwrap();
                });
            });
            group.finish();
            drop(store);
            let _ = std::fs::remove_file(&path);
        }

        if label == "64b" {
            let mut group = c.benchmark_group("shm_cas");
            group.throughput(Throughput::Elements(1));
            group.bench_function("update_1p", |b| {
                b.to_async(&rt).iter(|| async {
                    let observed = store
                        .get_with_revision(&format!("{label}-0"), None)
                        .await
                        .unwrap()
                        .unwrap();
                    let outcome = store
                        .compare_and_swap(
                            &format!("{label}-0"),
                            Some(&observed.revision),
                            value.clone(),
                            None,
                            None,
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        outcome,
                        openkeyv::CompareAndSwapResult::Applied { .. }
                    ));
                });
            });
            group.finish();
        }

        drop(rt);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }
}

fn bench_batch(c: &mut Criterion) {
    let path = bench_path("batch");
    let store = ShmStore::create(default_config(&path)).expect("store must create");
    let value = Value::utf8("x".repeat(64));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for batch_size in [10, 100, 1000] {
        let keys: Vec<String> = (0..batch_size).map(|i| format!("batch-{i}")).collect();
        rt.block_on(store.put_many(&keys, &vec![value.clone(); batch_size], None, None))
            .unwrap();

        let mut group = c.benchmark_group("shm_batch");
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(BenchmarkId::new("put_many", batch_size), &(), |b, &_| {
            b.to_async(&rt).iter(|| async {
                store
                    .put_many(&keys, &vec![value.clone(); batch_size], None, None)
                    .await
                    .unwrap();
            });
        });
        group.bench_with_input(BenchmarkId::new("get_many", batch_size), &(), |b, &_| {
            b.to_async(&rt).iter(|| async {
                let _ = store.get_many(&keys, None).await.unwrap();
            });
        });
        group.finish();
    }

    drop(rt);
    drop(store);
    let _ = std::fs::remove_file(&path);
}

fn bench_two_process(c: &mut Criterion) {
    let sizes: Vec<(&'static str, Value)> = workloads();

    for (label, value) in &sizes {
        let path = bench_path(&format!("two-{}-read", label));
        let store = ShmStore::create(default_config(&path)).expect("store must create");
        let rt = seed_pool(&store, label, value);
        let child = spawn_reader(&path, label);

        let mut group = c.benchmark_group("shm_read");
        group.throughput(Throughput::Elements(1));
        let cursor = Cell::new(0usize);
        group.bench_with_input(BenchmarkId::new("get_2p", label), &(), |b, &_| {
            b.to_async(&rt).iter(|| async {
                let key = format!("{label}-{}", cursor.get() % POOL);
                cursor.set(cursor.get() + 1);
                let _ = store.get(&key, None).await.unwrap();
            });
        });
        group.finish();

        stop_reader(child);
        drop(rt);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    // 99:1 mixed read/write with a concurrent reader, on the small sizes the
    // acceptance thresholds name.
    for (label, value) in &sizes {
        if *label != "64b" && *label != "1k" {
            continue;
        }
        let path = bench_path(&format!("two-{label}-mixed"));
        let store = ShmStore::create(default_config(&path)).expect("store must create");
        let rt = seed_pool(&store, label, value);
        let child = spawn_reader(&path, label);

        let mut group = c.benchmark_group("shm_mixed");
        group.throughput(Throughput::Elements(100));
        let cursor = Cell::new(0usize);
        group.bench_with_input(
            BenchmarkId::new("reads_writes_99_1_2p", label),
            &(),
            |b, &_| {
                b.to_async(&rt).iter(|| async {
                    for r in 0..99 {
                        let key = format!("{label}-{}", (cursor.get() + r) % POOL);
                        let _ = store.get(&key, None).await.unwrap();
                    }
                    cursor.set(cursor.get() + 99);
                    store
                        .put(&format!("{label}-hot"), value.clone(), None, None)
                        .await
                        .unwrap();
                });
            },
        );
        group.finish();

        stop_reader(child);
        drop(rt);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }
}

fn main() {
    if std::env::var("OKV_BENCH_CHILD").is_ok() {
        reader_child();
        return;
    }
    let mut criterion = Criterion::default().configure_from_args();
    bench_single(&mut criterion);
    bench_batch(&mut criterion);
    bench_two_process(&mut criterion);
    criterion.final_summary();
}
