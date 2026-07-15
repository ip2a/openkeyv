//! Benchmarks for Redis store single, batch, and CAS operations.
//!
//! Requires a running Redis instance at OPENKEYV_REDIS_URL.
//! Run with: OPENKEYV_REDIS_URL=redis://127.0.0.1:16379 cargo bench --bench redis_store --features redis

use std::env;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use openkeyv::protocol::{AsyncCompareAndSwap, AsyncDestroyStore, AsyncKeyValue};
use openkeyv::store::redis::RedisStore;
use openkeyv::{CompareAndSwapResult, Revision, Value};

fn redis_url() -> String {
    env::var("OPENKEYV_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379".to_string())
}

async fn setup_redis() -> RedisStore {
    let store = RedisStore::new(&redis_url()).await.unwrap();
    store.destroy().await.unwrap();
    store
}

fn bench_redis_single(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("redis_single");
    group.throughput(Throughput::Elements(1));

    group.bench_function("put", |b| {
        b.to_async(&rt).iter(|| async {
            let store = setup_redis().await;
            store
                .put("bench_key", Value::utf8("value"), None, None)
                .await
                .unwrap();
        });
    });

    group.bench_function("get", |b| {
        b.to_async(&rt).iter(|| async {
            let store = setup_redis().await;
            store
                .put("bench_key", Value::utf8("value"), None, None)
                .await
                .unwrap();
            let _ = store.get("bench_key", None).await.unwrap();
        });
    });

    group.bench_function("get_missing", |b| {
        b.to_async(&rt).iter(|| async {
            let store = setup_redis().await;
            let _ = store.get("nonexistent", None).await.unwrap();
        });
    });
    group.finish();
}

fn bench_redis_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("redis_batch");

    for batch_size in [10, 100] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("put_many", batch_size),
            &batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let keys: Vec<String> = (0..size).map(|i| format!("bkey_{i}")).collect();
                    let values: Vec<Value> = (0..size).map(|_| Value::utf8("value")).collect();
                    let store = setup_redis().await;
                    store.put_many(&keys, &values, None, None).await.unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("get_many", batch_size),
            &batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let keys: Vec<String> = (0..size).map(|i| format!("bkey_{i}")).collect();
                    let values: Vec<Value> = (0..size).map(|_| Value::utf8("value")).collect();
                    let store = setup_redis().await;
                    store.put_many(&keys, &values, None, None).await.unwrap();
                    let _ = store.get_many(&keys, None).await.unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_redis_cas(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("redis_cas");
    group.throughput(Throughput::Elements(1));

    // CAS create-if-absent.
    group.bench_function("create_if_absent", |b| {
        b.to_async(&rt).iter(|| async {
            let store = setup_redis().await;
            let result = store
                .compare_and_swap("cas_key", None, Value::utf8("v1"), None, None)
                .await
                .unwrap();
            assert!(matches!(result, CompareAndSwapResult::Applied { .. }));
        });
    });

    // CAS successful update with exact revision.
    group.bench_function("update_with_revision", |b| {
        b.to_async(&rt).iter(|| async {
            let store = setup_redis().await;
            store
                .put("cas_key", Value::utf8("v0"), None, None)
                .await
                .unwrap();
            let observed = store
                .get_with_revision("cas_key", None)
                .await
                .unwrap()
                .unwrap();
            let rev = observed.revision;
            let result = store
                .compare_and_swap("cas_key", Some(&rev), Value::utf8("v1"), None, None)
                .await
                .unwrap();
            assert!(matches!(result, CompareAndSwapResult::Applied { .. }));
        });
    });

    // CAS conflict path.
    group.bench_function("conflict", |b| {
        b.to_async(&rt).iter(|| async {
            let store = setup_redis().await;
            store
                .put("cas_key", Value::utf8("v0"), None, None)
                .await
                .unwrap();
            // Use a bogus revision to force conflict.
            let bogus = Revision::from_bytes([0u8; 16]);
            let result = store
                .compare_and_swap("cas_key", Some(&bogus), Value::utf8("v1"), None, None)
                .await
                .unwrap();
            assert!(matches!(result, CompareAndSwapResult::Conflict { .. }));
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_redis_single,
    bench_redis_batch,
    bench_redis_cas
);
criterion_main!(benches);
