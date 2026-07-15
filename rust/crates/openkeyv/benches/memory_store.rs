//! Benchmarks for Memory store single and batch operations.
//!
//! Run with: cargo bench --bench memory_store

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use openkeyv::Value;
use openkeyv::protocol::AsyncKeyValue;
use openkeyv::store::memory::MemoryStore;

fn bench_memory_single(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memory_single");
    group.throughput(Throughput::Elements(1));

    group.bench_function("put", |b| {
        b.to_async(&rt).iter(|| async {
            let store = MemoryStore::new();
            store
                .put("bench_key", Value::utf8("value"), None, None)
                .await
                .unwrap();
        });
    });

    group.bench_function("get", |b| {
        b.to_async(&rt).iter(|| async {
            let store = MemoryStore::new();
            store
                .put("bench_key", Value::utf8("value"), None, None)
                .await
                .unwrap();
            let _ = store.get("bench_key", None).await.unwrap();
        });
    });

    group.bench_function("get_missing", |b| {
        b.to_async(&rt).iter(|| async {
            let store = MemoryStore::new();
            let _ = store.get("nonexistent", None).await.unwrap();
        });
    });

    group.bench_function("delete", |b| {
        b.to_async(&rt).iter(|| async {
            let store = MemoryStore::new();
            store
                .put("bench_key", Value::utf8("value"), None, None)
                .await
                .unwrap();
            let _ = store.delete("bench_key", None).await.unwrap();
        });
    });
    group.finish();
}

fn bench_memory_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memory_batch");

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("put_many", batch_size),
            &batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let keys: Vec<String> = (0..size).map(|i| format!("key_{i}")).collect();
                    let values: Vec<Value> = (0..size).map(|_| Value::utf8("value")).collect();
                    let store = MemoryStore::new();
                    store.put_many(&keys, &values, None, None).await.unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("get_many", batch_size),
            &batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let keys: Vec<String> = (0..size).map(|i| format!("key_{i}")).collect();
                    let values: Vec<Value> = (0..size).map(|_| Value::utf8("value")).collect();
                    let store = MemoryStore::new();
                    store.put_many(&keys, &values, None, None).await.unwrap();
                    let _ = store.get_many(&keys, None).await.unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_memory_single, bench_memory_batch);
criterion_main!(benches);
