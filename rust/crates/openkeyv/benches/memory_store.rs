//! Benchmarks for Memory store single and batch operations.
//!
//! Stores and seed data are created outside the measured iteration; every
//! operation is measured alone.
//!
//! Run with: cargo bench --bench memory_store

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use openkeyv::Value;
use openkeyv::protocol::{AsyncCompareAndSwap, AsyncKeyValue};
use openkeyv::store::memory::MemoryStore;

fn bench_memory_single(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memory_single");
    group.throughput(Throughput::Elements(1));

    // One long-lived store per measured operation; the seed write happens
    // before timing starts.
    let put_store = MemoryStore::new();
    rt.block_on(put_store.put("bench_key", Value::utf8("value"), None, None))
        .unwrap();
    group.bench_function("put", |b| {
        b.to_async(&rt).iter(|| async {
            put_store
                .put("bench_key", Value::utf8("value"), None, None)
                .await
                .unwrap();
        });
    });

    let get_store = MemoryStore::new();
    rt.block_on(get_store.put("bench_key", Value::utf8("value"), None, None))
        .unwrap();
    group.bench_function("get", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = get_store.get("bench_key", None).await.unwrap();
        });
    });

    let missing_store = MemoryStore::new();
    group.bench_function("get_missing", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = missing_store.get("nonexistent", None).await.unwrap();
        });
    });

    let delete_store = MemoryStore::new();
    group.bench_function("delete", |b| {
        b.to_async(&rt).iter(|| async {
            delete_store
                .put("bench_key", Value::utf8("value"), None, None)
                .await
                .unwrap();
            let _ = delete_store.delete("bench_key", None).await.unwrap();
        });
    });

    let cas_store = MemoryStore::new();
    rt.block_on(cas_store.put("cas_key", Value::utf8("value"), None, None))
        .unwrap();
    group.bench_function("cas_update", |b| {
        b.to_async(&rt).iter(|| async {
            let observed = cas_store
                .get_with_revision("cas_key", None)
                .await
                .unwrap()
                .unwrap();
            cas_store
                .compare_and_swap(
                    "cas_key",
                    Some(&observed.revision),
                    Value::utf8("value"),
                    None,
                    None,
                )
                .await
                .unwrap();
        });
    });
    group.finish();
}

fn bench_memory_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memory_batch");

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        let keys: Vec<String> = (0..batch_size).map(|i| format!("key_{i}")).collect();
        let values: Vec<Value> = (0..batch_size).map(|_| Value::utf8("value")).collect();

        // Seed the store once; the iteration only measures the batch write.
        let put_store = MemoryStore::new();
        rt.block_on(put_store.put_many(&keys, &values, None, None))
            .unwrap();
        group.bench_with_input(
            BenchmarkId::new("put_many", batch_size),
            &batch_size,
            |b, &_size| {
                b.to_async(&rt).iter(|| async {
                    put_store
                        .put_many(&keys, &values, None, None)
                        .await
                        .unwrap();
                });
            },
        );

        let get_store = MemoryStore::new();
        rt.block_on(get_store.put_many(&keys, &values, None, None))
            .unwrap();
        group.bench_with_input(
            BenchmarkId::new("get_many", batch_size),
            &batch_size,
            |b, &_size| {
                b.to_async(&rt).iter(|| async {
                    let _ = get_store.get_many(&keys, None).await.unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_memory_single, bench_memory_batch);
criterion_main!(benches);
