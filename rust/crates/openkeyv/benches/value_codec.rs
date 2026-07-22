//! Benchmarks for Value and StructuredValue encoding/decoding.
//!
//! Run with: cargo bench --bench value_codec

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use openkeyv::{StructuredValue, Value, ValueKind};

/// Encode and decode each primitive ValueKind.
fn bench_value_kind_encode_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_kind");
    group.throughput(Throughput::Elements(1));

    let kinds: Vec<(&str, Value)> = vec![
        ("binary", Value::binary(Bytes::from_static(&[0; 64]))),
        ("utf8", Value::utf8("hello world".repeat(5))),
        ("integer", Value::integer(i64::MAX)),
        ("unsigned", Value::unsigned_integer(u64::MAX)),
        ("float", Value::float(f64::MAX)),
        ("bool", Value::bool(true)),
        ("null", Value::null()),
    ];

    for (name, value) in kinds {
        group.bench_with_input(BenchmarkId::new("encode", name), &value, |b, v| {
            b.iter(|| {
                let _ = black_box(v.kind());
                let _ = black_box(v.bytes());
            });
        });

        group.bench_with_input(BenchmarkId::new("decode", name), &value, |b, v| {
            b.iter(|| {
                if v.kind() == ValueKind::Structured {
                    let _ = black_box(v.decode_structured());
                }
            });
        });
    }
    group.finish();
}

/// StructuredValue small object encoding/decoding.
fn bench_structured_small(c: &mut Criterion) {
    let small = StructuredValue::Dict(vec![
        ("id".to_string(), StructuredValue::UnsignedInteger(42)),
        (
            "name".to_string(),
            StructuredValue::String("test".to_string()),
        ),
        ("active".to_string(), StructuredValue::Bool(true)),
        (
            "score".to_string(),
            StructuredValue::Float(std::f64::consts::PI),
        ),
    ]);

    let encoded = small.encode().unwrap();

    let mut group = c.benchmark_group("structured_small");
    group.throughput(Throughput::Bytes(encoded.len() as u64));

    group.bench_function("encode", |b| {
        b.iter(|| black_box(small.encode().unwrap()));
    });

    group.bench_function("decode", |b| {
        b.iter(|| black_box(StructuredValue::decode(&encoded).unwrap()));
    });
    group.finish();
}

/// StructuredValue large nested object encoding/decoding.
fn bench_structured_large(c: &mut Criterion) {
    let large = build_large_structured(100);

    let encoded = large.encode().unwrap();

    let mut group = c.benchmark_group("structured_large");
    group.throughput(Throughput::Bytes(encoded.len() as u64));

    group.bench_function("encode", |b| {
        b.iter(|| black_box(large.encode().unwrap()));
    });

    group.bench_function("decode", |b| {
        b.iter(|| black_box(StructuredValue::decode(&encoded).unwrap()));
    });
    group.finish();
}

/// Binary zero-copy read: Value::new with Binary kind should not copy.
fn bench_binary_zero_copy(c: &mut Criterion) {
    let data = Bytes::from(vec![0u8; 4096]);

    let mut group = c.benchmark_group("binary_zero_copy");
    group.throughput(Throughput::Bytes(4096));

    group.bench_function("construct", |b| {
        b.iter(|| {
            let v = Value::binary(data.clone());
            black_box(v);
        });
    });

    group.bench_function("checked_new", |b| {
        b.iter(|| {
            let v = Value::new(ValueKind::Binary, data.clone()).unwrap();
            black_box(v);
        });
    });
    group.finish();
}

fn build_large_structured(n: usize) -> StructuredValue {
    let items: Vec<StructuredValue> = (0..n)
        .map(|i| {
            StructuredValue::Dict(vec![
                (
                    "index".to_string(),
                    StructuredValue::UnsignedInteger(i as u64),
                ),
                (
                    "data".to_string(),
                    StructuredValue::String(format!("item_{i}")),
                ),
                (
                    "nested".to_string(),
                    StructuredValue::List(vec![
                        StructuredValue::Integer(i as i64),
                        StructuredValue::Integer((i + 1) as i64),
                        StructuredValue::Float(i as f64 / 3.0),
                    ]),
                ),
            ])
        })
        .collect();

    StructuredValue::Dict(vec![
        ("items".to_string(), StructuredValue::List(items)),
        (
            "count".to_string(),
            StructuredValue::UnsignedInteger(n as u64),
        ),
    ])
}

criterion_group!(
    benches,
    bench_value_kind_encode_decode,
    bench_structured_small,
    bench_structured_large,
    bench_binary_zero_copy,
);
criterion_main!(benches);
