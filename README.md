# openkeyv

Async key-value store with a pluggable Rust core and Python bindings.

## Rust

```toml
[dependencies]
openkeyv = "0.6"
```

The core crate ships every native backend unconditionally — Memory, Simple,
Null, FileTree, Disk (sled), SHM (unix only), Redis, Valkey, and Memcached —
with zero features to enable. The default dependency tree compiles no C/C++.

SDK-backed backends (PostgreSQL, SQLite, DuckDB, MongoDB, Firestore,
OpenSearch, S3, DynamoDB, Vault, Keyring, RocksDB) live in the separate
`openkeyv-bridges` crate, each behind its own opt-in feature:

```toml
[dependencies]
openkeyv = "0.6"
openkeyv-bridges = "0.1"
```

```toml
# Cargo.toml
[dependencies.openkeyv-bridges]
version = "0.1"
features = ["sqlite"]
```

```rust
// One entry point covers both crates: bridge names hit their backend,
// everything else falls through to openkeyv::factory::open_store.
let handle = openkeyv_bridges::open_store(config).await?;
```

## Python

```bash
pip install openkeyv
```

The Python wheel ships Memory, Simple, Disk, SQLite, Redis, and Valkey backends.
