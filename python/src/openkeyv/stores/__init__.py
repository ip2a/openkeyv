"""Store backends — implemented in Rust and exposed via PyO3."""

from openkeyv._internal import (
    MemoryStore,
    SimpleStore,
    FileTreeStore,
    NullStore,
)

try:
    from openkeyv._internal import DiskStore
except ImportError:
    DiskStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import RedisStore
except ImportError:
    RedisStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import ValkeyStore
except ImportError:
    ValkeyStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import RocksDBStore
except ImportError:
    RocksDBStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import PostgresStore
except ImportError:
    PostgresStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import MongoStore
except ImportError:
    MongoStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import DynamoDBStore
except ImportError:
    DynamoDBStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import S3Store
except ImportError:
    S3Store = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import DuckDBStore
except ImportError:
    DuckDBStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import MemcachedStore
except ImportError:
    MemcachedStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import VaultStore
except ImportError:
    VaultStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import KeyringStore
except ImportError:
    KeyringStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import FirestoreStore
except ImportError:
    FirestoreStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv._internal import OpenSearchStore
except ImportError:
    OpenSearchStore = None  # type: ignore[misc,assignment]

# Fallback Python-only stores (no Rust implementation yet)
try:
    from openkeyv.stores.aerospike import AerospikeStore
except ImportError:
    AerospikeStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv.stores.elasticsearch import ElasticsearchStore
except ImportError:
    ElasticsearchStore = None  # type: ignore[misc,assignment]

try:
    from openkeyv.stores.windows_registry import WindowsRegistryStore
except ImportError:
    WindowsRegistryStore = None  # type: ignore[misc,assignment]

__all__ = [
    "MemoryStore",
    "SimpleStore",
    "FileTreeStore",
    "NullStore",
    "DiskStore",
    "RedisStore",
    "ValkeyStore",
    "RocksDBStore",
    "PostgresStore",
    "MongoStore",
    "DynamoDBStore",
    "S3Store",
    "DuckDBStore",
    "MemcachedStore",
    "VaultStore",
    "KeyringStore",
    "FirestoreStore",
    "OpenSearchStore",
    "AerospikeStore",
    "ElasticsearchStore",
    "WindowsRegistryStore",
]
