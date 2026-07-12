"""openkeyv — Async Key-Value Store with Rust core.

This package provides high-performance key-value store backends
implemented in Rust and exposed to Python via PyO3.
"""

from openkeyv._internal import (
    FileTreeStore,
    MemoryStore,
    NullStore,
    SimpleStore,
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

from openkeyv.adapters.dataclass import DataclassAdapter
from openkeyv.adapters.pydantic import PydanticAdapter
from openkeyv.adapters.raise_on_missing import RaiseOnMissingAdapter
from openkeyv.wrappers.compression import CompressionWrapper
from openkeyv.wrappers.default_value import DefaultValueWrapper
from openkeyv.wrappers.encryption import FernetEncryptionWrapper
from openkeyv.wrappers.limit_size import LimitSizeWrapper
from openkeyv.wrappers.logging import LoggingWrapper
from openkeyv.wrappers.passthrough_cache import PassthroughCacheWrapper
from openkeyv.wrappers.prefix_collections import PrefixCollectionsWrapper
from openkeyv.wrappers.prefix_keys import PrefixKeysWrapper
from openkeyv.wrappers.read_only import ReadOnlyWrapper
from openkeyv.wrappers.routing import CollectionRoutingWrapper, RoutingWrapper
from openkeyv.wrappers.single_collection import SingleCollectionWrapper
from openkeyv.wrappers.statistics import StatisticsWrapper
from openkeyv.wrappers.timeout import TimeoutWrapper
from openkeyv.wrappers.ttl_clamp import TTLClampWrapper

__all__ = [
    "AerospikeStore",
    "CollectionRoutingWrapper",
    "CompressionWrapper",
    "DataclassAdapter",
    "DefaultValueWrapper",
    "DiskStore",
    "DuckDBStore",
    "DynamoDBStore",
    "ElasticsearchStore",
    "FernetEncryptionWrapper",
    "FileTreeStore",
    "FirestoreStore",
    "KeyringStore",
    "LimitSizeWrapper",
    "LoggingWrapper",
    "MemcachedStore",
    "MemoryStore",
    "MongoStore",
    "NullStore",
    "OpenSearchStore",
    "PassthroughCacheWrapper",
    "PostgresStore",
    "PrefixCollectionsWrapper",
    "PrefixKeysWrapper",
    "PydanticAdapter",
    "RaiseOnMissingAdapter",
    "ReadOnlyWrapper",
    "RedisStore",
    "RocksDBStore",
    "RoutingWrapper",
    "S3Store",
    "SimpleStore",
    "SingleCollectionWrapper",
    "StatisticsWrapper",
    "TTLClampWrapper",
    "TimeoutWrapper",
    "ValkeyStore",
    "VaultStore",
    "WindowsRegistryStore",
]
