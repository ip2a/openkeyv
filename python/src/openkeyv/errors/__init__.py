"""Error classes for key-value store operations.

This module provides a hierarchy of exception classes used throughout the key-value
store implementations. The hierarchy allows callers to handle broad error groups
through shared base classes or catch specific operation failures.

Exception Hierarchy:
    BaseKeyValueError (base for all KV errors)
    ├── KeyValueOperationError (operation-level errors)
    │   ├── SerializationError
    │   ├── DeserializationError
    │   ├── MissingKeyError
    │   ├── InvalidTTLError
    │   ├── InvalidKeyError
    │   ├── ValueTooLargeError
    │   ├── EncryptionError
    │   │   ├── DecryptionError
    │   │   │   └── CorruptedDataError
    │   │   └── EncryptionVersionError
    │   ├── CompressionError
    │   │   └── DecompressionError
    │   ├── ReadOnlyError
    │   ├── RoutingError
    │   ├── EntryTooLargeError
    │   └── EntryTooSmallError
    └── KeyValueStoreError (store-level errors)
        ├── StoreSetupError
        └── StoreConnectionError
"""

from openkeyv.errors.base import BaseKeyValueError, ExtraInfoType
from openkeyv.errors.key_value import (
    DeserializationError,
    InvalidKeyError,
    InvalidTTLError,
    KeyValueOperationError,
    MissingKeyError,
    SerializationError,
    ValueTooLargeError,
)
from openkeyv.errors.store import KeyValueStoreError, PathSecurityError, StoreConnectionError, StoreSetupError
from openkeyv.errors.wrappers import (
    CompressionError,
    CorruptedDataError,
    DecompressionError,
    DecryptionError,
    EncryptionError,
    EncryptionVersionError,
    EntryTooLargeError,
    EntryTooSmallError,
    ReadOnlyError,
    RoutingError,
)

__all__ = [
    "BaseKeyValueError",
    "CompressionError",
    "CorruptedDataError",
    "DecompressionError",
    "DecryptionError",
    "DeserializationError",
    "EncryptionError",
    "EncryptionVersionError",
    "EntryTooLargeError",
    "EntryTooSmallError",
    "ExtraInfoType",
    "InvalidKeyError",
    "InvalidTTLError",
    "KeyValueOperationError",
    "KeyValueStoreError",
    "MissingKeyError",
    "PathSecurityError",
    "ReadOnlyError",
    "RoutingError",
    "SerializationError",
    "StoreConnectionError",
    "StoreSetupError",
    "ValueTooLargeError",
]
