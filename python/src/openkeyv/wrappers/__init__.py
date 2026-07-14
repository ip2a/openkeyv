"""Top-level wrapper exports."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from openkeyv.wrappers.compression import CompressionWrapper
    from openkeyv.wrappers.default_value import DefaultValueWrapper
    from openkeyv.wrappers.encryption import BaseEncryptionWrapper, FernetEncryptionWrapper
    from openkeyv.wrappers.limit_size import LimitSizeWrapper
    from openkeyv.wrappers.logging import LoggingWrapper
    from openkeyv.wrappers.passthrough_cache import PassthroughCacheWrapper
    from openkeyv.wrappers.prefix_collections import PrefixCollectionsWrapper
    from openkeyv.wrappers.prefix_keys import PrefixKeysWrapper
    from openkeyv.wrappers.read_only import ReadOnlyWrapper
    from openkeyv.wrappers.routing import CollectionRoutingWrapper, RoutingFunction, RoutingWrapper
    from openkeyv.wrappers.single_collection import SingleCollectionWrapper
    from openkeyv.wrappers.statistics import StatisticsWrapper
    from openkeyv.wrappers.timeout import TimeoutWrapper
    from openkeyv.wrappers.ttl_clamp import TTLClampWrapper

_SYMBOL_TO_MODULE: dict[str, str] = {
    "BaseEncryptionWrapper": "openkeyv.wrappers.encryption",
    "CollectionRoutingWrapper": "openkeyv.wrappers.routing",
    "CompressionWrapper": "openkeyv.wrappers.compression",
    "DefaultValueWrapper": "openkeyv.wrappers.default_value",
    "FernetEncryptionWrapper": "openkeyv.wrappers.encryption",
    "LimitSizeWrapper": "openkeyv.wrappers.limit_size",
    "LoggingWrapper": "openkeyv.wrappers.logging",
    "PassthroughCacheWrapper": "openkeyv.wrappers.passthrough_cache",
    "PrefixCollectionsWrapper": "openkeyv.wrappers.prefix_collections",
    "PrefixKeysWrapper": "openkeyv.wrappers.prefix_keys",
    "ReadOnlyWrapper": "openkeyv.wrappers.read_only",
    "RoutingFunction": "openkeyv.wrappers.routing",
    "RoutingWrapper": "openkeyv.wrappers.routing",
    "SingleCollectionWrapper": "openkeyv.wrappers.single_collection",
    "StatisticsWrapper": "openkeyv.wrappers.statistics",
    "TimeoutWrapper": "openkeyv.wrappers.timeout",
    "TTLClampWrapper": "openkeyv.wrappers.ttl_clamp",
}

__all__ = [
    "BaseEncryptionWrapper",
    "CollectionRoutingWrapper",
    "CompressionWrapper",
    "DefaultValueWrapper",
    "FernetEncryptionWrapper",
    "LimitSizeWrapper",
    "LoggingWrapper",
    "PassthroughCacheWrapper",
    "PrefixCollectionsWrapper",
    "PrefixKeysWrapper",
    "ReadOnlyWrapper",
    "RoutingFunction",
    "RoutingWrapper",
    "SingleCollectionWrapper",
    "StatisticsWrapper",
    "TTLClampWrapper",
    "TimeoutWrapper",
]


def __getattr__(name: str) -> Any:
    module_path = _SYMBOL_TO_MODULE.get(name)
    if module_path is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)

    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
