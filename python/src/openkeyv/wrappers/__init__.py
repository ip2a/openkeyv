"""Top-level wrapper exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any

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

__all__ = sorted(_SYMBOL_TO_MODULE)


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
