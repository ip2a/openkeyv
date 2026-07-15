import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from typing import Any, cast

import pytest
from cryptography.fernet import Fernet
from typing_extensions import override

from openkeyv import MemoryStore
from openkeyv.errors import (
    CorruptedDataError,
    DecompressionError,
    EntryTooLargeError,
    EntryTooSmallError,
    ReadOnlyError,
    RoutingError,
)
from openkeyv.protocols.key_value import AsyncKeyValue
from openkeyv.wrappers import (
    BaseEncryptionWrapper,
    CollectionRoutingWrapper,
    CompressionWrapper,
    DefaultValueWrapper,
    FernetEncryptionWrapper,
    LimitSizeWrapper,
    LoggingWrapper,
    PassthroughCacheWrapper,
    PrefixCollectionsWrapper,
    PrefixKeysWrapper,
    ReadOnlyWrapper,
    RoutingWrapper,
    SingleCollectionWrapper,
    StatisticsWrapper,
    TimeoutWrapper,
    TTLClampWrapper,
)
from openkeyv.wrappers.base import BaseWrapper

WrapperFactory = Callable[[], AsyncKeyValue]


def _memory_store() -> AsyncKeyValue:
    return cast("AsyncKeyValue", MemoryStore())


def _routing_wrapper() -> RoutingWrapper:
    store = _memory_store()
    return RoutingWrapper(lambda _collection: store)


def _collection_routing_wrapper() -> CollectionRoutingWrapper:
    return CollectionRoutingWrapper({"items": _memory_store()})


def _base_encryption_wrapper() -> BaseEncryptionWrapper:
    return BaseEncryptionWrapper(
        _memory_store(),
        encryption_fn=lambda value: value[::-1],
        decryption_fn=lambda value, _version: value[::-1],
        encryption_version=1,
    )


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(lambda: CompressionWrapper(_memory_store(), min_size_to_compress=0), id="compression"),
        pytest.param(_base_encryption_wrapper, id="base-encryption"),
        pytest.param(
            lambda: FernetEncryptionWrapper(_memory_store(), fernet=Fernet(Fernet.generate_key())),
            id="fernet-encryption",
        ),
        pytest.param(lambda: LimitSizeWrapper(_memory_store(), min_size=1, max_size=10_000), id="limit-size"),
        pytest.param(lambda: LoggingWrapper(_memory_store()), id="logging"),
        pytest.param(lambda: PassthroughCacheWrapper(_memory_store(), _memory_store()), id="passthrough-cache"),
        pytest.param(lambda: PrefixCollectionsWrapper(_memory_store(), prefix="prefix"), id="prefix-collections"),
        pytest.param(lambda: PrefixKeysWrapper(_memory_store(), prefix="prefix"), id="prefix-keys"),
        pytest.param(_routing_wrapper, id="routing"),
        pytest.param(_collection_routing_wrapper, id="collection-routing"),
        pytest.param(lambda: SingleCollectionWrapper(_memory_store(), single_collection="all"), id="single-collection"),
        pytest.param(lambda: StatisticsWrapper(_memory_store()), id="statistics"),
        pytest.param(lambda: TimeoutWrapper(_memory_store(), timeout=1), id="timeout"),
        pytest.param(lambda: TTLClampWrapper(_memory_store(), min_ttl=1, max_ttl=120), id="ttl-clamp"),
    ],
)
async def test_wrappers_preserve_core_store_behavior(factory: WrapperFactory) -> None:
    store = factory()
    first = {"value": "first"}
    second = {"value": "second"}
    third = {"value": "third"}

    await store.put("one", first, collection="items", ttl=60)
    await store.put_many(["two", "three"], [second, third], collection="items", ttl=60)

    assert await store.get("one", collection="items") == first
    assert await store.get_many(["three", "missing", "two"], collection="items") == [third, None, second]

    value, ttl = await store.ttl("one", collection="items")
    assert value == first
    assert ttl is not None
    assert 0 < ttl <= 60

    ttl_many = await store.ttl_many(["missing", "two"], collection="items")
    assert ttl_many[0] == (None, None)
    assert ttl_many[1][0] == second
    assert ttl_many[1][1] is not None

    assert await store.delete("one", collection="items") is True
    assert await store.delete_many(["two", "missing", "three"], collection="items") == 2


async def test_compression_is_visible_only_in_the_backing_store_and_corruption_is_strict() -> None:
    backing = MemoryStore()
    store = CompressionWrapper(cast("AsyncKeyValue", backing), min_size_to_compress=0)
    value = {"payload": "compress me"}

    await store.put("key", value)

    raw = await backing.get("key")
    assert isinstance(raw, dict)
    assert set(raw) == {"__compressed_data__", "__compression_version__", "__compression_algorithm__"}
    assert await store.get("key") == value

    await backing.put(
        "corrupt",
        {
            "__compressed_data__": "not-base64",
            "__compression_version__": 1,
            "__compression_algorithm__": "gzip",
        },
    )
    with pytest.raises(DecompressionError):
        await store.get("corrupt")


async def test_encryption_is_visible_only_in_the_backing_store_and_plaintext_is_rejected() -> None:
    backing = MemoryStore()
    store = FernetEncryptionWrapper(cast("AsyncKeyValue", backing), fernet=Fernet(Fernet.generate_key()))
    value = {"secret": "value"}

    await store.put("encrypted", value)

    raw = await backing.get("encrypted")
    assert isinstance(raw, dict)
    assert set(raw) == {"__encrypted_data__", "__encryption_version__"}
    assert raw != value
    assert await store.get("encrypted") == value

    await backing.put("plaintext", value)
    with pytest.raises(CorruptedDataError):
        await store.get("plaintext")


async def test_default_value_returns_independent_values_without_persisting_them() -> None:
    backing = MemoryStore()
    store = DefaultValueWrapper(cast("AsyncKeyValue", backing), default_value={"items": []}, default_ttl=15)

    first = await store.get("missing")
    second = await store.get("missing")

    assert first == {"items": []}
    assert second == {"items": []}
    assert first is not second
    assert await store.ttl("missing") == ({"items": []}, 15.0)
    assert await backing.get("missing") is None


async def test_limit_size_rejects_single_and_batch_writes_before_storage() -> None:
    backing = MemoryStore()
    store = LimitSizeWrapper(cast("AsyncKeyValue", backing), min_size=20, max_size=40)

    with pytest.raises(EntryTooSmallError):
        await store.put("small", {"x": "y"})

    with pytest.raises(EntryTooLargeError):
        await store.put("large", {"payload": "x" * 100})

    with pytest.raises(EntryTooLargeError):
        await store.put_many(["valid", "large"], [{"payload": "x" * 10}, {"payload": "x" * 100}])

    assert await backing.get_many(["small", "large", "valid"]) == [None, None, None]


async def test_passthrough_cache_populates_clamps_and_invalidates_the_cache() -> None:
    primary = MemoryStore()
    cache = MemoryStore()
    store = PassthroughCacheWrapper(
        cast("AsyncKeyValue", primary),
        cast("AsyncKeyValue", cache),
        maximum_ttl=10,
        missing_ttl=5,
    )

    await primary.put("key", {"version": 1})
    assert await cache.get("key") is None

    assert await store.get("key") == {"version": 1}
    cached, cached_ttl = await cache.ttl("key")
    assert cached == {"version": 1}
    assert cached_ttl is not None
    assert 0 < cached_ttl <= 5

    await store.put("key", {"version": 2})
    assert await cache.get("key") is None
    assert await primary.get("key") == {"version": 2}


def test_single_collection_wrapper_has_no_separator_configuration() -> None:
    with pytest.raises(TypeError):
        SingleCollectionWrapper(_memory_store(), single_collection="all", separator="__")  # type: ignore[call-arg]


async def test_single_collection_wrapper_preserves_empty_and_collision_identities() -> None:
    backing = MemoryStore()
    store = SingleCollectionWrapper(cast("AsyncKeyValue", backing), single_collection="all")

    await store.put("c", {"value": "left"}, collection="a:b")
    await store.put("b:c", {"value": "right"}, collection="a")
    await store.put("key", {"value": "empty"}, collection="")

    assert await store.get("c", collection="a:b") == {"value": "left"}
    assert await store.get("b:c", collection="a") == {"value": "right"}
    assert await store.get("key", collection="") == {"value": "empty"}
    assert await backing.get("3:a:bc", collection="all") == {"value": "left"}
    assert await backing.get("1:ab:c", collection="all") == {"value": "right"}
    assert await backing.get("0:key", collection="all") == {"value": "empty"}


async def test_prefix_and_single_collection_wrappers_transform_backing_keys() -> None:
    collection_backing = MemoryStore()
    collection_store = PrefixCollectionsWrapper(cast("AsyncKeyValue", collection_backing), prefix="tenant")
    await collection_store.put("key", {"value": 1}, collection="users")
    assert await collection_backing.get("key", collection="tenant__users") == {"value": 1}

    key_backing = MemoryStore()
    key_store = PrefixKeysWrapper(cast("AsyncKeyValue", key_backing), prefix="tenant")
    await key_store.put("key", {"value": 1}, collection="users")
    assert await key_backing.get("tenant__key", collection="users") == {"value": 1}

    single_backing = MemoryStore()
    single_store = SingleCollectionWrapper(cast("AsyncKeyValue", single_backing), single_collection="all")
    await single_store.put("key", {"value": 1}, collection="users")
    assert await single_backing.get("5:userskey", collection="all") == {"value": 1}


async def test_read_only_wrapper_allows_reads_and_rejects_all_writes() -> None:
    backing = MemoryStore()
    await backing.put("key", {"value": 1})
    store = ReadOnlyWrapper(cast("AsyncKeyValue", backing))

    assert await store.get("key") == {"value": 1}

    with pytest.raises(ReadOnlyError):
        await store.put("key", {"value": 2})
    with pytest.raises(ReadOnlyError):
        await store.put_many(["key"], [{"value": 2}])
    with pytest.raises(ReadOnlyError):
        await store.delete("key")
    with pytest.raises(ReadOnlyError):
        await store.delete_many(["key"])

    assert await backing.get("key") == {"value": 1}


async def test_routing_requires_an_explicit_store() -> None:
    store = RoutingWrapper(lambda _collection: cast("AsyncKeyValue", None))

    with pytest.raises(RoutingError):
        await store.get("key", collection="missing")

    collection_store = CollectionRoutingWrapper({"known": _memory_store()})
    with pytest.raises(RoutingError):
        await collection_store.get("key", collection="missing")


async def test_statistics_count_single_and_batch_hits_and_misses() -> None:
    store = StatisticsWrapper(_memory_store())

    await store.put("one", {"value": 1}, collection="items")
    await store.put_many(["two", "three"], [{"value": 2}, {"value": 3}], collection="items")
    assert await store.get_many(["one", "missing"], collection="items") == [{"value": 1}, None]
    assert await store.ttl("two", collection="items") == ({"value": 2}, None)
    assert await store.delete_many(["one", "missing"], collection="items") == 1

    statistics = store.statistics.get_collection("items")
    assert statistics.put.count == 3
    assert (statistics.get.count, statistics.get.hit, statistics.get.miss) == (2, 1, 1)
    assert (statistics.ttl.count, statistics.ttl.hit, statistics.ttl.miss) == (1, 1, 0)
    assert (statistics.delete.count, statistics.delete.hit, statistics.delete.miss) == (2, 1, 1)


class _SlowGetWrapper(BaseWrapper):
    def __init__(self, key_value: AsyncKeyValue) -> None:
        self.key_value = key_value

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        await asyncio.sleep(0.05)
        return await super().get(key, collection=collection)


async def test_timeout_wrapper_cancels_slow_operations_and_validates_configuration() -> None:
    store = TimeoutWrapper(_SlowGetWrapper(_memory_store()), timeout=0.001)

    with pytest.raises(TimeoutError):
        await store.get("key")

    with pytest.raises(TypeError):
        TimeoutWrapper(_memory_store(), timeout=True)
    for invalid in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="finite number greater than zero"):
            TimeoutWrapper(_memory_store(), timeout=invalid)


async def test_ttl_clamp_applies_minimum_maximum_and_missing_ttl() -> None:
    backing = MemoryStore()
    store = TTLClampWrapper(cast("AsyncKeyValue", backing), min_ttl=10, max_ttl=20, missing_ttl=15)

    await store.put("minimum", {"value": 1}, ttl=1)
    await store.put("maximum", {"value": 2}, ttl=100)
    await store.put("missing", {"value": 3})

    for key, expected in (("minimum", 10), ("maximum", 20), ("missing", 15)):
        value, ttl = await backing.ttl(key)
        assert value is not None
        assert ttl is not None
        assert expected - 0.1 <= ttl <= expected


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"min_ttl": True, "max_ttl": 1}, TypeError),
        ({"min_ttl": -1, "max_ttl": 1}, ValueError),
        ({"min_ttl": 2, "max_ttl": 1}, ValueError),
        ({"min_ttl": 0, "max_ttl": float("inf")}, ValueError),
        ({"min_ttl": 0, "max_ttl": 1, "missing_ttl": 0}, ValueError),
    ],
)
def test_ttl_clamp_validates_configuration(kwargs: Mapping[str, Any], error: type[Exception]) -> None:
    with pytest.raises(error):
        TTLClampWrapper(_memory_store(), **kwargs)


async def test_logging_wrapper_emits_structured_start_and_finish_records(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("openkeyv.tests.structured")
    store = LoggingWrapper(_memory_store(), logger=logger, structured_logs=True, log_values=True)

    with caplog.at_level(logging.INFO, logger=logger.name):
        await store.put("key", {"value": 1}, collection="items")

    records = [json.loads(record.message) for record in caplog.records]
    assert [record["status"] for record in records] == ["start", "finish"]
    assert all(record["action"] == "PUT" for record in records)
    assert all(record["collection"] == "items" for record in records)
    assert all(record["value"] == {"value": 1} for record in records)
