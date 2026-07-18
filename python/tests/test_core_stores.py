import asyncio
from pathlib import Path

import pytest

from openkeyv import MemoryStore, NullStore, SimpleStore, SqliteStore


@pytest.mark.parametrize("store_type", [MemoryStore, SimpleStore])
async def test_local_store_roundtrip_and_batches(store_type: type[MemoryStore] | type[SimpleStore]) -> None:
    store = store_type()
    nested_value = {
        "bytes": b"payload",
        "list": ["text", 42, 1.5, True, None],
        "dict": {"nested": "value"},
    }

    await store.put("one", nested_value, collection="items")
    await store.put_many(["two", "three"], ["second", 3], collection="items")

    assert await store.get("one", collection="items") == nested_value
    assert await store.get_many(["three", "missing", "two"], collection="items") == [3, None, "second"]
    assert set(await store.keys(collection="items")) == {"one", "two", "three"}
    assert "items" in await store.collections()
    assert await store.delete_many(["one", "missing", "three"], collection="items") == 2
    assert await store.destroy_collection("items") is True


@pytest.mark.parametrize("store_type", [MemoryStore, SimpleStore])
async def test_local_store_public_lifecycle_capabilities(store_type: type[MemoryStore] | type[SimpleStore]) -> None:
    store = store_type()
    await store.put_many(["one", "two"], [1, 2], collection="first")
    await store.put("three", 3, collection="second")

    assert len(await store.keys(collection="first", limit=1)) == 1
    assert len(await store.collections(limit=1)) == 1

    await store.cull()
    assert await store.destroy() is True
    assert await store.collections() == []


@pytest.mark.parametrize("store_type", [MemoryStore, SimpleStore])
async def test_local_store_preserves_missing_ttl(store_type: type[MemoryStore] | type[SimpleStore]) -> None:
    store = store_type()
    await store.put("persistent", "value")

    assert await store.ttl("persistent") == ("value", None)
    assert await store.ttl_many(["persistent", "missing"]) == [("value", None), (None, None)]


@pytest.mark.parametrize("store_type", [MemoryStore, SimpleStore])
async def test_local_store_ttl_expires(store_type: type[MemoryStore] | type[SimpleStore]) -> None:
    store = store_type()
    await store.put("temporary", "value", ttl=0.01)

    value, ttl = await store.ttl("temporary")
    assert value == "value"
    assert ttl is not None
    assert 0.0 <= ttl <= 0.01

    await asyncio.sleep(0.03)

    assert await store.get("temporary") is None
    assert await store.ttl("temporary") == (None, None)


async def test_sqlite_store_persists_values(tmp_path: Path) -> None:
    path = str(tmp_path / "openkeyv.sqlite3")
    store = SqliteStore(path)
    value = {"bytes": b"payload", "items": [1, True, None]}

    await store.put("key", value, collection="items")

    reopened = SqliteStore(path)
    assert await reopened.get("key", collection="items") == value
    assert await reopened.destroy() is True


async def test_batch_size_mismatch_raises() -> None:
    with pytest.raises(RuntimeError, match="batch size mismatch"):
        await MemoryStore().put_many(["key"], [])


async def test_memory_store_roundtrips_unsigned_integer_boundaries() -> None:
    store = MemoryStore()
    values = [2**63, 2**64 - 1, {"nested": [2**63, 2**64 - 1]}]
    keys = ["lower", "upper", "nested"]

    await store.put_many(keys, values)

    assert await store.get_many(keys) == values


async def test_null_store_has_explicit_noop_results() -> None:
    store = NullStore()

    await store.put("key", "value")
    await store.put_many(["one"], [1])

    assert await store.get("key") is None
    assert await store.ttl("key") == (None, None)
    assert await store.delete("key") is False
    assert await store.delete_many(["one"]) == 0
    assert await store.keys() == []
    assert await store.collections() == []
    assert await store.destroy_collection("collection") is False
    await store.cull()
    assert await store.destroy() is True


@pytest.mark.parametrize("store_type", [MemoryStore, SimpleStore, NullStore])
@pytest.mark.parametrize("ttl", [float("nan"), float("inf"), float("-inf"), 0.0, -1.0])
async def test_stores_reject_invalid_ttl(store_type: type[MemoryStore] | type[SimpleStore] | type[NullStore], ttl: float) -> None:
    store = store_type()

    with pytest.raises(RuntimeError, match="invalid ttl"):
        await store.put("key", "value", ttl=ttl)

    with pytest.raises(RuntimeError, match="invalid ttl"):
        await store.put_many(["key"], ["value"], ttl=ttl)

    with pytest.raises(RuntimeError, match="invalid ttl"):
        await store.put_many([], [], ttl=ttl)
