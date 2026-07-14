import asyncio

import pytest

from openkeyv import MemoryStore, NullStore, SimpleStore


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


async def test_batch_size_mismatch_raises() -> None:
    with pytest.raises(RuntimeError, match="batch size mismatch"):
        await MemoryStore().put_many(["key"], [])


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
