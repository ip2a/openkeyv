from pathlib import Path
from typing import Any

import pytest

from openkeyv import DiskStore, MemoryStore, SimpleStore, SqliteStore


@pytest.fixture(params=["memory", "simple", "disk", "sqlite"])
def local_store(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    store_type = request.param
    if store_type == "memory":
        return MemoryStore()
    if store_type == "simple":
        return SimpleStore()
    if store_type == "disk":
        return DiskStore(path=str(tmp_path / "disk"))
    if store_type == "sqlite":
        return SqliteStore(path=str(tmp_path / "aggregate.sqlite"))
    raise AssertionError


async def test_local_stores_preserve_exact_identity_across_all_core_paths(local_store: Any) -> None:
    cases = [
        ("", ""),
        ("Users", "same"),
        ("users", "same"),
        ("e\u0301", "unicode"),
        ("é", "unicode"),
        ("*?[\\]", "line\nnull\0/:*?[]\\"),
        ("a:b", "c"),
        ("a", "b:c"),
    ]

    for index, (collection, key) in enumerate(cases):
        await local_store.put(key, f"value-{index}", collection=collection)

    for index, (collection, key) in enumerate(cases):
        assert await local_store.get(key, collection=collection) == f"value-{index}"

    batch_collection = "batch:*?[\\]"
    batch_keys = ["", "line\nnull\0/:*?[]\\", "Users", "users"]
    batch_values = ["empty", "special", "upper", "lower"]
    await local_store.put_many(batch_keys, batch_values, collection=batch_collection)
    assert await local_store.get_many(batch_keys, collection=batch_collection) == batch_values
    assert await local_store.ttl(batch_keys[1], collection=batch_collection) == ("special", None)


async def test_local_stores_keep_case_distinct_collections_and_destroy_only_one(local_store: Any) -> None:
    await local_store.put("same", "upper", collection="Users")
    await local_store.put("same", "lower", collection="users")

    assert await local_store.keys(collection="Users") == ["same"]
    assert await local_store.keys(collection="users") == ["same"]
    collections = await local_store.collections()
    assert "Users" in collections
    assert "users" in collections

    assert await local_store.destroy_collection("Users") is True
    assert await local_store.get("same", collection="Users") is None
    assert await local_store.get("same", collection="users") == "lower"

    assert await local_store.destroy_collection("users") is True
    assert await local_store.get("same", collection="users") is None
