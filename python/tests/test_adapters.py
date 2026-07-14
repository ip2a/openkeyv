from dataclasses import dataclass
from typing import cast

import pytest
from pydantic import BaseModel, ConfigDict

from openkeyv import MemoryStore
from openkeyv.adapters import BaseModelAdapter, DataclassAdapter, PydanticAdapter, RaiseOnMissingAdapter
from openkeyv.errors import DeserializationError, MissingKeyError, SerializationError
from openkeyv.protocols.key_value import AsyncKeyValue


@dataclass
class Record:
    name: str
    count: int


class User(BaseModel):
    name: str
    age: int


class Opaque:
    pass


class ContainsOpaque(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: Opaque


def _memory_store() -> AsyncKeyValue:
    return cast("AsyncKeyValue", MemoryStore())


async def test_dataclass_adapter_roundtrips_single_and_list_values() -> None:
    backing = _memory_store()
    single = DataclassAdapter[Record](backing, Record, default_collection="records")
    multiple = DataclassAdapter[list[Record]](backing, list[Record], default_collection="record-lists")
    record = Record(name="one", count=1)
    records = [record, Record(name="two", count=2)]

    await single.put("one", record)
    await multiple.put("many", records)

    assert await backing.get("one", collection="records") == {"name": "one", "count": 1}
    assert await backing.get("many", collection="record-lists") == {"items": [{"name": "one", "count": 1}, {"name": "two", "count": 2}]}
    assert await single.get("one") == record
    assert await multiple.get("many") == records
    assert await single.get("missing", default=record) == record
    assert await single.get_many(["one", "missing"], default=records[1]) == [record, records[1]]


async def test_dataclass_adapter_rejects_invalid_stored_payload() -> None:
    backing = _memory_store()
    adapter = DataclassAdapter[Record](backing, Record)
    await backing.put("invalid", {"name": "missing-count"})

    with pytest.raises(DeserializationError):
        await adapter.get("invalid")


async def test_base_model_adapter_roundtrips_single_and_list_values() -> None:
    backing = _memory_store()
    single = BaseModelAdapter[User](backing, User, default_collection="users")
    multiple = BaseModelAdapter[list[User]](backing, list[User], default_collection="user-lists")
    user = User(name="Ada", age=36)
    users = [user, User(name="Grace", age=40)]

    await single.put("one", user, ttl=60)
    await multiple.put_many(["many"], [users])

    assert await single.get("one") == user
    assert await multiple.get_many(["many", "missing"]) == [users, None]
    value, ttl = await single.ttl("one")
    assert value == user
    assert ttl is not None


async def test_base_model_adapter_raises_strict_validation_errors() -> None:
    backing = _memory_store()
    adapter = BaseModelAdapter[User](backing, User)
    await backing.put("invalid", {"name": "Ada", "age": "not-an-integer"})

    with pytest.raises(DeserializationError):
        await adapter.get("invalid")


async def test_pydantic_adapter_stores_dict_types_directly_and_wraps_other_types() -> None:
    backing = _memory_store()
    mapping_adapter = PydanticAdapter[dict[str, int]](backing, dict[str, int])
    scalar_adapter = PydanticAdapter[int](backing, int)
    list_adapter = PydanticAdapter[list[int]](backing, list[int])

    await mapping_adapter.put("mapping", {"one": 1})
    await scalar_adapter.put("scalar", 2)
    await list_adapter.put("list", [3, 4])

    assert await backing.get("mapping") == {"one": 1}
    assert await backing.get("scalar") == {"items": 2}
    assert await backing.get("list") == {"items": [3, 4]}
    assert await mapping_adapter.get("mapping") == {"one": 1}
    assert await scalar_adapter.get("scalar") == 2
    assert await list_adapter.get("list") == [3, 4]


async def test_pydantic_adapter_raises_serialization_and_deserialization_errors() -> None:
    backing = _memory_store()
    opaque_adapter = PydanticAdapter[ContainsOpaque](backing, ContainsOpaque)
    list_adapter = PydanticAdapter[list[int]](backing, list[int])

    with pytest.raises(SerializationError):
        await opaque_adapter.put("opaque", ContainsOpaque(value=Opaque()))

    await backing.put("invalid-list", {"not-items": [1, 2]})
    with pytest.raises(DeserializationError):
        await list_adapter.get("invalid-list")


async def test_raise_on_missing_adapter_raises_for_all_read_paths() -> None:
    adapter = RaiseOnMissingAdapter(_memory_store())
    await adapter.put("present", {"value": 1})

    assert await adapter.get("present", raise_on_missing=True) == {"value": 1}
    assert await adapter.ttl("present", raise_on_missing=True) == ({"value": 1}, None)

    with pytest.raises(MissingKeyError):
        await adapter.get("missing", raise_on_missing=True)
    with pytest.raises(MissingKeyError):
        await adapter.get_many(["present", "missing"], raise_on_missing=True)
    with pytest.raises(MissingKeyError):
        await adapter.ttl("missing", raise_on_missing=True)
    with pytest.raises(MissingKeyError):
        await adapter.ttl_many(["present", "missing"], raise_on_missing=True)


async def test_raise_on_missing_adapter_passes_writes_and_deletes_through() -> None:
    adapter = RaiseOnMissingAdapter(_memory_store())

    await adapter.put_many(["one", "two"], [{"value": 1}, {"value": 2}])
    assert await adapter.get_many(["one", "missing", "two"]) == [{"value": 1}, None, {"value": 2}]
    assert await adapter.delete("one") is True
    assert await adapter.delete_many(["two", "missing"]) == 1
