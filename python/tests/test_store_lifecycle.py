from typing import Any, cast

import pytest

from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv.errors import StoreSetupError
from openkeyv.stores.base import BaseContextManagerStore


class TrackingContextStore(BaseContextManagerStore):
    def __init__(self, *, fail_setup_once: bool = False) -> None:
        self.entries: dict[tuple[str, str], ManagedEntry] = {}
        self.setup_calls = 0
        self.collection_setup_calls: list[str] = []
        self.cleanup_calls = 0
        self.fail_setup_once = fail_setup_once
        super().__init__(stable_api=True)

    def _cleanup(self) -> None:
        self.cleanup_calls += 1

    async def _setup(self) -> None:
        self.setup_calls += 1
        self._exit_stack.callback(self._cleanup)
        if self.fail_setup_once:
            self.fail_setup_once = False
            msg = "setup failed"
            raise RuntimeError(msg)

    async def _setup_collection(self, *, collection: str) -> None:
        self.collection_setup_calls.append(collection)

    async def _get_managed_entry(self, *, collection: str, key: str) -> ManagedEntry | None:
        return self.entries.get((collection, key))

    async def _put_managed_entry(self, *, collection: str, key: str, managed_entry: ManagedEntry) -> None:
        self.entries[(collection, key)] = managed_entry

    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        return self.entries.pop((collection, key), None) is not None


async def test_context_store_cleans_up_failed_setup_before_retry() -> None:
    store = TrackingContextStore(fail_setup_once=True)

    with pytest.raises(StoreSetupError, match="setup failed"):
        await store.setup()

    assert store.setup_calls == 1
    assert store.cleanup_calls == 1

    await store.setup()
    assert store.setup_calls == 2

    await store.close()
    assert store.cleanup_calls == 2


async def test_context_store_setup_and_collection_setup_are_idempotent() -> None:
    store = TrackingContextStore()

    await store.put("one", {"value": 1})
    await store.put("two", {"value": 2})
    await store.setup()

    assert store.setup_calls == 1
    assert store.collection_setup_calls == [store.default_collection]

    async with store as entered:
        assert entered is store

    assert store.cleanup_calls == 1
    await store.close()
    assert store.cleanup_calls == 1


async def test_put_many_rejects_non_mapping_values_before_setup() -> None:
    store = TrackingContextStore()
    invalid_values = cast("list[dict[str, Any]]", [object()])

    with pytest.raises(TypeError):
        await store.put_many(["key"], invalid_values)

    assert store.setup_calls == 0
    assert store.entries == {}
