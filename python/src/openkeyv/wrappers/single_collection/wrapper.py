from collections.abc import Mapping, Sequence
from typing import Any, SupportsFloat

from typing_extensions import override

from openkeyv._utils.compound import compound_key
from openkeyv._utils.constants import DEFAULT_COLLECTION_NAME
from openkeyv.protocols.key_value import AsyncKeyValue
from openkeyv.wrappers.base import BaseWrapper


class SingleCollectionWrapper(BaseWrapper):
    """A wrapper that stores all collections within a single backing collection via canonical identities."""

    def __init__(self, key_value: AsyncKeyValue, single_collection: str, default_collection: str | None = None) -> None:
        """Initialize the wrapper using canonical collection/key identities.

        Args:
            key_value: The store to wrap.
            single_collection: The single collection to use to store all collections.
            default_collection: The default collection to use if no collection is provided.
        """
        self.key_value: AsyncKeyValue = key_value
        self.single_collection: str = single_collection
        self.default_collection: str = DEFAULT_COLLECTION_NAME if default_collection is None else default_collection
        super().__init__()

    def _compound_key(self, key: str, collection: str | None = None) -> str:
        collection_to_use = self.default_collection if collection is None else collection
        return compound_key(collection=collection_to_use, key=key)

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        new_key: str = self._compound_key(key=key, collection=collection)
        return await self.key_value.get(key=new_key, collection=self.single_collection)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        new_keys: Sequence[str] = [self._compound_key(key=key, collection=collection) for key in keys]
        return await self.key_value.get_many(keys=new_keys, collection=self.single_collection)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        new_key: str = self._compound_key(key=key, collection=collection)
        return await self.key_value.ttl(key=new_key, collection=self.single_collection)

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        new_keys: Sequence[str] = [self._compound_key(key=key, collection=collection) for key in keys]
        return await self.key_value.ttl_many(keys=new_keys, collection=self.single_collection)

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        new_key: str = self._compound_key(key=key, collection=collection)
        return await self.key_value.put(key=new_key, value=value, collection=self.single_collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        new_keys: Sequence[str] = [self._compound_key(key=key, collection=collection) for key in keys]
        return await self.key_value.put_many(keys=new_keys, values=values, collection=self.single_collection, ttl=ttl)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        new_key: str = self._compound_key(key=key, collection=collection)
        return await self.key_value.delete(key=new_key, collection=self.single_collection)

    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        new_keys: Sequence[str] = [self._compound_key(key=key, collection=collection) for key in keys]
        return await self.key_value.delete_many(keys=new_keys, collection=self.single_collection)
