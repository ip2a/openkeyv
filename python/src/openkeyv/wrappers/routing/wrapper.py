from collections.abc import Callable, Sequence
from typing import SupportsFloat, cast

from typing_extensions import override

from openkeyv.errors import RoutingError
from openkeyv.protocols.key_value import AsyncKeyValue, StoreValue
from openkeyv.wrappers.base import BaseWrapper

RoutingFunction = Callable[[str | None], AsyncKeyValue]


class RoutingWrapper(BaseWrapper):
    """Routes operations to different stores based on a routing function.

    The routing function receives the collection name and returns the appropriate store.
    This allows dynamic routing of requests to different backing stores based on
    collection name or other custom logic.

    Example:
        def route_by_collection(collection: str | None) -> AsyncKeyValue:
            if collection == "sessions":
                return redis_store
            if collection == "users":
                return dynamo_store
            raise RoutingError(collection)

        router = RoutingWrapper(routing_function=route_by_collection)
    """

    _routing_function: RoutingFunction

    def __init__(self, routing_function: RoutingFunction) -> None:
        """Initialize the routing wrapper.

        Args:
            routing_function: Function that takes a collection name and returns the store to use.
                It must raise when no store is configured.
        """
        self._routing_function = routing_function

        super().__init__()

    def _get_store(self, collection: str | None) -> AsyncKeyValue:
        """Get the appropriate store for the given collection.

        Args:
            collection: The collection name to route.

        Returns:
            The AsyncKeyValue store to use for this collection.

        Raises:
            RoutingError: The routing function did not return a store.
        """
        store = cast("AsyncKeyValue | None", self._routing_function(collection))
        if store is None:
            raise RoutingError(collection)
        return store

    @override
    async def get(self, key: str, *, collection: str | None = None) -> StoreValue:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.get(key=key, collection=collection)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[StoreValue]:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.get_many(keys=keys, collection=collection)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[StoreValue, float | None]:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.ttl(key=key, collection=collection)

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[StoreValue, float | None]]:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.ttl_many(keys=keys, collection=collection)

    @override
    async def put(self, key: str, value: StoreValue, *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.put(key=key, value=value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[StoreValue],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.put_many(keys=keys, values=values, collection=collection, ttl=ttl)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.delete(key=key, collection=collection)

    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        store: AsyncKeyValue = self._get_store(collection)
        return await store.delete_many(keys=keys, collection=collection)
