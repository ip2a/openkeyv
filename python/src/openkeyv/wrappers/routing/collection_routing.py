from collections.abc import Mapping
from types import MappingProxyType

from openkeyv.errors import RoutingError
from openkeyv.protocols.key_value import AsyncKeyValue
from openkeyv.wrappers.routing.wrapper import RoutingWrapper


class CollectionRoutingWrapper(RoutingWrapper):
    """Routes operations based on collection name using a simple map.

    This is a convenience wrapper that provides collection-based routing using a
    dictionary mapping collection names to stores. This is useful for directing
    different data types to different backing stores.

    Example:
        router = CollectionRoutingWrapper(
            collection_map={
                "sessions": redis_store,
                "users": dynamo_store,
                "cache": memory_store,
                None: disk_store,
            }
        )
    """

    _collection_map: MappingProxyType[str | None, AsyncKeyValue]

    def __init__(self, collection_map: Mapping[str | None, AsyncKeyValue]) -> None:
        """Initialize collection-based routing.

        Args:
            collection_map: Mapping from collection name to store. Each collection
                name, including None for the default collection, must be mapped
                explicitly.
        """
        self._collection_map = MappingProxyType(mapping=dict(collection_map))

        def route_by_collection(collection: str | None) -> AsyncKeyValue:
            try:
                return self._collection_map[collection]
            except KeyError as error:
                raise RoutingError(collection) from error

        super().__init__(routing_function=route_by_collection)
