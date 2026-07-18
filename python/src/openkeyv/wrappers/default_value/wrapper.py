from collections.abc import Sequence
from typing import SupportsFloat

from typing_extensions import override

from openkeyv._utils.managed_entry import dump_to_json, load_from_json
from openkeyv.protocols.key_value import AsyncKeyValue, StoreValue
from openkeyv.wrappers.base import BaseWrapper


class DefaultValueWrapper(BaseWrapper):
    """A wrapper that returns a default value when a key is not found.

    This wrapper provides dict.get(key, default) behavior for the key-value store,
    allowing you to specify a default value to return instead of None when a key doesn't exist.

    It does not store the default value in the underlying key-value store and the TTL returned with the default
    value is hard-coded based on the default_ttl parameter. Picking a default_ttl requires careful consideration
    of how the value will be used and if any other wrappers will be used that may rely on the TTL.
    """

    _default_ttl: float | None
    _default_value_json: str

    def __init__(
        self,
        key_value: AsyncKeyValue,
        default_value: StoreValue,
        default_ttl: SupportsFloat | None = None,
    ) -> None:
        """Initialize the DefaultValueWrapper.

        Args:
            key_value: The underlying key-value store to wrap.
            default_value: The default value to return when a key is not found.
            default_ttl: The TTL to return to the caller for default values. Defaults to None.
        """
        self.key_value = key_value
        self._default_value_json = dump_to_json(obj=default_value)
        self._default_ttl = None if default_ttl is None else float(default_ttl)

        super().__init__()

    def _new_default_value(self) -> StoreValue:
        return load_from_json(json_str=self._default_value_json)

    @override
    async def get(self, key: str, *, collection: str | None = None) -> StoreValue:
        result = await self.key_value.get(key=key, collection=collection)
        return result if result is not None else self._new_default_value()

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[StoreValue]:
        results = await self.key_value.get_many(keys=keys, collection=collection)
        return [result if result is not None else self._new_default_value() for result in results]

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[StoreValue, float | None]:
        result, ttl_value = await self.key_value.ttl(key=key, collection=collection)
        if result is None:
            return (self._new_default_value(), self._default_ttl)
        return (result, ttl_value)

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[StoreValue, float | None]]:
        results = await self.key_value.ttl_many(keys=keys, collection=collection)
        return [
            (result, ttl_value) if result is not None else (self._new_default_value(), self._default_ttl) for result, ttl_value in results
        ]
