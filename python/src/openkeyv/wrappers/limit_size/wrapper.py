from collections.abc import Sequence
from typing import SupportsFloat

from typing_extensions import override

from openkeyv._utils.managed_entry import estimate_serialized_size
from openkeyv.errors import EntryTooLargeError, EntryTooSmallError
from openkeyv.protocols.key_value import AsyncKeyValue, StoreValue
from openkeyv.wrappers.base import BaseWrapper


class LimitSizeWrapper(BaseWrapper):
    """Wrapper that rejects entries outside the configured serialized size limits.

    This wrapper checks the serialized size of values before storing them. This incurs a performance penalty
    as it requires JSON serialization of the value separate from serialization that occurs when the value is stored.

    This wrapper does not prevent returning objects (get, ttl, get_many, ttl_many) that exceed the size limit, just storing
    them (put, put_many).
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        *,
        min_size: int | None = None,
        max_size: int | None = None,
    ) -> None:
        """Initialize the limit size wrapper.

        Args:
            key_value: The store to wrap.
            min_size: The minimum size (in bytes) allowed for each entry. If None, no minimum size is enforced.
            max_size: The maximum size (in bytes) allowed for each entry. If None, no maximum size is enforced.
        """
        self.key_value: AsyncKeyValue = key_value
        self.min_size: int | None = min_size
        self.max_size: int | None = max_size

        super().__init__()

    def _validate_size(self, value: StoreValue, *, collection: str | None = None, key: str | None = None) -> None:
        """Raise when a value is outside the configured size limits.

        Args:
            value: The value to check.
            collection: The collection name (for error messages).
            key: The key name (for error messages).

        Raises:
            EntryTooSmallError: The value is smaller than min_size.
            EntryTooLargeError: The value is larger than max_size.
        """

        item_size: int = estimate_serialized_size(value=value)

        if self.min_size is not None and item_size < self.min_size:
            raise EntryTooSmallError(size=item_size, min_size=self.min_size, collection=collection, key=key)

        if self.max_size is not None and item_size > self.max_size:
            raise EntryTooLargeError(size=item_size, max_size=self.max_size, collection=collection, key=key)

    @override
    async def put(self, key: str, value: StoreValue, *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        self._validate_size(value=value, collection=collection, key=key)
        await self.key_value.put(collection=collection, key=key, value=value, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[StoreValue],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        for key, value in zip(keys, values, strict=True):
            self._validate_size(value=value, collection=collection, key=key)

        await self.key_value.put_many(keys=keys, values=values, collection=collection, ttl=ttl)
