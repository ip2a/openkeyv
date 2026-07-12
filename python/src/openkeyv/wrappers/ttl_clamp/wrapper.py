import math
from collections.abc import Mapping, Sequence
from typing import Any, SupportsFloat, overload

from typing_extensions import override

from openkeyv._utils.time_to_live import prepare_ttl
from openkeyv.protocols.key_value import AsyncKeyValue
from openkeyv.wrappers.base import BaseWrapper


class TTLClampWrapper(BaseWrapper):
    """Wrapper that enforces a maximum TTL for puts into the store.

    This wrapper only modifies write operations (put, put_many). All read operations
    (get, get_many, ttl, ttl_many, delete, delete_many) pass through unchanged to
    the underlying store.
    """

    def __init__(
        self, key_value: AsyncKeyValue, min_ttl: SupportsFloat, max_ttl: SupportsFloat, missing_ttl: SupportsFloat | None = None
    ) -> None:
        """Initialize the TTL clamp wrapper.

        Args:
            key_value: The store to wrap.
            min_ttl: The minimum TTL for puts into the store.
            max_ttl: The maximum TTL for puts into the store.
            missing_ttl: The TTL to use for entries that do not have a TTL. Defaults to None.
        """
        if isinstance(min_ttl, bool) or isinstance(max_ttl, bool) or isinstance(missing_ttl, bool):
            msg = "TTL clamp values must be finite numbers"
            raise TypeError(msg)

        min_ttl = float(min_ttl)
        max_ttl = float(max_ttl)
        missing_ttl = float(missing_ttl) if missing_ttl is not None else None

        if not math.isfinite(min_ttl) or min_ttl < 0:
            msg = "min_ttl must be finite and greater than or equal to zero"
            raise ValueError(msg)
        if not math.isfinite(max_ttl) or max_ttl <= 0:
            msg = "max_ttl must be finite and greater than zero"
            raise ValueError(msg)
        if min_ttl > max_ttl:
            msg = "min_ttl must be less than or equal to max_ttl"
            raise ValueError(msg)
        if missing_ttl is not None and (not math.isfinite(missing_ttl) or missing_ttl <= 0):
            msg = "missing_ttl must be finite and greater than zero"
            raise ValueError(msg)

        self.key_value: AsyncKeyValue = key_value
        self.min_ttl = min_ttl
        self.max_ttl = max_ttl
        self.missing_ttl = missing_ttl

        super().__init__()

    @overload
    def _ttl_clamp(self, ttl: SupportsFloat) -> float: ...

    @overload
    def _ttl_clamp(self, ttl: SupportsFloat | None) -> float | None: ...

    def _ttl_clamp(self, ttl: SupportsFloat | None) -> float | None:
        if ttl is None:
            ttl = self.missing_ttl
            if ttl is None:
                return None

        ttl = prepare_ttl(t=ttl)

        return max(self.min_ttl, min(ttl, self.max_ttl))

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        await self.key_value.put(collection=collection, key=key, value=value, ttl=self._ttl_clamp(ttl=ttl))

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        await self.key_value.put_many(keys=keys, values=values, collection=collection, ttl=self._ttl_clamp(ttl=ttl))
