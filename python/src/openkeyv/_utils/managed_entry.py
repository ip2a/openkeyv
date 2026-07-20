"""ManagedEntry dataclass for storing values with metadata.

The ManagedEntry class wraps stored values with metadata including creation time
and expiration time. This allows stores to track TTL information consistently.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import SupportsFloat

from typing_extensions import Self

from openkeyv._utils.time_to_live import now, now_plus, prepare_ttl
from openkeyv.protocols.key_value import StoreValue


@dataclass(kw_only=True)
class ManagedEntry:
    """A managed cache entry containing value data and TTL metadata.

    The entry supports either TTL seconds or absolute expiration datetime. On init:
    - If `ttl` is provided but `expires_at` is not, an `expires_at` will be computed.
    - If `expires_at` is provided but `ttl` is not, a live TTL will be computed on access.
    """

    value: StoreValue

    created_at: datetime | None = field(default=None)
    expires_at: datetime | None = field(default=None)

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return int(self.expires_at.timestamp() * 1000) <= int(now().timestamp() * 1000)

    @property
    def ttl(self) -> float | None:
        if self.expires_at is None:
            return None
        remaining_millis = int(self.expires_at.timestamp() * 1000) - int(now().timestamp() * 1000)
        return max(remaining_millis, 0) / 1000

    @property
    def created_at_isoformat(self) -> str | None:
        return self.created_at.isoformat() if self.created_at else None

    @property
    def expires_at_isoformat(self) -> str | None:
        return self.expires_at.isoformat() if self.expires_at else None

    @classmethod
    def from_ttl(cls, *, value: StoreValue, created_at: datetime | None = None, ttl: SupportsFloat) -> Self:
        ttl_seconds = prepare_ttl(t=ttl)
        return cls(
            value=value,
            created_at=created_at,
            expires_at=now_plus(seconds=ttl_seconds),
        )
