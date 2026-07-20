"""TTL handling and datetime utilities.

This module provides functions for working with TTL (time-to-live) values
and datetime conversions used throughout the key-value stores.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, SupportsFloat, overload

from openkeyv._internal import _prepare_entry_timestamps
from openkeyv.errors import InvalidTTLError


def epoch_to_datetime(epoch: float) -> datetime:
    """Convert an epoch timestamp to a datetime object."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def now_as_epoch() -> float:
    """Get the current time as epoch seconds."""
    return time.time()


def now() -> datetime:
    """Get the current time as a datetime object."""
    return datetime.now(tz=timezone.utc)


def seconds_to(datetime: datetime) -> float:
    """Get the number of seconds between the current time and a datetime object."""
    return (datetime - now()).total_seconds()


def now_plus(seconds: float) -> datetime:
    """Get the current time plus a number of seconds as a datetime object."""
    return datetime.now(tz=timezone.utc) + timedelta(seconds=seconds)


def try_parse_datetime_str(value: Any) -> datetime | None:
    """Try to parse a datetime string, returning None on failure."""
    try:
        if isinstance(value, str):
            return datetime.fromisoformat(value)
    except ValueError:
        return None

    return None


@overload
def prepare_ttl(t: SupportsFloat) -> float: ...


@overload
def prepare_ttl(t: SupportsFloat | None) -> float | None: ...


def prepare_ttl(t: SupportsFloat | None) -> float | None:
    """Validate a TTL with the Rust core and return it as seconds."""
    if t is None:
        return None
    if isinstance(t, bool):
        raise InvalidTTLError(ttl=t, extra_info={"type": type(t).__name__})
    try:
        ttl = float(t)
        _prepare_entry_timestamps(ttl)
    except (TypeError, ValueError, OverflowError) as error:
        raise InvalidTTLError(ttl=t, extra_info={"type": type(t).__name__}) from error
    return ttl


def prepare_entry_timestamps(ttl: SupportsFloat | None) -> tuple[datetime, float | None, datetime | None]:
    """Create entry timestamps with the Rust core TTL semantics."""
    if isinstance(ttl, bool):
        raise InvalidTTLError(ttl=ttl, extra_info={"type": type(ttl).__name__})
    try:
        ttl_seconds = None if ttl is None else float(ttl)
        created_at_millis, expires_at_millis = _prepare_entry_timestamps(ttl_seconds)
    except (TypeError, ValueError, OverflowError) as error:
        raise InvalidTTLError(ttl=ttl, extra_info={"type": type(ttl).__name__}) from error

    created_at = datetime.fromtimestamp(created_at_millis / 1000, tz=timezone.utc)
    expires_at = None if expires_at_millis is None else datetime.fromtimestamp(expires_at_millis / 1000, tz=timezone.utc)
    return created_at, ttl_seconds, expires_at
