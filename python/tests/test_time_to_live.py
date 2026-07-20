from datetime import timedelta

import pytest

from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv._utils.time_to_live import prepare_entry_timestamps, prepare_ttl
from openkeyv.errors import InvalidTTLError


@pytest.mark.parametrize("ttl", [0, -1, float("nan"), float("inf"), float("-inf"), 1e300, True])
def test_prepare_ttl_rejects_values_outside_core_contract(ttl: object) -> None:
    with pytest.raises(InvalidTTLError):
        prepare_ttl(ttl)  # type: ignore[arg-type]


def test_prepare_entry_timestamps_use_core_millisecond_precision() -> None:
    created_at, ttl, expires_at = prepare_entry_timestamps(1.5)

    assert ttl == 1.5
    assert expires_at is not None
    assert (expires_at - created_at) == timedelta(milliseconds=1500)


def test_managed_entry_ttl_uses_millisecond_precision() -> None:
    entry = ManagedEntry.from_ttl(value="value", ttl=1.5)

    assert entry.ttl is not None
    assert 0 < entry.ttl <= 1.5
