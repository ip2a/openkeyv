"""Redis store atomic CAS/revision Python tests.

Requires a real Redis service. Set ``OPENKEYV_REDIS_URL`` (default
``redis://127.0.0.1:16379``) to enable these integration tests.
"""

import asyncio
import os

import pytest

from openkeyv import RedisStore

_REDIS_URL = os.environ.get("OPENKEYV_REDIS_URL", "redis://127.0.0.1:16379")
_skip_if_no_redis = pytest.mark.skipif(
    not os.environ.get("OPENKEYV_REDIS_URL"),
    reason="requires OPENKEYV_REDIS_URL",
)


@_skip_if_no_redis
async def test_redis_get_with_revision_missing() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    assert await store.get_with_revision("missing") is None


@_skip_if_no_redis
async def test_redis_cas_create_if_absent_success() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    result = await store.compare_and_swap("k", None, "v")
    assert result.applied is True
    assert result.revision is not None
    assert result.current is None

    observed = await store.get_with_revision("k")
    assert observed is not None
    assert observed.value == "v"
    assert observed.revision == result.revision
    assert observed.ttl is None


@_skip_if_no_redis
async def test_redis_cas_create_if_absent_existing_conflict() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v")
    result = await store.compare_and_swap("k", None, "other")
    assert result.applied is False
    assert result.current is not None
    assert result.current.value == "v"
    assert result.current.ttl is None


@_skip_if_no_redis
async def test_redis_cas_exact_revision_update() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", 1)
    observed = await store.get_with_revision("k")
    assert observed is not None

    result = await store.compare_and_swap("k", observed.revision, 2)
    assert result.applied is True
    assert result.revision != observed.revision

    after = await store.get_with_revision("k")
    assert after is not None
    assert after.value == 2
    assert after.revision == result.revision


@_skip_if_no_redis
async def test_redis_cas_stale_revision_conflict() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", 1)
    first = await store.get_with_revision("k")
    assert first is not None
    await store.put("k", 2)

    result = await store.compare_and_swap("k", first.revision, 3)
    assert result.applied is False
    assert result.current is not None
    assert result.current.value == 2


@_skip_if_no_redis
async def test_redis_cas_same_value_changes_revision() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "same")
    before = await store.get_with_revision("k")
    assert before is not None

    result = await store.compare_and_swap("k", before.revision, "same")
    assert result.applied is True
    assert result.revision != before.revision


@_skip_if_no_redis
async def test_redis_cas_new_ttl_replaces_old_ttl() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v", ttl=100.0)
    observed = await store.get_with_revision("k")
    assert observed is not None
    assert observed.ttl is not None

    result = await store.compare_and_swap("k", observed.revision, "v2")
    assert result.applied is True

    after = await store.get_with_revision("k")
    assert after is not None
    assert after.value == "v2"
    assert after.ttl is None


@_skip_if_no_redis
async def test_redis_cas_conflict_does_not_refresh_ttl() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v", ttl=100.0)
    observed = await store.get_with_revision("k")
    assert observed is not None
    ttl_before = observed.ttl
    assert ttl_before is not None

    # Force a conflict using a wrong revision; the entry must not be touched.
    await store.put("other", "x")
    other = await store.get_with_revision("other")
    assert other is not None
    result = await store.compare_and_swap("k", other.revision, "y")
    assert result.applied is False
    assert result.current is not None
    after = await store.get_with_revision("k")
    assert after is not None
    assert after.ttl is not None
    assert abs(after.ttl - ttl_before) < 5.0


@_skip_if_no_redis
async def test_redis_cas_expired_treated_as_absent() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v", ttl=0.5)
    await asyncio.sleep(1.0)

    result = await store.compare_and_swap("k", None, "rebuilt")
    assert result.applied is True


@_skip_if_no_redis
async def test_redis_compare_and_delete_success() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v")
    observed = await store.get_with_revision("k")
    assert observed is not None

    result = await store.compare_and_delete("k", observed.revision)
    assert result.deleted is True
    assert result.current is None
    assert await store.get("k") is None


@_skip_if_no_redis
async def test_redis_compare_and_delete_stale_conflict() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v")
    first = await store.get_with_revision("k")
    assert first is not None
    await store.put("k", "v2")

    result = await store.compare_and_delete("k", first.revision)
    assert result.deleted is False
    assert result.current is not None
    assert result.current.value == "v2"


@_skip_if_no_redis
async def test_redis_compare_and_delete_missing_conflict() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    # Cannot construct Revision directly; get one from a real op then use it on a missing key.
    await store.put("seed", "v")
    seed_rev = await store.get_with_revision("seed")
    assert seed_rev is not None
    result = await store.compare_and_delete("missing", seed_rev.revision)
    assert result.deleted is False
    assert result.current is None


@_skip_if_no_redis
async def test_redis_revision_equality_and_hash() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v")
    observed = await store.get_with_revision("k")
    assert observed is not None
    assert observed.revision == observed.revision
    assert hash(observed.revision) == hash(observed.revision)
    await store.put("k2", "v")
    other = await store.get_with_revision("k2")
    assert other is not None
    assert observed.revision != other.revision


@_skip_if_no_redis
async def test_redis_revision_not_orderable() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "v")
    observed = await store.get_with_revision("k")
    assert observed is not None
    with pytest.raises(TypeError):
        _ = observed.revision < observed.revision  # type: ignore[operator]


@_skip_if_no_redis
async def test_redis_concurrent_cas_exactly_one_success() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    await store.put("k", "seed")
    observed = await store.get_with_revision("k")
    assert observed is not None
    rev = observed.revision

    results = await asyncio.gather(*(store.compare_and_swap("k", rev, i) for i in range(8)))
    applied = [r for r in results if r.applied]
    assert len(applied) == 1


@_skip_if_no_redis
async def test_redis_concurrent_create_if_absent_exactly_one_success() -> None:
    store = RedisStore(_REDIS_URL)
    await store.destroy()
    results = await asyncio.gather(*(store.compare_and_swap("k", None, i) for i in range(8)))
    applied = [r for r in results if r.applied]
    assert len(applied) == 1


@_skip_if_no_redis
async def test_redis_revision_is_not_constructible() -> None:
    from openkeyv import Revision

    with pytest.raises(TypeError):
        Revision()  # type: ignore[call-arg]
