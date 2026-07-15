"""Memory store atomic CAS/revision Python tests."""

import asyncio

import pytest

from openkeyv import MemoryStore


async def test_get_with_revision_missing() -> None:
    store = MemoryStore()
    assert await store.get_with_revision("missing") is None


async def test_cas_create_if_absent_success() -> None:
    store = MemoryStore()
    result = await store.compare_and_swap("k", None, "v")
    assert result.applied is True
    assert result.revision is not None
    assert result.current is None

    observed = await store.get_with_revision("k")
    assert observed is not None
    assert observed.value == "v"
    assert observed.revision == result.revision
    assert observed.ttl is None


async def test_cas_create_if_absent_existing_conflict() -> None:
    store = MemoryStore()
    await store.put("k", "v")
    result = await store.compare_and_swap("k", None, "other")
    assert result.applied is False
    assert result.current is not None
    assert result.current.value == "v"
    assert result.current.ttl is None


async def test_cas_exact_revision_update() -> None:
    store = MemoryStore()
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


async def test_cas_stale_revision_conflict() -> None:
    store = MemoryStore()
    await store.put("k", 1)
    first = await store.get_with_revision("k")
    assert first is not None
    await store.put("k", 2)

    result = await store.compare_and_swap("k", first.revision, 3)
    assert result.applied is False
    assert result.current is not None
    assert result.current.value == 2


async def test_cas_same_value_changes_revision() -> None:
    store = MemoryStore()
    await store.put("k", "same")
    before = await store.get_with_revision("k")
    assert before is not None

    result = await store.compare_and_swap("k", before.revision, "same")
    assert result.applied is True
    assert result.revision != before.revision


async def test_cas_new_ttl_replaces_old_ttl() -> None:
    store = MemoryStore()
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


async def test_cas_conflict_does_not_refresh_ttl() -> None:
    store = MemoryStore()
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
    assert abs(after.ttl - ttl_before) < 1.0


async def test_cas_expired_treated_as_absent() -> None:
    store = MemoryStore()
    await store.put("k", "v", ttl=0.01)
    await asyncio.sleep(0.05)

    result = await store.compare_and_swap("k", None, "rebuilt")
    assert result.applied is True


async def test_compare_and_delete_success() -> None:
    store = MemoryStore()
    await store.put("k", "v")
    observed = await store.get_with_revision("k")
    assert observed is not None

    result = await store.compare_and_delete("k", observed.revision)
    assert result.deleted is True
    assert result.current is None
    assert await store.get("k") is None


async def test_compare_and_delete_stale_conflict() -> None:
    store = MemoryStore()
    await store.put("k", "v")
    first = await store.get_with_revision("k")
    assert first is not None
    await store.put("k", "v2")

    result = await store.compare_and_delete("k", first.revision)
    assert result.deleted is False
    assert result.current is not None
    assert result.current.value == "v2"


async def test_compare_and_delete_missing_conflict() -> None:
    store = MemoryStore()
    # Cannot construct Revision directly; get one from a real op then use it on a missing key.
    await store.put("seed", "v")
    seed_rev = await store.get_with_revision("seed")
    assert seed_rev is not None
    result = await store.compare_and_delete("missing", seed_rev.revision)
    assert result.deleted is False
    assert result.current is None


async def test_revision_equality_and_hash() -> None:
    store = MemoryStore()
    await store.put("k", "v")
    observed = await store.get_with_revision("k")
    assert observed is not None
    # Same token is equal and hashes equal.
    assert observed.revision == observed.revision
    assert hash(observed.revision) == hash(observed.revision)
    # Different revisions differ.
    await store.put("k2", "v")
    other = await store.get_with_revision("k2")
    assert other is not None
    assert observed.revision != other.revision


async def test_revision_not_orderable() -> None:
    store = MemoryStore()
    await store.put("k", "v")
    observed = await store.get_with_revision("k")
    assert observed is not None
    with pytest.raises(TypeError):
        _ = observed.revision < observed.revision  # type: ignore[operator]


async def test_concurrent_cas_exactly_one_success() -> None:
    store = MemoryStore()
    await store.put("k", "seed")
    observed = await store.get_with_revision("k")
    assert observed is not None
    rev = observed.revision

    results = await asyncio.gather(*(store.compare_and_swap("k", rev, i) for i in range(8)))
    applied = [r for r in results if r.applied]
    assert len(applied) == 1


async def test_concurrent_create_if_absent_exactly_one_success() -> None:
    store = MemoryStore()
    results = await asyncio.gather(*(store.compare_and_swap("k", None, i) for i in range(8)))
    applied = [r for r in results if r.applied]
    assert len(applied) == 1


async def test_revision_is_not_constructible() -> None:
    from openkeyv import Revision

    with pytest.raises(TypeError):
        Revision()  # type: ignore[call-arg]
