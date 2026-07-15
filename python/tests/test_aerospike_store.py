from typing import Any

import aerospike
import pytest

from openkeyv.stores.aerospike import AerospikeStore


class FakeScan:
    def __init__(self, records: dict[tuple[str, str, str], tuple[dict[str, Any], dict[str, Any]]]) -> None:
        self._records = records

    def foreach(self, callback: Any) -> None:
        for key, (metadata, bins) in self._records.items():
            callback((key, metadata, bins))


class FakeAerospikeClient:
    def __init__(self) -> None:
        self.records: dict[tuple[str, str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
        self.put_calls: list[tuple[tuple[str, str, str], dict[str, Any], dict[str, Any]]] = []
        self.put_policies: list[dict[str, Any] | None] = []
        self.info_response: object = {"node": (None, "test;other")}
        self.get_result: object | None = None
        self.get_error: Exception | None = None
        self.connected = False
        self.truncated = False

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.connected = False

    def info_all(self, command: str) -> object:
        assert command == "namespaces"
        return self.info_response

    def get(self, key: tuple[str, str, str]) -> object:
        if self.get_error is not None:
            raise self.get_error
        if self.get_result is not None:
            return self.get_result
        try:
            metadata, bins = self.records[key]
        except KeyError as error:
            raise aerospike.exception.RecordNotFound from error
        return (key, metadata, bins)

    def put(
        self,
        key: tuple[str, str, str],
        bins: dict[str, Any],
        *,
        meta: dict[str, Any],
        policy: dict[str, Any] | None = None,
    ) -> None:
        self.records[key] = (dict(meta), dict(bins))
        self.put_calls.append((key, dict(bins), dict(meta)))
        self.put_policies.append(None if policy is None else dict(policy))

    def remove(self, key: tuple[str, str, str]) -> None:
        try:
            del self.records[key]
        except KeyError as error:
            raise aerospike.exception.RecordNotFound from error

    def scan(self, namespace: str, set_name: str) -> FakeScan:
        records = {key: value for key, value in self.records.items() if key[:2] == (namespace, set_name)}
        return FakeScan(records)

    def truncate(self, namespace: str, set_name: str, before_nanos: int) -> None:
        assert before_nanos == 0
        self.records = {key: value for key, value in self.records.items() if key[:2] != (namespace, set_name)}
        self.truncated = True


async def test_aerospike_public_roundtrip_uses_binary_entries_and_native_ttl() -> None:
    client = FakeAerospikeClient()
    store = AerospikeStore(client=client, namespace="test", set_name="entries")  # type: ignore[arg-type]

    await store.put("persistent", {"value": "kept"}, collection="items")
    persistent_call = client.put_calls[-1]
    assert persistent_call[0] == ("test", "entries", "5:itemspersistent")
    assert persistent_call[1]["value"].startswith(b"OKVE1")
    assert persistent_call[2] == {"ttl": aerospike.TTL_NEVER_EXPIRE}
    assert client.put_policies[-1] == {"key": aerospike.POLICY_KEY_SEND}
    assert await store.ttl("persistent", collection="items") == ({"value": "kept"}, None)

    await store.put("temporary", {"value": "short"}, collection="items", ttl=0.1)
    assert client.put_calls[-1][2] == {"ttl": 1}
    assert await store.get("temporary", collection="items") == {"value": "short"}

    await store.put("other", {"value": "hidden"}, collection="elsewhere")
    assert set(await store.keys(collection="items")) == {"persistent", "temporary"}
    assert await store.delete("persistent", collection="items") is True
    assert await store.delete("persistent", collection="items") is False
    assert await store.destroy() is True
    assert client.truncated is True
    assert client.records == {}


@pytest.mark.parametrize(
    ("record", "error", "message"),
    [
        ("not-a-tuple", TypeError, "record must be"),
        ((None, {}, {"value": b"encoded", "extra": b"bad"}), ValueError, "exactly one"),
        ((None, {}, {"value": "not-bytes"}), TypeError, "must contain bytes"),
    ],
)
async def test_aerospike_rejects_malformed_records(record: object, error: type[Exception], message: str) -> None:
    client = FakeAerospikeClient()
    client.get_result = record
    store = AerospikeStore(client=client)  # type: ignore[arg-type]

    with pytest.raises(error, match=message):
        await store._get_managed_entry(key="key", collection="items")


async def test_aerospike_only_maps_record_not_found_to_missing() -> None:
    client = FakeAerospikeClient()
    store = AerospikeStore(client=client)  # type: ignore[arg-type]

    assert await store._get_managed_entry(key="missing", collection="items") is None

    client.get_error = RuntimeError("server failed")
    with pytest.raises(RuntimeError, match="server failed"):
        await store._get_managed_entry(key="key", collection="items")


@pytest.mark.parametrize(
    ("response", "error", "message"),
    [
        ([], TypeError, "response must be a dict"),
        ({"node": "bad"}, TypeError, "result must be"),
        ({"node": ("error", "test")}, RuntimeError, "namespace query failed"),
        ({"node": (None, 1)}, TypeError, "invalid field types"),
        ({"node": (0, "test")}, RuntimeError, "namespace query failed"),
        ({"node": (None, "other")}, ValueError, "does not exist"),
    ],
)
async def test_aerospike_namespace_validation_is_strict(response: object, error: type[Exception], message: str) -> None:
    client = FakeAerospikeClient()
    client.info_response = response
    store = AerospikeStore(client=client, namespace="test", auto_create=False)  # type: ignore[arg-type]

    with pytest.raises(error, match=message):
        await store._setup()


async def test_aerospike_namespace_validation_accepts_real_client_response() -> None:
    client = FakeAerospikeClient()
    client.info_response = {"node": (None, "test\n")}
    store = AerospikeStore(client=client, namespace="test", auto_create=False)  # type: ignore[arg-type]

    await store._setup()
