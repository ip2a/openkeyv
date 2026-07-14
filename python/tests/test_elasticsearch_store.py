from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from elastic_transport import SerializationError as ElasticsearchSerializationError

from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv.errors import DeserializationError, StoreConnectionError
from openkeyv.stores.elasticsearch.serializers import LessCapableJsonSerializer
from openkeyv.stores.elasticsearch.store import ElasticsearchSerializationAdapter, ElasticsearchStore


class FakeResponse:
    def __init__(self, body: object) -> None:
        self.body = body


class FakeElasticsearchClient:
    def __init__(self) -> None:
        serializers = SimpleNamespace(serializers={}, default_serializer=None)
        self.transport = SimpleNamespace(serializers=serializers)
        self.get_body: object = {}
        self.index_body: object = {}
        self.delete_body: object = {}
        self.bulk_body: object = {}
        self.index_calls: list[dict[str, Any]] = []
        self.bulk_calls: list[dict[str, Any]] = []
        self.options_calls: list[dict[str, Any]] = []

    def options(self, **kwargs: Any) -> "FakeElasticsearchClient":
        self.options_calls.append(kwargs)
        return self

    async def get(self, **_kwargs: Any) -> FakeResponse:
        return FakeResponse(self.get_body)

    async def index(self, **kwargs: Any) -> FakeResponse:
        self.index_calls.append(kwargs)
        return FakeResponse(self.index_body)

    async def delete(self, **_kwargs: Any) -> FakeResponse:
        return FakeResponse(self.delete_body)

    async def bulk(self, **kwargs: Any) -> FakeResponse:
        self.bulk_calls.append(kwargs)
        return FakeResponse(self.bulk_body)


@pytest.fixture
def elasticsearch_store() -> tuple[ElasticsearchStore, FakeElasticsearchClient]:
    client = FakeElasticsearchClient()
    with pytest.warns(UserWarning, match="unstable"):
        store = ElasticsearchStore(elasticsearch_client=client, index_prefix="OpenKeyV")  # type: ignore[arg-type]
    return store, client


def managed_entry() -> ManagedEntry:
    return ManagedEntry(value={"nested": {"value": 1}}, created_at=datetime(2026, 1, 2, tzinfo=timezone.utc))


def source_document(*, key: str = "key", collection: str = "items") -> dict[str, Any]:
    return ElasticsearchSerializationAdapter().dump_dict(entry=managed_entry(), key=key, collection=collection)


def test_elasticsearch_serialization_adapter_roundtrip() -> None:
    adapter = ElasticsearchSerializationAdapter()
    document = adapter.dump_dict(entry=managed_entry(), key="key", collection="items")

    assert document == {
        "version": 1,
        "key": "key",
        "collection": "items",
        "created_at": "2026-01-02T00:00:00+00:00",
        "value": {"flattened": {"nested": {"value": 1}}},
    }
    assert adapter.load_dict(data=document) == managed_entry()


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"version": 2}, "version must be 1"),
        ({"version": 1, "created_at": 1}, "created_at must be"),
        ({"version": 1, "created_at": "2026-01-02T00:00:00+00:00", "value": {"flattened": []}}, "flattened must be"),
    ],
)
def test_elasticsearch_serialization_adapter_rejects_invalid_documents(document: dict[str, Any], message: str) -> None:
    with pytest.raises(DeserializationError, match=message):
        ElasticsearchSerializationAdapter().load_dict(data=document)


def test_elasticsearch_json_serializer_rejects_unknown_objects() -> None:
    with pytest.raises(ElasticsearchSerializationError, match="Unable to serialize"):
        LessCapableJsonSerializer().default(object())


async def test_elasticsearch_get_handles_found_and_missing(elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient]) -> None:
    store, client = elasticsearch_store
    client.get_body = {"_id": "missing", "found": False}
    assert await store._get_managed_entry(key="missing", collection="items") is None

    client.get_body = {"_id": "key", "found": True, "_source": source_document()}
    assert await store._get_managed_entry(key="key", collection="items") == managed_entry()
    assert client.options_calls == [{"ignore_status": 404}, {"ignore_status": 404}]


@pytest.mark.parametrize(
    ("body", "error", "message"),
    [
        ([], StoreConnectionError, "body must be an object"),
        ({"_id": "other", "found": False}, StoreConnectionError, "ID does not match"),
        ({"_id": "key", "found": "yes"}, StoreConnectionError, "must be a boolean"),
        (
            {"_id": "key", "found": True, "_source": source_document(key="other")},
            DeserializationError,
            "identity does not match",
        ),
    ],
)
async def test_elasticsearch_get_rejects_malformed_responses(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient], body: object, error: type[Exception], message: str
) -> None:
    store, client = elasticsearch_store
    client.get_body = body

    with pytest.raises(error, match=message):
        await store._get_managed_entry(key="key", collection="items")


async def test_elasticsearch_put_requires_acknowledged_write(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    client.index_body = {"_id": "key", "result": "created", "_shards": {"failed": 0}}

    await store._put_managed_entry(key="key", collection="items", managed_entry=managed_entry())

    assert client.index_calls == [
        {
            "index": "openkeyv-items",
            "id": "key",
            "body": source_document(),
            "refresh": True,
        }
    ]

    client.index_body = {"_id": "key", "result": "noop", "_shards": {"failed": 0}}
    with pytest.raises(StoreConnectionError, match="did not confirm"):
        await store._put_managed_entry(key="key", collection="items", managed_entry=managed_entry())


@pytest.mark.parametrize(("result", "expected"), [("deleted", True), ("not_found", False)])
async def test_elasticsearch_delete_accepts_only_typed_results(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient], result: str, expected: bool
) -> None:
    store, client = elasticsearch_store
    client.delete_body = {"_id": "key", "result": result}

    assert await store._delete_managed_entry(key="key", collection="items") is expected


async def test_elasticsearch_delete_rejects_invalid_result(elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient]) -> None:
    store, client = elasticsearch_store
    client.delete_body = {"_id": "key", "result": "noop"}

    with pytest.raises(StoreConnectionError, match="deleted or not_found"):
        await store._delete_managed_entry(key="key", collection="items")


async def test_elasticsearch_bulk_put_validates_each_item(elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient]) -> None:
    store, client = elasticsearch_store
    entries = [managed_entry(), managed_entry()]
    client.bulk_body = {
        "errors": False,
        "items": [
            {"index": {"_id": "one", "result": "created", "status": 201}},
            {"index": {"_id": "two", "result": "updated", "status": 200}},
        ],
    }

    await store._put_managed_entries(
        collection="items",
        keys=["one", "two"],
        managed_entries=entries,
        ttl=None,
        created_at=entries[0].created_at,
        expires_at=None,
    )

    assert client.bulk_calls[-1]["refresh"] is True
    assert client.bulk_calls[-1]["operations"] == [
        {"index": {"_index": "openkeyv-items", "_id": "one"}},
        source_document(key="one"),
        {"index": {"_index": "openkeyv-items", "_id": "two"}},
        source_document(key="two"),
    ]


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"errors": True, "items": []}, "reported one or more failed writes"),
        ({"errors": False, "items": []}, "one item per requested key"),
        ({"errors": False, "items": [{"index": {"_id": "other", "result": "created", "status": 201}}]}, "did not confirm"),
        ({"errors": False, "items": [{"index": {"_id": "key", "result": "created", "status": 500}}]}, "invalid status"),
    ],
)
async def test_elasticsearch_bulk_put_rejects_malformed_results(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient], body: object, message: str
) -> None:
    store, client = elasticsearch_store
    entry = managed_entry()
    client.bulk_body = body

    with pytest.raises(StoreConnectionError, match=message):
        await store._put_managed_entries(
            collection="items",
            keys=["key"],
            managed_entries=[entry],
            ttl=None,
            created_at=entry.created_at,
            expires_at=None,
        )


async def test_elasticsearch_bulk_delete_counts_deleted_and_missing(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    client.bulk_body = {
        "errors": False,
        "items": [
            {"delete": {"_id": "one", "result": "deleted", "status": 200}},
            {"delete": {"_id": "two", "result": "not_found", "status": 404}},
        ],
    }

    assert await store._delete_managed_entries(keys=["one", "two"], collection="items") == 1

    client.bulk_body = {
        "errors": False,
        "items": [{"delete": {"_id": "one", "result": "not_found", "status": 200}}],
    }
    with pytest.raises(StoreConnectionError, match="failed for document"):
        await store._delete_managed_entries(keys=["one"], collection="items")
