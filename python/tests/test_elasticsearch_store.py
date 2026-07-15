import base64
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from elastic_transport import SerializationError as ElasticsearchSerializationError

from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv.errors import DeserializationError, InvalidKeyError, StoreConnectionError
from openkeyv.stores.elasticsearch.serializers import LessCapableJsonSerializer
from openkeyv.stores.elasticsearch.store import ElasticsearchSerializationAdapter, ElasticsearchStore


class FakeResponse:
    def __init__(self, body: object, status: int = 200) -> None:
        self.body = body
        self.meta = SimpleNamespace(status=status)


class FakeIndices:
    def __init__(self) -> None:
        self.get_body: object = {}
        self.get_status = 200
        self.get_calls: list[dict[str, Any]] = []

    async def get(self, **kwargs: Any) -> FakeResponse:
        self.get_calls.append(kwargs)
        return FakeResponse(self.get_body, status=self.get_status)


class FakeElasticsearchClient:
    def __init__(self) -> None:
        serializers = SimpleNamespace(serializers={}, default_serializer=None)
        self.transport = SimpleNamespace(serializers=serializers)
        self.indices = FakeIndices()
        self.get_body: object = {}
        self.index_body: object = {}
        self.delete_body: object = {}
        self.delete_by_query_body: object = {}
        self.bulk_body: object = {}
        self.search_body: object = {}
        self.index_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []
        self.delete_by_query_calls: list[dict[str, Any]] = []
        self.bulk_calls: list[dict[str, Any]] = []
        self.search_calls: list[dict[str, Any]] = []
        self.options_calls: list[dict[str, Any]] = []

    def options(self, **kwargs: Any) -> "FakeElasticsearchClient":
        self.options_calls.append(kwargs)
        return self

    async def get(self, **_kwargs: Any) -> FakeResponse:
        return FakeResponse(self.get_body)

    async def index(self, **kwargs: Any) -> FakeResponse:
        self.index_calls.append(kwargs)
        return FakeResponse(self.index_body)

    async def delete(self, **kwargs: Any) -> FakeResponse:
        self.delete_calls.append(kwargs)
        return FakeResponse(self.delete_body)

    async def delete_by_query(self, **kwargs: Any) -> FakeResponse:
        self.delete_by_query_calls.append(kwargs)
        return FakeResponse(self.delete_by_query_body)

    async def bulk(self, **kwargs: Any) -> FakeResponse:
        self.bulk_calls.append(kwargs)
        return FakeResponse(self.bulk_body)

    async def search(self, **kwargs: Any) -> FakeResponse:
        self.search_calls.append(kwargs)
        return FakeResponse(self.search_body)


@pytest.fixture
def elasticsearch_store() -> tuple[ElasticsearchStore, FakeElasticsearchClient]:
    client = FakeElasticsearchClient()
    with pytest.warns(UserWarning, match="unstable"):
        store = ElasticsearchStore(elasticsearch_client=client, index_prefix="OpenKeyV")  # type: ignore[arg-type]
    return store, client


def managed_entry() -> ManagedEntry:
    return ManagedEntry(value={"nested": {"value": 1}}, created_at=datetime(2026, 1, 2, tzinfo=timezone.utc))


def index_name(collection: str) -> str:
    return f"openkeyv-okv1-{collection.encode('utf-8').hex()}"


def document_id(key: str) -> str:
    encoded = base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii").rstrip("=")
    return f"okv1-{encoded}"


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
    client.get_body = {"_id": document_id("missing"), "found": False}
    assert await store._get_managed_entry(key="missing", collection="items") is None

    client.get_body = {"_id": document_id("key"), "found": True, "_source": source_document()}
    assert await store._get_managed_entry(key="key", collection="items") == managed_entry()
    assert client.options_calls == [{"ignore_status": 404}, {"ignore_status": 404}]


@pytest.mark.parametrize(
    ("body", "error", "message"),
    [
        ([], StoreConnectionError, "body must be an object"),
        ({"_id": document_id("other"), "found": False}, StoreConnectionError, "ID does not match"),
        ({"_id": document_id("key"), "found": "yes"}, StoreConnectionError, "must be a boolean"),
        (
            {"_id": document_id("key"), "found": True, "_source": source_document(key="other")},
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
    client.index_body = {"_id": document_id("key"), "result": "created", "_shards": {"failed": 0}}

    await store._put_managed_entry(key="key", collection="items", managed_entry=managed_entry())

    assert client.index_calls == [
        {
            "index": index_name("items"),
            "id": document_id("key"),
            "body": source_document(),
            "refresh": True,
        }
    ]

    client.index_body = {"_id": document_id("key"), "result": "noop", "_shards": {"failed": 0}}
    with pytest.raises(StoreConnectionError, match="did not confirm"):
        await store._put_managed_entry(key="key", collection="items", managed_entry=managed_entry())


@pytest.mark.parametrize(("result", "expected"), [("deleted", True), ("not_found", False)])
async def test_elasticsearch_delete_accepts_only_typed_results(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient], result: str, expected: bool
) -> None:
    store, client = elasticsearch_store
    client.delete_body = {"_id": document_id("key"), "result": result}

    assert await store._delete_managed_entry(key="key", collection="items") is expected
    assert client.delete_calls[-1] == {"index": index_name("items"), "id": document_id("key"), "refresh": True}


async def test_elasticsearch_delete_rejects_invalid_result(elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient]) -> None:
    store, client = elasticsearch_store
    client.delete_body = {"_id": document_id("key"), "result": "noop"}

    with pytest.raises(StoreConnectionError, match="deleted or not_found"):
        await store._delete_managed_entry(key="key", collection="items")


async def test_elasticsearch_bulk_put_validates_each_item(elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient]) -> None:
    store, client = elasticsearch_store
    entries = [managed_entry(), managed_entry()]
    client.bulk_body = {
        "errors": False,
        "items": [
            {"index": {"_id": document_id("one"), "result": "created", "status": 201}},
            {"index": {"_id": document_id("two"), "result": "updated", "status": 200}},
        ],
    }

    await store._put_managed_entries(
        collection="items",
        keys=["one", "two"],
        managed_entries=entries,
        ttl=None,
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        expires_at=None,
    )

    assert client.bulk_calls[-1]["refresh"] is True
    assert client.bulk_calls[-1]["operations"] == [
        {"index": {"_index": index_name("items"), "_id": document_id("one")}},
        source_document(key="one"),
        {"index": {"_index": index_name("items"), "_id": document_id("two")}},
        source_document(key="two"),
    ]


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"errors": True, "items": []}, "reported one or more failed writes"),
        ({"errors": False, "items": []}, "one item per requested key"),
        ({"errors": False, "items": [{"index": {"_id": document_id("other"), "result": "created", "status": 201}}]}, "did not confirm"),
        ({"errors": False, "items": [{"index": {"_id": document_id("key"), "result": "created", "status": 500}}]}, "invalid status"),
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
            created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            expires_at=None,
        )


async def test_elasticsearch_bulk_delete_counts_deleted_and_missing(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    client.bulk_body = {
        "errors": False,
        "items": [
            {"delete": {"_id": document_id("one"), "result": "deleted", "status": 200}},
            {"delete": {"_id": document_id("two"), "result": "not_found", "status": 404}},
        ],
    }

    assert await store._delete_managed_entries(keys=["one", "two"], collection="items") == 1
    assert client.bulk_calls[-1]["refresh"] is True

    client.bulk_body = {
        "errors": False,
        "items": [{"delete": {"_id": document_id("one"), "result": "not_found", "status": 200}}],
    }
    with pytest.raises(StoreConnectionError, match="failed for document"):
        await store._delete_managed_entries(keys=["one"], collection="items")


async def test_elasticsearch_key_search_requests_keyword_field(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    client.search_body = {
        "hits": {
            "hits": [
                {"_index": index_name("items"), "_id": document_id("one"), "fields": {"key": ["one"]}},
                {"_index": index_name("items"), "_id": document_id("two"), "fields": {"key": ["two"]}},
            ]
        }
    }

    assert await store._get_collection_keys(collection="items") == ["one", "two"]
    assert client.search_calls == [
        {
            "index": index_name("items"),
            "fields": ["key"],
            "body": {"query": {"term": {"collection": "items"}}},
            "source_includes": [],
            "size": 10000,
        }
    ]


async def test_elasticsearch_delete_by_query_operations_refresh(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    client.delete_by_query_body = {"timed_out": False, "failures": [], "deleted": 1}

    assert await store._delete_collection(collection="items") is True
    assert client.delete_by_query_calls[-1]["refresh"] is True

    client.indices.get_body = {index_name("items"): {}}
    await store._cull()
    assert client.delete_by_query_calls[-1]["index"] == [index_name("items")]
    assert client.delete_by_query_calls[-1]["refresh"] is True


@pytest.mark.parametrize("collection", ["", "Users", "users", "e\u0301", "\u00e9", "a/b", "a\x00b"])
def test_elasticsearch_collection_identity_is_reversible_and_case_preserving(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient], collection: str
) -> None:
    store, _client = elasticsearch_store
    physical = store._get_index_name(collection)
    assert physical == index_name(collection)
    assert store._decode_index_name(physical) == collection


def test_elasticsearch_key_identity_is_reversible_without_hashing(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, _client = elasticsearch_store
    assert store._get_document_id("a\x00/b") == document_id("a\x00/b")
    assert store._get_document_id("e\u0301") != store._get_document_id("\u00e9")


def test_elasticsearch_identity_boundaries_are_rejected_before_requests(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    accepted_index = store._get_index_name("x" * 120)
    assert len(accepted_index.encode("ascii")) == 254
    with pytest.raises(InvalidKeyError, match="collection index"):
        store._get_index_name("x" * 121)
    accepted_document_id = store._get_document_id("x" * 380)
    assert len(accepted_document_id.encode("ascii")) == 512
    with pytest.raises(InvalidKeyError, match="document ID"):
        store._get_document_id("x" * 381)
    assert client.index_calls == []
    assert client.bulk_calls == []


async def test_elasticsearch_batch_identity_preflight_has_no_partial_request(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    entries = [managed_entry(), managed_entry()]
    with pytest.raises(InvalidKeyError, match="document ID"):
        await store._put_managed_entries(
            collection="items",
            keys=["valid", "x" * 381],
            managed_entries=entries,
            ttl=None,
            created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            expires_at=None,
        )
    assert client.bulk_calls == []


async def test_elasticsearch_owned_index_enumeration_is_strict(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    client.indices.get_body = {index_name("Users"): {}, index_name(""): {}}
    assert await store._get_collection_names() == ["Users", ""]
    assert client.indices.get_calls == [{"index": "openkeyv-okv1-*"}]

    client.indices.get_body = {"openkeyv-okv1-not-hex": {}}
    with pytest.raises(StoreConnectionError, match="malformed canonical"):
        await store._get_collection_names()

    client.indices.get_status = 404
    assert await store._get_collection_names() == []

    client.indices.get_status = 500
    with pytest.raises(StoreConnectionError, match="unexpected status"):
        await store._get_collection_names()


async def test_elasticsearch_cull_validates_owned_indices_before_mutation(
    elasticsearch_store: tuple[ElasticsearchStore, FakeElasticsearchClient],
) -> None:
    store, client = elasticsearch_store
    client.indices.get_body = {"openkeyv-okv1-zz": {}}
    with pytest.raises(StoreConnectionError, match="malformed canonical"):
        await store._cull()
    assert client.delete_by_query_calls == []
