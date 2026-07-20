import base64
from asyncio import Lock
from collections.abc import Sequence
from http import HTTPStatus
from typing import Any, SupportsFloat, cast, overload

from openkeyv._utils.beartype import bear_enforce
from openkeyv._utils.constants import DEFAULT_COLLECTION_NAME
from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv._utils.time_to_live import now, now_as_epoch, prepare_entry_timestamps, prepare_ttl
from openkeyv.errors import DeserializationError, InvalidKeyError, SerializationError, StoreConnectionError
from openkeyv.protocols.key_value import StoreValue
from openkeyv.stores.elasticsearch.codec import ElasticsearchDocumentCodec
from openkeyv.stores.elasticsearch.serializers import LessCapableJsonSerializer, LessCapableNdjsonSerializer

try:
    from elastic_transport import ObjectApiResponse
    from elastic_transport import SerializationError as ElasticsearchSerializationError
    from elasticsearch import AsyncElasticsearch
except ImportError as e:
    msg = "ElasticsearchStore requires openkeyv[elasticsearch]"
    raise ImportError(msg) from e

DEFAULT_INDEX_PREFIX = "kv_store"

DEFAULT_MAPPING = {
    "properties": {
        "created_at": {"type": "date"},
        "expires_at": {"type": "date"},
        "collection": {"type": "keyword"},
        "key": {"type": "keyword"},
        "entry": {"type": "binary", "doc_values": False},
    },
}

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000

INDEX_ENCODING_PREFIX = "okv1-"
MAX_INDEX_BYTES = 255
MAX_DOCUMENT_ID_BYTES = 512
ALLOWED_INDEX_PREFIX_CHARACTERS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_-.")


class ElasticsearchStore:
    """An Elasticsearch-based store.

    Stores collections in their own indices and stores values in Flattened fields.

    Logical keys and collections are transported through reversible, canonical Elasticsearch-safe identities.
    Collection names are encoded into owned index names and keys into document IDs; no logical identity is lowercased,
    hashed, truncated, or silently replaced.
    """

    _client: AsyncElasticsearch

    _is_serverless: bool

    _index_prefix: str

    _default_collection: str | None

    _serializer: ElasticsearchDocumentCodec

    _auto_create: bool

    @overload
    def __init__(
        self,
        *,
        elasticsearch_client: AsyncElasticsearch,
        index_prefix: str,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the elasticsearch store.

        Args:
            elasticsearch_client: The elasticsearch client to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create indices if they don't exist. Defaults to True.
        """

    @overload
    def __init__(
        self,
        *,
        url: str,
        api_key: str | None = None,
        index_prefix: str,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the elasticsearch store.

        Args:
            url: The url of the elasticsearch cluster.
            api_key: The api key to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create indices if they don't exist. Defaults to True.
        """

    def __init__(
        self,
        *,
        elasticsearch_client: AsyncElasticsearch | None = None,
        url: str | None = None,
        api_key: str | None = None,
        index_prefix: str,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the elasticsearch store.

        Args:
            elasticsearch_client: The elasticsearch client to use. If provided, the store will not
                manage the client's lifecycle (will not close it). The caller is responsible for
                managing the client's lifecycle.
            url: The url of the elasticsearch cluster.
            api_key: The api key to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create indices if they don't exist. Defaults to True.
                When False, raises ValueError if an index doesn't exist.
        """
        if elasticsearch_client is None and url is None:
            msg = "Either elasticsearch_client or url must be provided"
            raise ValueError(msg)

        if not index_prefix:
            msg = "index_prefix must not be empty"
            raise ValueError(msg)
        normalized_index_prefix = index_prefix.lower()
        if normalized_index_prefix[0] in "-_+" or not all(
            character in ALLOWED_INDEX_PREFIX_CHARACTERS for character in normalized_index_prefix
        ):
            msg = "index_prefix must contain only lowercase Elasticsearch index characters"
            raise ValueError(msg)
        if len((normalized_index_prefix + "-" + INDEX_ENCODING_PREFIX).encode("ascii")) > MAX_INDEX_BYTES:
            msg = f"index_prefix leaves no room for an Elasticsearch collection index within {MAX_INDEX_BYTES} bytes"
            raise ValueError(msg)

        client_provided = elasticsearch_client is not None

        if elasticsearch_client:
            self._client = elasticsearch_client
        elif url:
            self._client = AsyncElasticsearch(hosts=[url], api_key=api_key, http_compress=True, request_timeout=10, max_retries=0)
        else:
            msg = "Either elasticsearch_client or url must be provided"
            raise ValueError(msg)

        LessCapableJsonSerializer.install_serializer(client=self._client)
        LessCapableJsonSerializer.install_default_serializer(client=self._client)
        LessCapableNdjsonSerializer.install_serializer(client=self._client)

        self._index_prefix = normalized_index_prefix
        self._is_serverless = False

        self._serializer = ElasticsearchDocumentCodec()
        self._auto_create = auto_create

        self.default_collection = DEFAULT_COLLECTION_NAME if default_collection is None else default_collection
        self._client_provided_by_user = client_provided
        self._setup_complete = False
        self._setup_lock = Lock()

    def _collection(self, collection: str | None) -> str:
        return self.default_collection if collection is None else collection

    async def setup(self) -> None:
        if self._setup_complete:
            return
        async with self._setup_lock:
            if self._setup_complete:
                return
            await self._setup()
            self._setup_complete = True

    async def setup_collection(self, *, collection: str) -> None:
        self._get_index_name(collection=collection)
        await self.setup()
        await self._setup_collection(collection=collection)

    async def __aenter__(self) -> "ElasticsearchStore":
        await self.setup()
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        await self.close()

    async def close(self) -> None:
        if not self._client_provided_by_user:
            await self._client.close()

    @bear_enforce
    async def get(self, key: str, *, collection: str | None = None) -> StoreValue:
        self._get_document_id(key=key)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        entry = await self._get_managed_entry(key=key, collection=resolved)
        return None if entry is None or entry.is_expired else entry.value

    @bear_enforce
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[StoreValue]:
        for key in keys:
            self._get_document_id(key=key)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        current = now()
        entries = await self._get_managed_entries(keys=keys, collection=resolved)
        return [
            entry.value if entry is not None and (entry.expires_at is None or entry.expires_at > current) else None for entry in entries
        ]

    @bear_enforce
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[StoreValue, float | None]:
        self._get_document_id(key=key)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        entry = await self._get_managed_entry(key=key, collection=resolved)
        return (None, None) if entry is None or entry.is_expired else (entry.value, entry.ttl)

    @bear_enforce
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[StoreValue, float | None]]:
        for key in keys:
            self._get_document_id(key=key)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        current = now()
        entries = await self._get_managed_entries(keys=keys, collection=resolved)
        return [
            (entry.value, entry.ttl) if entry is not None and (entry.expires_at is None or entry.expires_at > current) else (None, None)
            for entry in entries
        ]

    @bear_enforce
    async def put(self, key: str, value: StoreValue, *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        self._get_document_id(key=key)
        created_at, _, expires_at = prepare_entry_timestamps(ttl=ttl)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        await self._put_managed_entry(
            key=key, collection=resolved, managed_entry=ManagedEntry(value=value, created_at=created_at, expires_at=expires_at)
        )

    @bear_enforce
    async def put_many(
        self, keys: Sequence[str], values: Sequence[StoreValue], *, collection: str | None = None, ttl: SupportsFloat | None = None
    ) -> None:
        if len(keys) != len(values):
            msg = "put_many called but a different number of keys and values were provided"
            raise ValueError(msg)
        for key in keys:
            self._get_document_id(key=key)
        validated_ttl = prepare_ttl(t=ttl)
        created_at, _, expires_at = prepare_entry_timestamps(ttl=validated_ttl)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        entries = [ManagedEntry(value=value, created_at=created_at, expires_at=expires_at) for value in values]
        await self._put_managed_entries(keys=keys, managed_entries=entries, collection=resolved)

    @bear_enforce
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        self._get_document_id(key=key)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        return await self._delete_managed_entry(key=key, collection=resolved)

    @bear_enforce
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        for key in keys:
            self._get_document_id(key=key)
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        return await self._delete_managed_entries(keys=keys, collection=resolved)

    async def keys(self, collection: str | None = None, *, limit: int | None = None) -> list[str]:
        resolved = self._collection(collection)
        await self.setup_collection(collection=resolved)
        return await self._get_collection_keys(collection=resolved, limit=limit)

    async def collections(self, *, limit: int | None = None) -> list[str]:
        await self.setup()
        return await self._get_collection_names(limit=limit)

    async def destroy_collection(self, collection: str) -> bool:
        self._get_index_name(collection=collection)
        await self.setup()
        return await self._delete_collection(collection=collection)

    async def destroy(self) -> bool:
        await self.setup()
        for collection in await self._get_collection_names():
            await self._delete_collection(collection=collection)
        return True

    async def cull(self) -> None:
        await self.setup()
        await self._cull()

    async def _setup(self) -> None:
        cluster_info = await self._client.options(ignore_status=404).info()
        body = cluster_info.body
        if not isinstance(body, dict):
            msg = "Elasticsearch info response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(key, str) for key in body):
            msg = "Elasticsearch info response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)

        version = body.get("version")
        if not isinstance(version, dict):
            msg = "Elasticsearch info response version must be an object"
            raise StoreConnectionError(msg)
        version = cast("dict[str, Any]", version)

        build_flavor = version.get("build_flavor")
        if not isinstance(build_flavor, str):
            msg = "Elasticsearch info response version.build_flavor must be a string"
            raise StoreConnectionError(msg)

        self._is_serverless = build_flavor == "serverless"

    async def _setup_collection(self, *, collection: str) -> None:
        index_name = self._get_index_name(collection=collection)

        if await self._client.options(ignore_status=404).indices.exists(index=index_name):
            return

        if not self._auto_create:
            msg = f"Index '{index_name}' does not exist. Either create the index manually or set auto_create=True."
            raise ValueError(msg)

        response = await self._client.indices.create(index=index_name, mappings=DEFAULT_MAPPING, settings={})
        body = response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch create-index response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(key, str) for key in body):
            msg = "Elasticsearch create-index response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)
        if body.get("acknowledged") is not True or body.get("index") != index_name:
            msg = f"Elasticsearch did not acknowledge creation of index '{index_name}'"
            raise StoreConnectionError(msg)

    def _get_index_namespace_prefix(self) -> str:
        return f"{self._index_prefix}-{INDEX_ENCODING_PREFIX}"

    def _get_index_name(self, collection: str) -> str:
        encoded_collection = collection.encode("utf-8").hex()
        index_name = self._get_index_namespace_prefix() + encoded_collection
        if len(index_name.encode("ascii")) > MAX_INDEX_BYTES:
            msg = f"Elasticsearch collection index exceeds the maximum size of {MAX_INDEX_BYTES} bytes"
            raise InvalidKeyError(msg)
        return index_name

    def _get_document_id(self, key: str) -> str:
        encoded_key = base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii").rstrip("=")
        document_id = INDEX_ENCODING_PREFIX + encoded_key
        if len(document_id.encode("ascii")) > MAX_DOCUMENT_ID_BYTES:
            msg = f"Elasticsearch document ID exceeds the maximum size of {MAX_DOCUMENT_ID_BYTES} bytes"
            raise InvalidKeyError(msg)
        return document_id

    def _decode_index_name(self, index_name: str) -> str:
        namespace_prefix = self._get_index_namespace_prefix()
        if not index_name.startswith(namespace_prefix):
            msg = "Elasticsearch index is outside the OpenKeyV namespace"
            raise StoreConnectionError(msg)

        encoded_collection = index_name[len(namespace_prefix) :]
        if len(encoded_collection) % 2 or not all(character in "0123456789abcdef" for character in encoded_collection):
            msg = "Elasticsearch owned index has a malformed canonical collection identity"
            raise StoreConnectionError(msg)

        try:
            collection = bytes.fromhex(encoded_collection).decode("utf-8")
        except UnicodeDecodeError as error:
            msg = "Elasticsearch owned index has a non-UTF-8 canonical collection identity"
            raise StoreConnectionError(msg) from error

        try:
            canonical_index_name = self._get_index_name(collection=collection)
        except InvalidKeyError as error:
            msg = "Elasticsearch owned index exceeds the canonical collection boundary"
            raise StoreConnectionError(msg) from error
        if canonical_index_name != index_name:
            msg = "Elasticsearch owned index is not a canonical collection identity"
            raise StoreConnectionError(msg)

        return collection

    async def _get_owned_index_names(self) -> list[str]:
        response = await self._client.options(ignore_status=404).indices.get(index=f"{self._get_index_namespace_prefix()}*")
        status = getattr(getattr(response, "meta", None), "status", HTTPStatus.OK)
        if status == HTTPStatus.NOT_FOUND:
            return []
        if status != HTTPStatus.OK:
            msg = "Elasticsearch owned-index lookup returned an unexpected status"
            raise StoreConnectionError(msg)

        body = response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch owned-index response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(index_name, str) for index_name in body):
            msg = "Elasticsearch owned-index response keys must be strings"
            raise StoreConnectionError(msg)

        index_names = list(cast("dict[str, Any]", body))
        for index_name in index_names:
            self._decode_index_name(index_name=index_name)
        return index_names

    def _get_destination(self, *, collection: str, key: str) -> tuple[str, str]:
        index_name: str = self._get_index_name(collection=collection)
        document_id: str = self._get_document_id(key=key)

        return index_name, document_id

    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        index_name, document_id = self._get_destination(collection=collection, key=key)

        elasticsearch_response = await self._client.options(ignore_status=404).get(index=index_name, id=document_id)
        body = elasticsearch_response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch get response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch get response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)

        if body.get("_id") != document_id:
            msg = "Elasticsearch get response document ID does not match the requested key"
            raise StoreConnectionError(msg)

        found = body.get("found")
        if found is False:
            return None
        if found is not True:
            msg = "Elasticsearch get response found field must be a boolean"
            raise StoreConnectionError(msg)

        source = body.get("_source")
        if not isinstance(source, dict):
            msg = "Elasticsearch document _source must be an object with string keys"
            raise DeserializationError(msg)
        source = cast("dict[Any, Any]", source)
        if not all(isinstance(field, str) for field in source):
            msg = "Elasticsearch document _source must be an object with string keys"
            raise DeserializationError(msg)
        source = cast("dict[str, Any]", source)
        if source.get("key") != key or source.get("collection") != collection:
            msg = "Elasticsearch document identity does not match the requested key and collection"
            raise DeserializationError(msg)

        return self._serializer.load_dict(data=source)

    async def _get_managed_entries(  # noqa: PLR0912, PLR0915
        self, *, collection: str, keys: Sequence[str]
    ) -> list[ManagedEntry | None]:
        if not keys:
            return []

        # Use mget for efficient batch retrieval
        index_name = self._get_index_name(collection=collection)
        document_ids = [self._get_document_id(key=key) for key in keys]
        docs = [{"_id": document_id} for document_id in document_ids]

        elasticsearch_response = await self._client.options(ignore_status=404).mget(index=index_name, docs=docs)
        body = elasticsearch_response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch mget response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch mget response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)

        docs_result = body.get("docs")
        if not isinstance(docs_result, list):
            msg = "Elasticsearch mget response must contain one document result per requested key"
            raise StoreConnectionError(msg)
        docs_result = cast("list[Any]", docs_result)
        if len(docs_result) != len(keys):
            msg = "Elasticsearch mget response must contain one document result per requested key"
            raise StoreConnectionError(msg)

        entries: list[ManagedEntry | None] = []
        for key, document_id, doc in zip(keys, document_ids, docs_result, strict=True):
            if not isinstance(doc, dict):
                msg = "Elasticsearch mget document result must be an object with string keys"
                raise StoreConnectionError(msg)
            doc = cast("dict[Any, Any]", doc)
            if not all(isinstance(field, str) for field in doc):
                msg = "Elasticsearch mget document result must be an object with string keys"
                raise StoreConnectionError(msg)
            doc = cast("dict[str, Any]", doc)
            if doc.get("_id") != document_id:
                msg = "Elasticsearch mget response document ID does not match the requested key"
                raise StoreConnectionError(msg)

            found = doc.get("found")
            if found is False:
                entries.append(None)
                continue
            if found is not True:
                msg = "Elasticsearch mget document found field must be a boolean"
                raise StoreConnectionError(msg)

            source = doc.get("_source")
            if not isinstance(source, dict):
                msg = "Elasticsearch mget document _source must be an object with string keys"
                raise DeserializationError(msg)
            source = cast("dict[Any, Any]", source)
            if not all(isinstance(field, str) for field in source):
                msg = "Elasticsearch mget document _source must be an object with string keys"
                raise DeserializationError(msg)
            source = cast("dict[str, Any]", source)
            if source.get("key") != key or source.get("collection") != collection:
                msg = "Elasticsearch mget document identity does not match the requested key and collection"
                raise DeserializationError(msg)

            entries.append(self._serializer.load_dict(data=source))

        return entries

    @property
    def _should_refresh_on_put(self) -> bool:
        return not self._is_serverless

    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        index_name: str = self._get_index_name(collection=collection)
        document_id: str = self._get_document_id(key=key)

        document: dict[str, Any] = self._serializer.dump_dict(entry=managed_entry, key=key, collection=collection)

        try:
            response = await self._client.index(
                index=index_name,
                id=document_id,
                body=document,
                refresh=self._should_refresh_on_put,
            )
        except ElasticsearchSerializationError as e:
            msg = f"Failed to serialize document: {e}"
            raise SerializationError(message=msg) from e

        body = response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch index response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch index response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)
        if body.get("_id") != document_id or body.get("result") not in {"created", "updated"}:
            msg = "Elasticsearch index response did not confirm the requested write"
            raise StoreConnectionError(msg)

        shards = body.get("_shards")
        if not isinstance(shards, dict):
            msg = "Elasticsearch index response reported failed shards"
            raise StoreConnectionError(msg)
        shards = cast("dict[Any, Any]", shards)
        if type(shards.get("failed")) is not int or shards["failed"] != 0:
            msg = "Elasticsearch index response reported failed shards"
            raise StoreConnectionError(msg)

    async def _put_managed_entries(  # noqa: PLR0912, PLR0915
        self,
        *,
        collection: str,
        keys: Sequence[str],
        managed_entries: Sequence[ManagedEntry],
    ) -> None:
        if not keys:
            return

        operations: list[dict[str, Any]] = []

        index_name: str = self._get_index_name(collection=collection)

        for key, managed_entry in zip(keys, managed_entries, strict=True):
            document_id: str = self._get_document_id(key=key)

            index_action = {"index": {"_index": index_name, "_id": document_id}}

            document: dict[str, Any] = self._serializer.dump_dict(entry=managed_entry, key=key, collection=collection)

            operations.extend([index_action, document])

        try:
            response = await self._client.bulk(operations=operations, refresh=self._should_refresh_on_put)
        except ElasticsearchSerializationError as e:
            msg = f"Failed to serialize bulk operations: {e}"
            raise SerializationError(message=msg) from e

        body = response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch bulk-index response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch bulk-index response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)
        if body.get("errors") is not False:
            msg = "Elasticsearch bulk-index response reported one or more failed writes"
            raise StoreConnectionError(msg)

        items = body.get("items")
        if not isinstance(items, list):
            msg = "Elasticsearch bulk-index response must contain one item per requested key"
            raise StoreConnectionError(msg)
        items = cast("list[Any]", items)
        if len(items) != len(keys):
            msg = "Elasticsearch bulk-index response must contain one item per requested key"
            raise StoreConnectionError(msg)

        for key, item in zip(keys, items, strict=True):
            if not isinstance(item, dict):
                msg = "Elasticsearch bulk-index item must contain exactly one index result"
                raise StoreConnectionError(msg)
            item = cast("dict[Any, Any]", item)
            if set(item) != {"index"}:
                msg = "Elasticsearch bulk-index item must contain exactly one index result"
                raise StoreConnectionError(msg)
            item = cast("dict[str, Any]", item)

            result = item["index"]
            document_id = self._get_document_id(key=key)
            if not isinstance(result, dict):
                msg = "Elasticsearch bulk-index result must be an object"
                raise StoreConnectionError(msg)
            result = cast("dict[str, Any]", result)
            if result.get("_id") != document_id or result.get("result") not in {"created", "updated"}:
                msg = "Elasticsearch bulk-index item did not confirm the requested write"
                raise StoreConnectionError(msg)
            if type(result.get("status")) is not int or result["status"] not in {HTTPStatus.OK, HTTPStatus.CREATED}:
                msg = "Elasticsearch bulk-index item returned an invalid status"
                raise StoreConnectionError(msg)

    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        index_name: str = self._get_index_name(collection=collection)
        document_id: str = self._get_document_id(key=key)

        elasticsearch_response: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).delete(
            index=index_name, id=document_id, refresh=True
        )
        body = elasticsearch_response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch delete response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch delete response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)
        if body.get("_id") != document_id:
            msg = "Elasticsearch delete response document ID does not match the requested key"
            raise StoreConnectionError(msg)

        result = body.get("result")
        if result == "not_found":
            return False
        if result != "deleted":
            msg = "Elasticsearch delete response result must be deleted or not_found"
            raise StoreConnectionError(msg)

        return True

    async def _delete_managed_entries(  # noqa: PLR0912
        self, *, keys: Sequence[str], collection: str
    ) -> int:
        if not keys:
            return 0

        operations: list[dict[str, Any]] = []

        for key in keys:
            index_name, document_id = self._get_destination(collection=collection, key=key)
            operations.append({"delete": {"_index": index_name, "_id": document_id}})

        elasticsearch_response = await self._client.bulk(operations=operations, refresh=True)
        body = elasticsearch_response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch bulk-delete response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch bulk-delete response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)

        if body.get("errors") is not False:
            msg = "Elasticsearch bulk-delete response reported one or more failed deletes"
            raise StoreConnectionError(msg)

        items = body.get("items")
        if not isinstance(items, list):
            msg = "Elasticsearch bulk-delete response must contain one item per requested key"
            raise StoreConnectionError(msg)
        items = cast("list[Any]", items)
        if len(items) != len(keys):
            msg = "Elasticsearch bulk-delete response must contain one item per requested key"
            raise StoreConnectionError(msg)

        deleted_count = 0
        for key, item in zip(keys, items, strict=True):
            if not isinstance(item, dict):
                msg = "Elasticsearch bulk-delete item must contain exactly one delete result"
                raise StoreConnectionError(msg)
            item = cast("dict[Any, Any]", item)
            if set(item) != {"delete"}:
                msg = "Elasticsearch bulk-delete item must contain exactly one delete result"
                raise StoreConnectionError(msg)
            item = cast("dict[str, Any]", item)

            result = item["delete"]
            document_id = self._get_document_id(key=key)
            if not isinstance(result, dict):
                msg = "Elasticsearch bulk-delete result must be an object"
                raise StoreConnectionError(msg)
            result = cast("dict[str, Any]", result)
            if result.get("_id") != document_id:
                msg = "Elasticsearch bulk-delete item document ID does not match the requested key"
                raise StoreConnectionError(msg)

            status = result.get("status")
            operation_result = result.get("result")
            if operation_result == "deleted" and status == HTTPStatus.OK:
                deleted_count += 1
            elif operation_result != "not_found" or status != HTTPStatus.NOT_FOUND:
                msg = f"Elasticsearch bulk-delete failed for document '{document_id}'"
                raise StoreConnectionError(msg)

        return deleted_count

    async def _get_collection_keys(  # noqa: PLR0912, PLR0915
        self, *, collection: str, limit: int | None = None
    ) -> list[str]:
        """Get up to 10,000 keys in the specified collection (eventually consistent)."""

        limit = min(DEFAULT_PAGE_SIZE if limit is None else limit, PAGE_LIMIT)

        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).search(
            index=self._get_index_name(collection=collection),
            fields=cast("Any", ["key"]),
            body={
                "query": {
                    "term": {
                        "collection": collection,
                    },
                },
            },
            source_includes=[],
            size=PAGE_LIMIT,
        )
        body = result.body
        if not isinstance(body, dict):
            msg = "Elasticsearch key-search response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch key-search response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)

        hits = body.get("hits")
        if not isinstance(hits, dict):
            msg = "Elasticsearch key-search response hits must be an object"
            raise StoreConnectionError(msg)
        hits = cast("dict[str, Any]", hits)
        hit_items = hits.get("hits")
        if not isinstance(hit_items, list):
            msg = "Elasticsearch key-search response hits.hits must be a list"
            raise StoreConnectionError(msg)
        hit_items = cast("list[Any]", hit_items)

        all_keys: list[str] = []

        for hit in hit_items:
            if not isinstance(hit, dict):
                msg = "Elasticsearch key-search hit must be an object"
                raise StoreConnectionError(msg)
            hit = cast("dict[str, Any]", hit)
            if hit.get("_index") != self._get_index_name(collection=collection):
                msg = "Elasticsearch key-search hit index does not match the requested collection"
                raise StoreConnectionError(msg)
            document_id = hit.get("_id")
            if not isinstance(document_id, str):
                msg = "Elasticsearch key-search hit document ID must be a string"
                raise StoreConnectionError(msg)

            fields = hit.get("fields")
            if not isinstance(fields, dict):
                msg = "Elasticsearch key-search hit fields must be an object"
                raise StoreConnectionError(msg)
            fields = cast("dict[str, Any]", fields)
            key_values = fields.get("key")
            if not isinstance(key_values, list):
                msg = "Elasticsearch key-search hit must contain exactly one string key field"
                raise StoreConnectionError(msg)
            key_values = cast("list[Any]", key_values)
            if len(key_values) != 1 or not isinstance(key_values[0], str):
                msg = "Elasticsearch key-search hit must contain exactly one string key field"
                raise StoreConnectionError(msg)
            key_values = cast("list[str]", key_values)
            try:
                expected_document_id = self._get_document_id(key=key_values[0])
            except InvalidKeyError as error:
                msg = "Elasticsearch key-search hit contains an invalid canonical key"
                raise StoreConnectionError(msg) from error
            if document_id != expected_document_id:
                msg = "Elasticsearch key-search hit document ID does not match the returned key"
                raise StoreConnectionError(msg)

            all_keys.append(key_values[0])

        return all_keys[:limit]

    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        """List up to 10,000 canonical OpenKeyV collection names."""

        limit = min(DEFAULT_PAGE_SIZE if limit is None else limit, PAGE_LIMIT)
        index_names = await self._get_owned_index_names()
        collection_names = [self._decode_index_name(index_name=index_name) for index_name in index_names]
        return collection_names[:limit]

    async def _delete_collection(self, *, collection: str) -> bool:
        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).delete_by_query(
            index=self._get_index_name(collection=collection),
            body={
                "query": {
                    "term": {
                        "collection": collection,
                    },
                },
            },
            refresh=True,
        )
        body = result.body
        if not isinstance(body, dict):
            msg = "Elasticsearch delete-by-query response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch delete-by-query response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)

        if body.get("timed_out") is not False:
            msg = "Elasticsearch delete-by-query timed out"
            raise StoreConnectionError(msg)
        failures = body.get("failures")
        if not isinstance(failures, list) or failures:
            msg = "Elasticsearch delete-by-query reported failures"
            raise StoreConnectionError(msg)

        deleted = body.get("deleted")
        if type(deleted) is not int or deleted < 0:
            msg = "Elasticsearch delete-by-query deleted count must be a non-negative integer"
            raise StoreConnectionError(msg)

        return deleted > 0

    async def _cull(self) -> None:
        index_names = await self._get_owned_index_names()
        if not index_names:
            return

        ms_epoch = int(now_as_epoch() * 1000)
        response = await self._client.options(ignore_status=404).delete_by_query(
            index=index_names,
            body={
                "query": {
                    "range": {
                        "expires_at": {"lt": ms_epoch},
                    },
                },
            },
            refresh=True,
        )
        body = response.body
        if not isinstance(body, dict):
            msg = "Elasticsearch cull response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[Any, Any]", body)
        if not all(isinstance(field, str) for field in body):
            msg = "Elasticsearch cull response body must be an object with string keys"
            raise StoreConnectionError(msg)
        body = cast("dict[str, Any]", body)
        if body.get("timed_out") is not False:
            msg = "Elasticsearch cull operation timed out"
            raise StoreConnectionError(msg)
        failures = body.get("failures")
        if not isinstance(failures, list) or failures:
            msg = "Elasticsearch cull operation reported failures"
            raise StoreConnectionError(msg)
