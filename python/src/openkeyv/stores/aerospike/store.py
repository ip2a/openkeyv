from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from math import ceil
from typing import Any, SupportsFloat, cast, overload

from typing_extensions import override

from openkeyv._internal import _decode_entry, _encode_entry
from openkeyv._utils.beartype import bear_enforce
from openkeyv._utils.compound import compound_key, get_keys_from_compound_keys, uncompound_key
from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv.errors import InvalidKeyError
from openkeyv.stores.base import BaseContextManagerStore, BaseDestroyStore, BaseEnumerateKeysStore, BaseStore

try:
    import aerospike
except ImportError as e:
    msg = "AerospikeStore requires openkeyv[aerospike]"
    raise ImportError(msg) from e

DEFAULT_NAMESPACE = "test"
DEFAULT_SET = "kv-store"
DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000
MAX_COMPOUND_KEY_BYTES = 1_048_000


class AerospikeStore(BaseDestroyStore, BaseEnumerateKeysStore, BaseContextManagerStore, BaseStore):
    """Aerospike-based key-value store.

    Note: Aerospike namespaces must be pre-configured on the server. Sets are created
    automatically when the first record is written.

    When `auto_create=False`, the store will verify that the configured namespace exists
    during setup and raise a ValueError if it doesn't.
    """

    _client: aerospike.Client
    _namespace: str
    _set: str
    _auto_create: bool

    @overload
    def __init__(
        self,
        *,
        client: aerospike.Client,
        namespace: str = DEFAULT_NAMESPACE,
        set_name: str = DEFAULT_SET,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Aerospike store.

        Args:
            client: The Aerospike client to use. You must have connected the client before passing this in.
            namespace: Aerospike namespace. Defaults to "test".
            set_name: Aerospike set. Defaults to "kv-store".
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to skip namespace validation. When False, verifies the namespace
                exists during setup. Defaults to True.
        """

    @overload
    def __init__(
        self,
        *,
        hosts: list[tuple[str, int]] | None = None,
        namespace: str = DEFAULT_NAMESPACE,
        set_name: str = DEFAULT_SET,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Aerospike store.

        Args:
            hosts: List of (host, port) tuples. Defaults to [("localhost", 3000)].
            namespace: Aerospike namespace. Defaults to "test".
            set_name: Aerospike set. Defaults to "kv-store".
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to skip namespace validation. When False, verifies the namespace
                exists during setup. Defaults to True.
        """

    def __init__(
        self,
        *,
        client: aerospike.Client | None = None,
        hosts: list[tuple[str, int]] | None = None,
        namespace: str = DEFAULT_NAMESPACE,
        set_name: str = DEFAULT_SET,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Aerospike store.

        Args:
            client: An existing Aerospike client to use. If provided, the store will not manage
                the client's lifecycle (will not close it). The caller is responsible for
                managing the client's lifecycle.
            hosts: List of (host, port) tuples. Defaults to [("localhost", 3000)].
            namespace: Aerospike namespace. Defaults to "test".
            set_name: Aerospike set. Defaults to "kv-store".
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to skip namespace validation. When False, verifies the namespace
                exists during setup. Defaults to True. Note that Aerospike namespaces must be
                pre-configured on the server; this option only controls validation.
        """
        client_provided = client is not None

        if client is not None:
            self._client = client
        else:
            hosts = [("localhost", 3000)] if hosts is None else hosts
            config = {"hosts": hosts}
            self._client = aerospike.client(config)  # pyright: ignore[reportUnknownMemberType]

        self._namespace = namespace
        self._set = set_name
        self._auto_create = auto_create

        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided,
            stable_api=True,
        )

    def _physical_key(self, *, collection: str, key: str) -> str:
        combo_key = compound_key(collection=collection, key=key)
        if "\x00" in combo_key:
            msg = "Aerospike canonical identities cannot contain NUL"
            raise InvalidKeyError(msg)
        if len(combo_key.encode("utf-8")) > MAX_COMPOUND_KEY_BYTES:
            msg = f"Aerospike canonical identity exceeds the maximum size of {MAX_COMPOUND_KEY_BYTES} UTF-8 bytes"
            raise InvalidKeyError(msg)
        return combo_key

    def _validate_keys(self, *, collection: str, keys: Sequence[str]) -> None:
        for key in keys:
            self._physical_key(collection=collection, key=key)

    @bear_enforce
    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        collection = self.default_collection if collection is None else collection
        self._physical_key(collection=collection, key=key)
        return await super().get(key=key, collection=collection)

    @bear_enforce
    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        collection = self.default_collection if collection is None else collection
        self._validate_keys(collection=collection, keys=keys)
        return await super().get_many(keys=keys, collection=collection)

    @bear_enforce
    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        collection = self.default_collection if collection is None else collection
        self._physical_key(collection=collection, key=key)
        return await super().ttl(key=key, collection=collection)

    @bear_enforce
    @override
    async def ttl_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[tuple[dict[str, Any] | None, float | None]]:
        collection = self.default_collection if collection is None else collection
        self._validate_keys(collection=collection, keys=keys)
        return await super().ttl_many(keys=keys, collection=collection)

    @bear_enforce
    @override
    async def put(
        self,
        key: str,
        value: Mapping[str, Any],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        collection = self.default_collection if collection is None else collection
        self._physical_key(collection=collection, key=key)
        await super().put(key=key, value=value, collection=collection, ttl=ttl)

    @bear_enforce
    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        if len(keys) != len(values):
            msg = "put_many called but a different number of keys and values were provided"
            raise ValueError(msg) from None
        collection = self.default_collection if collection is None else collection
        self._validate_keys(collection=collection, keys=keys)
        await super().put_many(keys=keys, values=values, collection=collection, ttl=ttl)

    @bear_enforce
    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        collection = self.default_collection if collection is None else collection
        self._physical_key(collection=collection, key=key)
        return await super().delete(key=key, collection=collection)

    @bear_enforce
    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        collection = self.default_collection if collection is None else collection
        self._validate_keys(collection=collection, keys=keys)
        return await super().delete_many(keys=keys, collection=collection)

    @bear_enforce
    @override
    async def keys(self, collection: str | None = None, *, limit: int | None = None) -> list[str]:
        collection = self.default_collection if collection is None else collection
        self._physical_key(collection=collection, key="")
        return await super().keys(collection=collection, limit=limit)

    @override
    async def _setup(self) -> None:
        """Connect to Aerospike and register cleanup."""
        self._client.connect()

        if not self._client_provided_by_user:
            self._exit_stack.callback(self._client.close)

        if not self._auto_create:
            info_response = cast("object", self._client.info_all("namespaces"))  # pyright: ignore[reportUnknownMemberType]
            if not isinstance(info_response, dict):
                msg = "Aerospike namespaces response must be a dict"
                raise TypeError(msg)

            namespaces: set[str] = set()
            typed_info_response = cast("dict[object, object]", info_response)
            for result in typed_info_response.values():
                if not isinstance(result, tuple):
                    msg = "Aerospike namespace result must be an (error, response) tuple"
                    raise TypeError(msg)
                try:
                    error, response = cast("tuple[object, ...]", result)
                except ValueError as error:
                    msg = "Aerospike namespace result must be an (error, response) tuple"
                    raise TypeError(msg) from error
                if error is not None:
                    msg = f"Aerospike namespace query failed: {error!r}"
                    raise RuntimeError(msg)
                if not isinstance(response, str):
                    msg = "Aerospike namespace result contains invalid field types"
                    raise TypeError(msg)
                namespaces.update(namespace for item in response.split(";") if (namespace := item.strip()))

            if self._namespace not in namespaces:
                msg = (
                    f"Namespace '{self._namespace}' does not exist. "
                    "Either configure the namespace on the Aerospike server or set auto_create=True."
                )
                raise ValueError(msg)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key = self._physical_key(collection=collection, key=key)
        aerospike_key = (self._namespace, self._set, combo_key)

        try:
            record = cast("object", self._client.get(aerospike_key))  # pyright: ignore[reportUnknownMemberType]
        except aerospike.exception.RecordNotFound:  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
            return None

        if not isinstance(record, tuple):
            msg = "Aerospike record must be a (key, metadata, bins) tuple"
            raise TypeError(msg)
        try:
            _record_key, _metadata, bins = cast("tuple[object, ...]", record)
        except ValueError as error:
            msg = "Aerospike record must be a (key, metadata, bins) tuple"
            raise TypeError(msg) from error
        if not isinstance(bins, dict):
            msg = "Aerospike record bins must be a dict"
            raise TypeError(msg)
        typed_bins = cast("dict[object, object]", bins)
        if set(typed_bins) != {"value"}:
            msg = "Aerospike record must contain exactly one 'value' bin"
            raise ValueError(msg)

        encoded = typed_bins["value"]
        if not isinstance(encoded, bytes):
            msg = "Aerospike 'value' bin must contain bytes"
            raise TypeError(msg)

        decoded = cast("tuple[object, int | None, int | None]", _decode_entry(encoded))
        value, created_at_millis, expires_at_millis = decoded
        if not isinstance(value, dict):
            msg = "Aerospike entry value must be a dict with string keys"
            raise TypeError(msg)
        typed_value = cast("dict[object, object]", value)
        if not all(isinstance(item_key, str) for item_key in typed_value):
            msg = "Aerospike entry value must be a dict with string keys"
            raise TypeError(msg)

        created_at = None if created_at_millis is None else datetime.fromtimestamp(created_at_millis / 1000, tz=timezone.utc)
        expires_at = None if expires_at_millis is None else datetime.fromtimestamp(expires_at_millis / 1000, tz=timezone.utc)
        return ManagedEntry(
            value=cast("dict[str, object]", typed_value),
            created_at=created_at,
            expires_at=expires_at,
        )

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key = self._physical_key(collection=collection, key=key)
        aerospike_key = (self._namespace, self._set, combo_key)
        created_at_millis = None if managed_entry.created_at is None else int(managed_entry.created_at.timestamp() * 1000)
        expires_at_millis = None if managed_entry.expires_at is None else int(managed_entry.expires_at.timestamp() * 1000)
        encoded = _encode_entry(dict(managed_entry.value), created_at_millis, expires_at_millis)

        ttl = managed_entry.ttl
        native_ttl = aerospike.TTL_NEVER_EXPIRE if ttl is None else max(1, ceil(ttl))
        self._client.put(  # pyright: ignore[reportUnknownMemberType]
            aerospike_key,
            {"value": encoded},
            meta={"ttl": native_ttl},
            policy={"key": aerospike.POLICY_KEY_SEND},
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key = self._physical_key(collection=collection, key=key)
        aerospike_key = (self._namespace, self._set, combo_key)

        try:
            self._client.remove(aerospike_key)  # pyright: ignore[reportUnknownMemberType]
        except aerospike.exception.RecordNotFound:  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
            return False
        return True

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        limit = min(DEFAULT_PAGE_SIZE if limit is None else limit, PAGE_LIMIT)

        pattern = self._physical_key(collection=collection, key="")

        keys: list[str] = []

        def callback(record: object) -> None:
            if not isinstance(record, tuple):
                msg = "Aerospike scan record must be a (key, metadata, bins) tuple"
                raise TypeError(msg)
            try:
                key_tuple, _metadata, bins = cast("tuple[object, ...]", record)
            except ValueError as error:
                msg = "Aerospike scan record must be a (key, metadata, bins) tuple"
                raise TypeError(msg) from error
            if not isinstance(key_tuple, tuple):
                msg = "Aerospike scan key must contain namespace, set, and primary key"
                raise TypeError(msg)
            try:
                _namespace, _set, primary_key, *_digest = cast("tuple[object, ...]", key_tuple)
            except ValueError as error:
                msg = "Aerospike scan key must contain namespace, set, and primary key"
                raise TypeError(msg) from error
            if not isinstance(bins, dict):
                msg = "Aerospike scan bins must be a dict"
                raise TypeError(msg)

            if not isinstance(primary_key, str) or not primary_key.startswith(pattern):
                return
            if "\x00" in primary_key:
                msg = "Aerospike scan primary key cannot contain NUL"
                raise InvalidKeyError(msg)
            try:
                parsed_collection, parsed_key = uncompound_key(primary_key)
            except TypeError as error:
                msg = "Aerospike scan primary key is not a canonical compound identity"
                raise TypeError(msg) from error
            if parsed_collection != collection:
                msg = "Aerospike scan primary key is not a canonical compound identity"
                raise TypeError(msg)
            if self._physical_key(collection=parsed_collection, key=parsed_key) != primary_key:
                msg = "Aerospike scan primary key is not a canonical compound identity"
                raise TypeError(msg)
            keys.append(primary_key)

        scan = self._client.scan(self._namespace, self._set)
        scan.foreach(callback)  # pyright: ignore[reportUnknownMemberType]

        result_keys = get_keys_from_compound_keys(compound_keys=keys, collection=collection)

        return result_keys[:limit]

    @override
    async def _delete_store(self) -> bool:
        """Truncate the set (delete all records in the set)."""
        self._client.truncate(self._namespace, self._set, 0)  # pyright: ignore[reportUnknownMemberType]
        return True
