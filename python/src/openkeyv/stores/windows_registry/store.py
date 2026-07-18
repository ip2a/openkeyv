"""Windows Registry-based key-value store."""

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Literal, SupportsFloat, cast

from typing_extensions import override

from openkeyv._internal import _decode_entry, _encode_entry
from openkeyv._utils.beartype import bear_enforce
from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv.errors import InvalidKeyError
from openkeyv.protocols.key_value import StoreValue
from openkeyv.stores.base import BaseStore

try:
    import winreg
except ImportError as e:
    msg = "WindowsRegistryStore requires Windows platform (winreg module)"
    raise ImportError(msg) from e

DEFAULT_REGISTRY_PATH = "Software\\openkeyv"
DEFAULT_HIVE = "HKEY_CURRENT_USER"
REGISTRY_ENCODING_PREFIX = "okv1-"
MAX_REGISTRY_KEY_PATH_LENGTH = 255
MAX_REGISTRY_VALUE_NAME_LENGTH = 16_383


class WindowsRegistryStore(BaseStore):
    """Windows Registry-based key-value store.

    Each collection is a Registry subkey and each key is a REG_BINARY value. Logical
    collection and key names are transported as ``okv1-`` followed by lowercase hex
    of their exact UTF-8 bytes. This preserves case, Unicode normalization, NUL, path
    characters, and empty names without relying on Registry case-sensitive behavior.

    TTL is not natively supported by Windows Registry. Complete entries, including TTL
    metadata, use the OpenKeyV binary entry format and are checked at retrieval time.
    """

    def __init__(
        self,
        *,
        hive: Literal["HKEY_CURRENT_USER", "HKEY_LOCAL_MACHINE"] | None = None,
        registry_path: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Windows Registry store.

        Args:
            hive: The hive to use. Defaults to ``HKEY_CURRENT_USER``.
            registry_path: The registry path to use under the hive. Defaults to
                ``Software\\openkeyv``.
            default_collection: The default collection to use if no collection is provided.
        """
        hive_name = DEFAULT_HIVE if hive is None else hive
        self._hive = winreg.HKEY_LOCAL_MACHINE if hive_name == "HKEY_LOCAL_MACHINE" else winreg.HKEY_CURRENT_USER
        self._hive_name = hive_name
        self._registry_path = DEFAULT_REGISTRY_PATH if registry_path is None else registry_path

        super().__init__(default_collection=default_collection)

    def _get_registry_collection_name(self, *, collection: str) -> str:
        physical_name = REGISTRY_ENCODING_PREFIX + collection.encode("utf-8").hex()
        registry_path = f"{self._registry_path}\\{physical_name}"
        absolute_path = f"{self._hive_name}\\{registry_path}"
        if len(absolute_path.encode("utf-16-le")) // 2 > MAX_REGISTRY_KEY_PATH_LENGTH:
            msg = f"Windows Registry collection identity exceeds the maximum key path length of {MAX_REGISTRY_KEY_PATH_LENGTH} characters"
            raise InvalidKeyError(msg)
        return physical_name

    def _get_registry_value_name(self, *, key: str) -> str:
        physical_name = REGISTRY_ENCODING_PREFIX + key.encode("utf-8").hex()
        if len(physical_name) > MAX_REGISTRY_VALUE_NAME_LENGTH:
            msg = f"Windows Registry key identity exceeds the maximum value name length of {MAX_REGISTRY_VALUE_NAME_LENGTH} characters"
            raise InvalidKeyError(msg)
        return physical_name

    def _validate_identities(self, *, collection: str, keys: Sequence[str]) -> None:
        self._get_registry_collection_name(collection=collection)
        for key in keys:
            self._get_registry_value_name(key=key)

    def _get_registry_path(self, *, collection: str) -> str:
        """Get the full registry path for a collection."""
        physical_collection = self._get_registry_collection_name(collection=collection)
        return f"{self._registry_path}\\{physical_collection}"

    @override
    async def setup_collection(self, *, collection: str) -> None:
        self._get_registry_collection_name(collection=collection)
        await super().setup_collection(collection=collection)

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        registry_path = self._get_registry_path(collection=collection)
        with winreg.CreateKey(self._hive, registry_path):
            pass

    @bear_enforce
    @override
    async def get(self, key: str, *, collection: str | None = None) -> StoreValue:
        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=(key,))
        return await super().get(key=key, collection=collection)

    @bear_enforce
    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[StoreValue]:
        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=keys)
        return await super().get_many(keys=keys, collection=collection)

    @bear_enforce
    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[StoreValue, float | None]:
        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=(key,))
        return await super().ttl(key=key, collection=collection)

    @bear_enforce
    @override
    async def ttl_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[tuple[StoreValue, float | None]]:
        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=keys)
        return await super().ttl_many(keys=keys, collection=collection)

    @bear_enforce
    @override
    async def put(
        self,
        key: str,
        value: StoreValue,
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=(key,))
        await super().put(key=key, value=value, collection=collection, ttl=ttl)

    @bear_enforce
    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[StoreValue],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        if len(keys) != len(values):
            msg = "put_many called but a different number of keys and values were provided"
            raise ValueError(msg) from None

        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=keys)
        await super().put_many(keys=keys, values=values, collection=collection, ttl=ttl)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        physical_key = self._get_registry_value_name(key=key)
        registry_path = self._get_registry_path(collection=collection)

        with winreg.OpenKey(self._hive, registry_path) as registry_key:
            try:
                raw_value = cast("tuple[object, int]", winreg.QueryValueEx(registry_key, physical_key))
            except FileNotFoundError:
                return None

        encoded, value_type = raw_value
        if value_type != winreg.REG_BINARY:
            msg = "Windows Registry entry must use REG_BINARY"
            raise ValueError(msg)
        if not isinstance(encoded, bytes):
            msg = "Windows Registry entry must contain bytes"
            raise TypeError(msg)

        decoded = cast("tuple[object, int | None, int | None]", _decode_entry(encoded))
        value, created_at_millis, expires_at_millis = decoded
        if not isinstance(value, dict):
            msg = "Windows Registry entry value must be a dict with string keys"
            raise TypeError(msg)
        typed_value = cast("dict[object, object]", value)
        if not all(isinstance(item_key, str) for item_key in typed_value):
            msg = "Windows Registry entry value must be a dict with string keys"
            raise TypeError(msg)

        created_at = None if created_at_millis is None else datetime.fromtimestamp(created_at_millis / 1000, tz=timezone.utc)
        expires_at = None if expires_at_millis is None else datetime.fromtimestamp(expires_at_millis / 1000, tz=timezone.utc)
        return ManagedEntry(
            value=cast("dict[str, object]", typed_value),
            created_at=created_at,
            expires_at=expires_at,
        )

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        physical_key = self._get_registry_value_name(key=key)
        registry_path = self._get_registry_path(collection=collection)
        created_at_millis = None if managed_entry.created_at is None else int(managed_entry.created_at.timestamp() * 1000)
        expires_at_millis = None if managed_entry.expires_at is None else int(managed_entry.expires_at.timestamp() * 1000)
        encoded = _encode_entry(
            dict(managed_entry.value) if isinstance(managed_entry.value, dict) else managed_entry.value,
            created_at_millis,
            expires_at_millis,
        )

        with winreg.OpenKey(self._hive, registry_path, access=winreg.KEY_SET_VALUE) as registry_key:
            winreg.SetValueEx(registry_key, physical_key, 0, winreg.REG_BINARY, encoded)

    @bear_enforce
    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=(key,))
        return await super().delete(key=key, collection=collection)

    @bear_enforce
    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        resolved_collection = self.default_collection if collection is None else collection
        self._validate_identities(collection=resolved_collection, keys=keys)
        return await super().delete_many(keys=keys, collection=collection)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        physical_key = self._get_registry_value_name(key=key)
        registry_path = self._get_registry_path(collection=collection)

        with winreg.OpenKey(self._hive, registry_path, access=winreg.KEY_SET_VALUE) as registry_key:
            try:
                winreg.DeleteValue(registry_key, physical_key)
            except FileNotFoundError:
                return False
        return True
