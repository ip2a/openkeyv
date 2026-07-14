"""Windows Registry-based key-value store."""

from datetime import datetime, timezone
from typing import Literal, cast

from typing_extensions import override

from openkeyv._internal import _decode_entry, _encode_entry  # pyright: ignore[reportUnknownVariableType]
from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv._utils.sanitization import HybridSanitizationStrategy, SanitizationStrategy
from openkeyv._utils.sanitize import ALPHANUMERIC_CHARACTERS
from openkeyv.stores.base import BaseStore

try:
    import winreg
except ImportError as e:
    msg = "WindowsRegistryStore requires Windows platform (winreg module)"
    raise ImportError(msg) from e

DEFAULT_REGISTRY_PATH = "Software\\openkeyv"
DEFAULT_HIVE = "HKEY_CURRENT_USER"

MAX_COLLECTION_LENGTH = 96


class WindowsRegistryV1CollectionSanitizationStrategy(HybridSanitizationStrategy):
    def __init__(self) -> None:
        super().__init__(
            max_length=MAX_COLLECTION_LENGTH,
            allowed_characters=ALPHANUMERIC_CHARACTERS,
        )


class WindowsRegistryStore(BaseStore):
    """Windows Registry-based key-value store.

    This store uses the Windows Registry to persist key-value pairs. Each entry is stored
    as a binary value under HKEY_CURRENT_USER\\Software\\{root}\\{collection}, with the
    key being the registry value name.

    This store has specific restrictions on what is allowed in collections. Collections are not sanitized
    by default which may result in errors when using the store.

    To avoid issues, you may want to consider leveraging the `WindowsRegistryV1CollectionSanitizationStrategy`.

    TTL is not natively supported by Windows Registry. Complete entries, including TTL
    metadata, use the OpenKeyV binary entry format and are checked at retrieval time.
    """

    def __init__(
        self,
        *,
        hive: Literal["HKEY_CURRENT_USER", "HKEY_LOCAL_MACHINE"] | None = None,
        registry_path: str | None = None,
        default_collection: str | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the Windows Registry store.

        Args:
            hive: The hive to use. Defaults to "HKEY_CURRENT_USER".
            registry_path: The registry path to use. Must be a valid registry path under the hive. Defaults to "Software\\openkeyv".
            default_collection: The default collection to use if no collection is provided.
            key_sanitization_strategy: The sanitization strategy to use for keys.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """
        self._hive = winreg.HKEY_LOCAL_MACHINE if hive == "HKEY_LOCAL_MACHINE" else winreg.HKEY_CURRENT_USER
        self._registry_path = DEFAULT_REGISTRY_PATH if registry_path is None else registry_path

        super().__init__(
            default_collection=default_collection,
            key_sanitization_strategy=key_sanitization_strategy,
            collection_sanitization_strategy=collection_sanitization_strategy,
        )

    def _get_registry_path(self, *, collection: str) -> str:
        """Get the full registry path for a collection."""
        sanitized_collection = self._sanitize_collection(collection=collection)
        return f"{self._registry_path}\\{sanitized_collection}"

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        registry_path = self._get_registry_path(collection=collection)
        with winreg.CreateKey(self._hive, registry_path):
            pass

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        sanitized_key = self._sanitize_key(key=key)
        registry_path = self._get_registry_path(collection=collection)

        with winreg.OpenKey(self._hive, registry_path) as registry_key:
            try:
                raw_value = cast("tuple[object, int]", winreg.QueryValueEx(registry_key, sanitized_key))
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
        sanitized_key = self._sanitize_key(key=key)
        registry_path = self._get_registry_path(collection=collection)
        created_at_millis = None if managed_entry.created_at is None else int(managed_entry.created_at.timestamp() * 1000)
        expires_at_millis = None if managed_entry.expires_at is None else int(managed_entry.expires_at.timestamp() * 1000)
        encoded = cast("bytes", _encode_entry(dict(managed_entry.value), created_at_millis, expires_at_millis))

        with winreg.OpenKey(self._hive, registry_path, access=winreg.KEY_SET_VALUE) as registry_key:
            winreg.SetValueEx(registry_key, sanitized_key, 0, winreg.REG_BINARY, encoded)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        sanitized_key = self._sanitize_key(key=key)
        registry_path = self._get_registry_path(collection=collection)

        with winreg.OpenKey(self._hive, registry_path, access=winreg.KEY_SET_VALUE) as registry_key:
            try:
                winreg.DeleteValue(registry_key, sanitized_key)
            except FileNotFoundError:
                return False
        return True
