import base64
import binascii
from collections.abc import Callable, Sequence
from typing import Any, SupportsFloat

from typing_extensions import override

from openkeyv._utils.serialization import dump_to_json_bytes, load_from_json
from openkeyv.errors import CorruptedDataError, DecryptionError, DeserializationError, EncryptionError
from openkeyv.protocols.key_value import AsyncKeyValue, StoreValue
from openkeyv.wrappers.base import BaseWrapper

_ENCRYPTED_DATA_KEY = "__encrypted_data__"
_ENCRYPTION_VERSION_KEY = "__encryption_version__"
_ENCRYPTION_KEYS = frozenset({_ENCRYPTED_DATA_KEY, _ENCRYPTION_VERSION_KEY})


EncryptionFn = Callable[[bytes], bytes]
DecryptionFn = Callable[[bytes, int], bytes]


class BaseEncryptionWrapper(BaseWrapper):
    """Wrapper that encrypts values before storing and decrypts on retrieval.

    This wrapper encrypts the JSON-serialized value using a custom encryption function
    and stores it as a base64-encoded string within a special envelope dict. This allows
    encryption for any JSON-serializable StoreValue (dict, list, str, int, etc.).

    The encrypted envelope looks like:
    {
        "__encrypted_data__": "base64-encoded-encrypted-data",
        "__encryption_version__": 1
    }
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        encryption_fn: EncryptionFn,
        decryption_fn: DecryptionFn,
        encryption_version: int,
    ) -> None:
        """Initialize the encryption wrapper.

        Args:
            key_value: The store to wrap.
            encryption_fn: The encryption function to use. A callable that takes bytes and returns encrypted bytes.
            decryption_fn: The decryption function to use. A callable that takes bytes and an
                           encryption version int and returns decrypted bytes.
            encryption_version: The encryption version to use.
        """
        if type(encryption_version) is not int:
            msg = "Encryption version must be an integer."
            raise TypeError(msg)

        self.key_value: AsyncKeyValue = key_value

        self.encryption_version: int = encryption_version

        self._encryption_fn: EncryptionFn = encryption_fn
        self._decryption_fn: DecryptionFn = decryption_fn

        super().__init__()

    def _encrypt_value(self, value: StoreValue) -> dict[str, int | str]:
        """Encrypt a value into the encrypted envelope format."""
        json_bytes = dump_to_json_bytes(obj=value)

        try:
            encrypted_bytes: bytes = self._encryption_fn(json_bytes)

            base64_str: str = base64.b64encode(encrypted_bytes).decode(encoding="ascii")
        except Exception as e:
            msg = "Failed to encrypt value"
            raise EncryptionError(msg) from e

        return {
            _ENCRYPTED_DATA_KEY: base64_str,
            _ENCRYPTION_VERSION_KEY: self.encryption_version,
        }

    def _validate_encrypted_payload(self, value: dict[str, Any]) -> tuple[int, str]:
        if value.keys() != _ENCRYPTION_KEYS:
            msg = "Encrypted value must contain exactly the data and version fields."
            raise CorruptedDataError(msg)

        encryption_version = value[_ENCRYPTION_VERSION_KEY]
        if type(encryption_version) is not int:
            msg = f"expected encryption version to be an int, got {type(encryption_version)}"
            raise CorruptedDataError(msg)

        encrypted_data = value[_ENCRYPTED_DATA_KEY]

        if not isinstance(encrypted_data, str):
            msg = f"expected encrypted data to be a str, got {type(encrypted_data)}"
            raise CorruptedDataError(msg)

        return encryption_version, encrypted_data

    def _decrypt_value(self, value: StoreValue) -> StoreValue:
        """Decrypt a value from the encrypted envelope format.

        Non-dict values pass through unchanged (they may be raw bytes, ints, etc.
        written outside the encryption wrapper). Dict values must be valid encrypted
        envelopes — a dict that resembles data but is not encrypted is treated as
        corrupted.
        """
        if value is None:
            return None

        if not isinstance(value, dict):
            return value

        encryption_version, encrypted_data = self._validate_encrypted_payload(value)

        try:
            encrypted_bytes = base64.b64decode(encrypted_data, validate=True)
        except (binascii.Error, ValueError) as e:
            msg = "Encrypted data must be a valid Base64 string."
            raise CorruptedDataError(msg) from e

        try:
            json_bytes = self._decryption_fn(encrypted_bytes, encryption_version)
        except EncryptionError:
            raise
        except Exception as e:
            msg = "Failed to decrypt value."
            raise DecryptionError(msg) from e

        try:
            return load_from_json(json_str=json_bytes)
        except (DeserializationError, TypeError) as e:
            msg = "Decrypted value must contain a JSON object."
            raise CorruptedDataError(msg) from e

    @override
    async def get(self, key: str, *, collection: str | None = None) -> StoreValue:
        value = await self.key_value.get(key=key, collection=collection)
        return self._decrypt_value(value)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[StoreValue]:
        values = await self.key_value.get_many(keys=keys, collection=collection)
        return [self._decrypt_value(value) for value in values]

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[StoreValue, float | None]:
        value, ttl = await self.key_value.ttl(key=key, collection=collection)
        return self._decrypt_value(value), ttl

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[StoreValue, float | None]]:
        results = await self.key_value.ttl_many(keys=keys, collection=collection)
        return [(self._decrypt_value(value), ttl) for value, ttl in results]

    @override
    async def put(self, key: str, value: StoreValue, *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        encrypted_value = self._encrypt_value(value)
        return await self.key_value.put(key=key, value=encrypted_value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[StoreValue],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        encrypted_values = [self._encrypt_value(value) for value in values]
        return await self.key_value.put_many(keys=keys, values=encrypted_values, collection=collection, ttl=ttl)
