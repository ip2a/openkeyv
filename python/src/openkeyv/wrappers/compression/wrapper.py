import base64
import binascii
import gzip
from collections.abc import Sequence
from typing import SupportsFloat

from typing_extensions import override

from openkeyv._utils.serialization import dump_to_json_bytes, load_from_json
from openkeyv.errors import DecompressionError, DeserializationError
from openkeyv.protocols.key_value import AsyncKeyValue, StoreValue
from openkeyv.wrappers.base import BaseWrapper

# Special keys used to store compressed data
_COMPRESSED_DATA_KEY = "__compressed_data__"
_COMPRESSION_VERSION_KEY = "__compression_version__"
_COMPRESSION_VERSION = 1
_COMPRESSION_ALGORITHM_KEY = "__compression_algorithm__"
_COMPRESSION_ALGORITHM = "gzip"
_COMPRESSION_KEYS = frozenset(
    {
        _COMPRESSED_DATA_KEY,
        _COMPRESSION_VERSION_KEY,
        _COMPRESSION_ALGORITHM_KEY,
    }
)


class CompressionWrapper(BaseWrapper):
    """Wrapper that compresses values before storing and decompresses on retrieval.

    This wrapper compresses the JSON-serialized value using gzip and stores it as a
    base64-encoded string within a special envelope dict. This allows compression
    for any JSON-serializable StoreValue (dict, list, str, int, etc.).

    The compressed envelope looks like:
    {
        "__compressed_data__": "base64-encoded-compressed-data",
        "__compression_algorithm__": "gzip",
        "__compression_version__": 1
    }
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        min_size_to_compress: int = 1024,
    ) -> None:
        """Initialize the compression wrapper.

        Args:
            key_value: The store to wrap.
            min_size_to_compress: Only compress values larger than this many bytes.
                                 Defaults to 1024 bytes (1KB).
        """
        self.key_value: AsyncKeyValue = key_value
        self.min_size_to_compress: int = min_size_to_compress

        super().__init__()

    def _compress_value(self, value: StoreValue) -> StoreValue:
        """Compress a value into the compressed envelope format."""
        # If already a compressed envelope, decompress first then re-compress
        if isinstance(value, dict) and _COMPRESSED_DATA_KEY in value:
            decompressed = self._decompress_value(value)
            return self._compress_value(decompressed)

        # Serialize to compact JSON bytes once and check size
        json_bytes = dump_to_json_bytes(obj=value)
        if len(json_bytes) < self.min_size_to_compress:
            return value

        # Compress with gzip
        compressed_bytes = gzip.compress(json_bytes, compresslevel=1)

        # Encode to base64 for storage in dict
        base64_str = base64.b64encode(compressed_bytes).decode("ascii")

        return {
            _COMPRESSED_DATA_KEY: base64_str,
            _COMPRESSION_VERSION_KEY: _COMPRESSION_VERSION,
            _COMPRESSION_ALGORITHM_KEY: _COMPRESSION_ALGORITHM,
        }

    def _decompress_value(self, value: StoreValue) -> StoreValue:
        """Decompress a value from the compressed envelope format."""
        if value is None:
            return None

        if not (isinstance(value, dict) and _COMPRESSED_DATA_KEY in value):
            return value

        if value.keys() != _COMPRESSION_KEYS:
            msg = "Compressed value must contain exactly the data, version, and algorithm fields."
            raise DecompressionError(msg)

        base64_str = value[_COMPRESSED_DATA_KEY]
        if not isinstance(base64_str, str):
            msg = "Compressed data must be a Base64 string."
            raise DecompressionError(msg)

        version = value[_COMPRESSION_VERSION_KEY]
        if type(version) is not int or version != _COMPRESSION_VERSION:
            msg = f"Unsupported compression version: {version!r}."
            raise DecompressionError(msg)

        algorithm = value[_COMPRESSION_ALGORITHM_KEY]
        if algorithm != _COMPRESSION_ALGORITHM:
            msg = f"Unsupported compression algorithm: {algorithm!r}."
            raise DecompressionError(msg)

        try:
            compressed_bytes = base64.b64decode(base64_str, validate=True)
            json_bytes = gzip.decompress(compressed_bytes)
            return load_from_json(json_str=json_bytes)
        except (binascii.Error, OSError, DeserializationError, TypeError, ValueError) as e:
            msg = "Failed to decompress stored value."
            raise DecompressionError(msg) from e

    @override
    async def get(self, key: str, *, collection: str | None = None) -> StoreValue:
        value = await self.key_value.get(key=key, collection=collection)
        return self._decompress_value(value)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[StoreValue]:
        values = await self.key_value.get_many(keys=keys, collection=collection)
        return [self._decompress_value(value) for value in values]

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[StoreValue, float | None]:
        value, ttl = await self.key_value.ttl(key=key, collection=collection)
        return self._decompress_value(value), ttl

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[StoreValue, float | None]]:
        results = await self.key_value.ttl_many(keys=keys, collection=collection)
        return [(self._decompress_value(value), ttl) for value, ttl in results]

    @override
    async def put(self, key: str, value: StoreValue, *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        compressed_value = self._compress_value(value)
        return await self.key_value.put(key=key, value=compressed_value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[StoreValue],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        compressed_values = [self._compress_value(value) for value in values]
        return await self.key_value.put_many(keys=keys, values=compressed_values, collection=collection, ttl=ttl)
