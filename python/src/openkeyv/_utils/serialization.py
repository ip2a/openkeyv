"""JSON serialization for wrapper and logging boundaries."""

from typing import Any

import orjson

from openkeyv._utils.beartype import bear_enforce
from openkeyv.errors import DeserializationError, SerializationError
from openkeyv.protocols.key_value import StoreValue


@bear_enforce
def dump_to_json(obj: StoreValue) -> str:
    """Serialize a value to a sorted JSON string."""
    try:
        return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS).decode()
    except (ValueError, TypeError) as error:
        msg = f"Failed to serialize object to JSON: {error}"
        raise SerializationError(msg) from error


@bear_enforce
def dump_to_json_bytes(obj: StoreValue) -> bytes:
    """Serialize a value to compact JSON bytes."""
    try:
        return orjson.dumps(obj)
    except (ValueError, TypeError) as error:
        msg = f"Failed to serialize object to JSON: {error}"
        raise SerializationError(msg) from error


@bear_enforce
def load_from_json(json_str: str | bytes) -> Any:
    """Deserialize JSON string or bytes to a native Python value."""
    try:
        return orjson.loads(json_str)
    except (ValueError, TypeError) as error:
        msg = f"Failed to deserialize JSON string: {error}"
        raise DeserializationError(msg) from error
