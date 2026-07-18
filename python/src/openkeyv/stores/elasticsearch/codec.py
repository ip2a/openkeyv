"""Elasticsearch-specific document codec.

Converts between ManagedEntry objects and Elasticsearch document format.
This is an internal implementation detail of the Elasticsearch store, not a public API.
"""

from datetime import datetime
from typing import Any, Literal, TypeVar, cast

from openkeyv._utils.beartype import bear_enforce
from openkeyv._utils.managed_entry import ManagedEntry, dump_to_json, load_from_json
from openkeyv.errors import DeserializationError, SerializationError

T = TypeVar("T")


@bear_enforce
def key_must_be(dictionary: dict[str, Any], /, key: str, expected_type: type[T]) -> T | None:
    """Check that a dictionary key is of the expected type, returning None if missing."""
    if key not in dictionary:
        return None
    if not isinstance(dictionary[key], expected_type):
        msg = f"{key} must be a {expected_type.__name__}"
        raise TypeError(msg)
    return dictionary[key]


@bear_enforce
def parse_datetime_str(value: str) -> datetime:
    """Parse an ISO format datetime string."""
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        msg = f"Invalid datetime string: {value}"
        raise DeserializationError(message=msg) from None


class ElasticsearchDocumentCodec:
    """Document codec for Elasticsearch documents.

    Converts between ManagedEntry objects and the Elasticsearch document format
    where values are stored under a ``value.flattened`` key and timestamps are
    ISO-format strings.
    """

    _date_format: Literal["isoformat", "datetime"]
    _value_format: Literal["string", "dict"]

    def __init__(
        self, *, date_format: Literal["isoformat", "datetime"] = "isoformat", value_format: Literal["string", "dict"] = "dict"
    ) -> None:
        self._date_format = date_format
        self._value_format = value_format

    def load_json(self, json_str: str) -> ManagedEntry:
        """Convert a JSON string to a ManagedEntry."""
        loaded_data: dict[str, Any] = load_from_json(json_str=json_str)
        return self.load_dict(data=loaded_data)

    def load_dict(self, data: dict[str, Any]) -> ManagedEntry:
        """Convert an Elasticsearch document dict to a ManagedEntry."""
        if not isinstance(data.get("created_at"), str):
            msg = "Elasticsearch document created_at must be an ISO datetime string"
            raise DeserializationError(msg)

        if "expires_at" in data and not isinstance(data["expires_at"], str):
            msg = "Elasticsearch document expires_at must be an ISO datetime string"
            raise DeserializationError(msg)

        managed_entry_proto: dict[str, Any] = {}

        if created_at := key_must_be(data, key="created_at", expected_type=str):
            managed_entry_proto["created_at"] = parse_datetime_str(created_at)
        if expires_at := key_must_be(data, key="expires_at", expected_type=str):
            managed_entry_proto["expires_at"] = parse_datetime_str(expires_at)

        if "value" not in data:
            msg = "Value field not found"
            raise DeserializationError(message=msg)

        value = data["value"]

        if not isinstance(value, dict):
            msg = "Elasticsearch document value must be an object"
            raise DeserializationError(msg)

        value_object = cast("dict[object, object]", value)
        flattened = value_object.get("flattened")
        if not isinstance(flattened, dict):
            msg = "Elasticsearch document value.flattened must be an object with string keys"
            raise DeserializationError(msg)
        flattened_object = cast("dict[object, object]", flattened)
        if not all(isinstance(key, str) for key in flattened_object):
            msg = "Elasticsearch document value.flattened must be an object with string keys"
            raise DeserializationError(msg)

        managed_entry_value = dict(cast("dict[str, Any]", flattened_object))

        return ManagedEntry(
            value=managed_entry_value,
            created_at=managed_entry_proto.get("created_at"),
            expires_at=managed_entry_proto.get("expires_at"),
        )

    def dump_dict(
        self,
        entry: ManagedEntry,
        exclude_none: bool = True,
        *,
        key: str | None = None,
        collection: str | None = None,
    ) -> dict[str, Any]:
        """Convert a ManagedEntry to an Elasticsearch document dict.

        Args:
            entry: The ManagedEntry to serialize
            exclude_none: Whether to exclude None values from the output
            key: Optional unsanitized key name to include in the document
            collection: Optional unsanitized collection name to include in the document

        Returns:
            A dictionary representation of the ManagedEntry for Elasticsearch.
        """

        if self._value_format == "dict":
            raw = entry.value
            if not isinstance(raw, dict):
                msg = "Elasticsearch value_format='dict' requires a dict value"
                raise SerializationError(msg)
            value: dict[str, Any] | str = raw
        else:
            value = entry.value_as_json

        data: dict[str, Any] = {
            "value": {"flattened": value},
        }

        if key is not None:
            data["key"] = key

        if collection is not None:
            data["collection"] = collection

        if self._date_format == "isoformat":
            data["created_at"] = entry.created_at_isoformat
            data["expires_at"] = entry.expires_at_isoformat

        if self._date_format == "datetime":
            data["created_at"] = entry.created_at
            data["expires_at"] = entry.expires_at

        if exclude_none:
            data = {k: v for k, v in data.items() if v is not None}

        return data

    def dump_json(
        self,
        entry: ManagedEntry,
        exclude_none: bool = True,
        *,
        key: str | None = None,
        collection: str | None = None,
    ) -> str:
        """Convert a ManagedEntry to a JSON string.

        Args:
            entry: The ManagedEntry to serialize
            exclude_none: Whether to exclude None values from the output
            key: Optional unsanitized key name to include in the document
            collection: Optional unsanitized collection name to include in the document

        Returns:
            A JSON string representation of the ManagedEntry for Elasticsearch.
        """
        if self._date_format == "datetime":
            msg = 'dump_json is incompatible with date_format="datetime"; use date_format="isoformat" or dump_dict().'
            raise SerializationError(msg)
        return dump_to_json(obj=self.dump_dict(entry=entry, exclude_none=exclude_none, key=key, collection=collection))
