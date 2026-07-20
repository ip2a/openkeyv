"""Elasticsearch document encoding at the external JSON boundary."""

import base64
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

import orjson

from openkeyv._internal import _decode_entry, _encode_entry
from openkeyv._utils.managed_entry import ManagedEntry
from openkeyv.errors import DeserializationError, SerializationError

if TYPE_CHECKING:
    from openkeyv.protocols.key_value import StoreValue


def _milliseconds(value: datetime | None) -> int | None:
    return None if value is None else int(value.timestamp() * 1000)


def _datetime(value: int | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc)


class ElasticsearchDocumentCodec:
    """Store the Rust core ``OKVE1`` entry inside an Elasticsearch binary field."""

    def load_json(self, json_str: str) -> ManagedEntry:
        try:
            loaded: object = orjson.loads(json_str)
        except (orjson.JSONDecodeError, TypeError) as error:
            msg = f"Invalid Elasticsearch document JSON: {error}"
            raise DeserializationError(msg) from error
        if not isinstance(loaded, dict):
            msg = "Elasticsearch document must be an object with string keys"
            raise DeserializationError(msg)
        data = cast("dict[object, object]", loaded)
        if not all(isinstance(key, str) for key in data):
            msg = "Elasticsearch document must be an object with string keys"
            raise DeserializationError(msg)
        return self.load_dict(cast("dict[str, Any]", data))

    def load_dict(self, data: dict[str, Any]) -> ManagedEntry:
        encoded = data.get("entry")
        if not isinstance(encoded, str):
            msg = "Elasticsearch document entry must be a base64 string"
            raise DeserializationError(msg)
        try:
            raw = base64.b64decode(encoded, validate=True)
            value, created_at_millis, expires_at_millis = cast("tuple[object, int | None, int | None]", _decode_entry(raw))
        except (ValueError, TypeError) as error:
            msg = f"Invalid Elasticsearch entry: {error}"
            raise DeserializationError(msg) from error
        return ManagedEntry(
            value=cast("StoreValue", value),
            created_at=_datetime(created_at_millis),
            expires_at=_datetime(expires_at_millis),
        )

    def dump_dict(
        self,
        entry: ManagedEntry,
        exclude_none: bool = True,
        *,
        key: str | None = None,
        collection: str | None = None,
    ) -> dict[str, Any]:
        try:
            encoded = _encode_entry(entry.value, _milliseconds(entry.created_at), _milliseconds(entry.expires_at))
        except (ValueError, TypeError) as error:
            msg = f"Invalid Elasticsearch entry: {error}"
            raise SerializationError(msg) from error
        data: dict[str, Any] = {
            "entry": base64.b64encode(encoded).decode("ascii"),
            "created_at": entry.created_at_isoformat,
            "expires_at": entry.expires_at_isoformat,
            "key": key,
            "collection": collection,
        }
        return {name: value for name, value in data.items() if value is not None} if exclude_none else data

    def dump_json(
        self,
        entry: ManagedEntry,
        exclude_none: bool = True,
        *,
        key: str | None = None,
        collection: str | None = None,
    ) -> str:
        try:
            return orjson.dumps(self.dump_dict(entry, exclude_none, key=key, collection=collection)).decode()
        except (orjson.JSONEncodeError, TypeError) as error:
            msg = f"Failed to serialize Elasticsearch document: {error}"
            raise SerializationError(msg) from error
