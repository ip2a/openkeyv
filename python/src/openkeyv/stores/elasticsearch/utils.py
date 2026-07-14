from typing import Any, ClassVar

from elastic_transport import (
    JsonSerializer,
    NdjsonSerializer,
    SerializationError,
)
from elasticsearch import AsyncElasticsearch


class LessCapableJsonSerializer(JsonSerializer):
    """A JSON Serializer that doesnt try to be smart with datetime, floats, etc."""

    mimetype: ClassVar[str] = "application/json"
    compatibility_mimetype: ClassVar[str] = "application/vnd.elasticsearch+json"

    def default(self, data: Any) -> Any:
        raise SerializationError(
            message=f"Unable to serialize to JSON: {data!r} (type: {type(data).__name__})",
        )

    @classmethod
    def install_default_serializer(cls, client: AsyncElasticsearch) -> None:
        cls.install_serializer(client=client)
        client.transport.serializers.default_serializer = cls()

    @classmethod
    def install_serializer(cls, client: AsyncElasticsearch) -> None:
        client.transport.serializers.serializers.update(
            {
                cls.mimetype: cls(),
                cls.compatibility_mimetype: cls(),
            }
        )


class LessCapableNdjsonSerializer(NdjsonSerializer):
    """A NDJSON Serializer that doesnt try to be smart with datetime, floats, etc."""

    mimetype: ClassVar[str] = "application/x-ndjson"
    compatibility_mimetype: ClassVar[str] = "application/vnd.elasticsearch+x-ndjson"

    def default(self, data: Any) -> Any:
        return LessCapableJsonSerializer.default(self=self, data=data)  # pyright: ignore[reportArgumentType]

    @classmethod
    def install_serializer(cls, client: AsyncElasticsearch) -> None:
        client.transport.serializers.serializers.update(
            {
                cls.mimetype: cls(),
                cls.compatibility_mimetype: cls(),
            }
        )
