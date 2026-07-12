from collections.abc import Sequence
from typing import TypeVar, get_args, get_origin

from pydantic import BaseModel
from pydantic.type_adapter import TypeAdapter

from openkeyv._utils.beartype import no_bear_type_check
from openkeyv.adapters.pydantic.base import BasePydanticAdapter
from openkeyv.protocols.key_value import AsyncKeyValue

T = TypeVar("T", bound=BaseModel | Sequence[BaseModel])


class BaseModelAdapter(BasePydanticAdapter[T]):
    """Adapter around a KVStore-compliant Store that allows type-safe persistence of Pydantic BaseModels.

    This adapter is constrained to BaseModel subclasses and sequences of BaseModels,
    providing a safe, type-checked interface for Pydantic model persistence.
    """

    # Beartype cannot inspect type[T].
    @no_bear_type_check
    def __init__(
        self,
        key_value: AsyncKeyValue,
        pydantic_model: type[T],
        default_collection: str | None = None,
    ) -> None:
        """Create a new BaseModelAdapter.

        Args:
            key_value: The KVStore to use.
            pydantic_model: The Pydantic model to use. Can be a single BaseModel subclass or list[BaseModel].
            default_collection: The default collection to use.

        Raises:
            TypeError: If pydantic_model is a sequence type other than list (e.g., tuple is not supported).
        """
        self._key_value = key_value

        origin = get_origin(pydantic_model)
        self._needs_wrapping = origin is list

        # Validate that if it's a generic type, it must be a list (not tuple, etc.)
        if origin is not None and origin is not list:
            msg = f"Only list[BaseModel] is supported for sequence types, got {pydantic_model}"
            raise TypeError(msg)

        # Validate that the inner type is a BaseModel subclass
        if origin is list:
            args = get_args(pydantic_model)
            if not args:
                msg = f"List type must have a type argument, got {pydantic_model}"
                raise TypeError(msg)
            inner = args[0]
            if not (isinstance(inner, type) and issubclass(inner, BaseModel)):
                msg = f"List element type must be a BaseModel subclass, got {inner}"
                raise TypeError(msg)
        # Validate single model type
        elif not issubclass(pydantic_model, BaseModel):
            msg = f"pydantic_model must be a BaseModel subclass, got {pydantic_model}"
            raise TypeError(msg)

        self._type_adapter = TypeAdapter[T](pydantic_model)
        self._default_collection = default_collection

    def _get_model_type_name(self) -> str:
        """Return the model type name for error messages."""
        return "BaseModel"
