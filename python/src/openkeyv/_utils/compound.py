"""Utilities for canonical compound identities and explicit prefixes."""

from collections.abc import Sequence

from openkeyv._utils.beartype import bear_enforce

DEFAULT_PREFIX_SEPARATOR = "__"


@bear_enforce
def collection_prefix(collection: str) -> str:
    """Return the canonical length-prefixed identity for a collection."""
    return f"{len(collection.encode('utf-8'))}:{collection}"


@bear_enforce
def compound_key(collection: str, key: str) -> str:
    """Encode an exact ``(collection, key)`` pair as one canonical identity."""
    return f"{collection_prefix(collection)}{key}"


@bear_enforce
def uncompound_key(compound: str) -> tuple[str, str]:
    """Decode a canonical compound identity into its collection and key."""
    length_text, delimiter, payload = compound.partition(":")
    if delimiter == "":
        msg = f"String {compound} is not a canonical compound identifier"
        raise TypeError(msg) from None
    if not length_text or not all("0" <= character <= "9" for character in length_text):
        msg = f"String {compound} has an invalid collection length"
        raise TypeError(msg) from None
    if len(length_text) > 1 and length_text.startswith("0"):
        msg = f"String {compound} has a non-canonical collection length"
        raise TypeError(msg) from None

    try:
        collection_length = int(length_text)
    except ValueError as error:
        msg = f"String {compound} has an invalid collection length"
        raise TypeError(msg) from error

    payload_bytes = payload.encode("utf-8")
    if len(payload_bytes) < collection_length:
        msg = f"String {compound} has a collection length outside the payload"
        raise TypeError(msg) from None

    try:
        collection = payload_bytes[:collection_length].decode("utf-8")
        key = payload_bytes[collection_length:].decode("utf-8")
    except UnicodeDecodeError as error:
        msg = f"String {compound} has a collection length outside a UTF-8 boundary"
        raise TypeError(msg) from error

    return collection, key


def prefix_key(key: str, prefix: str, separator: str | None = None) -> str:
    """Add an explicit separator-based prefix to a key."""
    separator = DEFAULT_PREFIX_SEPARATOR if separator is None else separator
    if separator == "":
        msg = "Separator must not be empty"
        raise ValueError(msg)
    return f"{prefix}{separator}{key}"


def unprefix_key(key: str, prefix: str, separator: str | None = None) -> str:
    """Remove an explicit separator-based prefix from a key."""
    separator = DEFAULT_PREFIX_SEPARATOR if separator is None else separator
    if separator == "":
        msg = "Separator must not be empty"
        raise ValueError(msg)
    if not key.startswith(prefix + separator):
        msg = f"Key {key} is not prefixed with {prefix}{separator}"
        raise ValueError(msg)
    return key[len(prefix + separator) :]


def prefix_collection(collection: str, prefix: str, separator: str | None = None) -> str:
    """Add an explicit separator-based prefix to a collection name."""
    separator = DEFAULT_PREFIX_SEPARATOR if separator is None else separator
    if separator == "":
        msg = "Separator must not be empty"
        raise ValueError(msg)
    return f"{prefix}{separator}{collection}"


def unprefix_collection(collection: str, prefix: str, separator: str | None = None) -> str:
    """Remove an explicit separator-based prefix from a collection name."""
    separator = DEFAULT_PREFIX_SEPARATOR if separator is None else separator
    if separator == "":
        msg = "Separator must not be empty"
        raise ValueError(msg)
    if not collection.startswith(prefix + separator):
        msg = f"Collection {collection} is not prefixed with {prefix}{separator}"
        raise ValueError(msg)
    return collection[len(prefix + separator) :]


def get_collections_from_compound_keys(compound_keys: Sequence[str]) -> list[str]:
    """Return the unique collections represented by canonical compound keys."""
    return list({collection for collection, _ in (uncompound_key(compound) for compound in compound_keys)})


def get_keys_from_compound_keys(compound_keys: Sequence[str], collection: str) -> list[str]:
    """Return canonical compound keys belonging to ``collection``."""
    return [key for key_collection, key in (uncompound_key(compound) for compound in compound_keys) if key_collection == collection]
