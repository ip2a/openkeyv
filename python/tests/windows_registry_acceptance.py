"""Real Windows Registry acceptance runner.

Run this file on a native Windows host after ``uv run maturin develop``. It uses a
unique HKCU subtree and removes that subtree before exiting.
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from typing import Any


def _utf16_units(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _physical_name(value: str) -> str:
    return "okv1-" + value.encode("utf-8").hex()


def _delete_registry_tree(winreg: Any, hive: Any, path: str) -> None:
    try:
        with winreg.OpenKey(hive, path, access=winreg.KEY_READ | winreg.KEY_WRITE) as key:
            children: list[str] = []
            index = 0
            while True:
                try:
                    children.append(winreg.EnumKey(key, index))
                except OSError:
                    break
                index += 1
    except FileNotFoundError:
        return

    for child in children:
        _delete_registry_tree(winreg, hive, f"{path}\\{child}")
    winreg.DeleteKey(hive, path)


async def _expect_invalid_key(operation: Coroutine[Any, Any, object], invalid_key_error: type[Exception]) -> None:
    try:
        await operation
    except invalid_key_error:
        return
    message = "operation accepted an identity beyond the documented Registry boundary"
    raise AssertionError(message)


async def _run() -> None:  # noqa: PLR0915
    import winreg

    from openkeyv.errors import InvalidKeyError
    from openkeyv.stores.windows_registry import WindowsRegistryStore
    from openkeyv.stores.windows_registry.store import MAX_REGISTRY_KEY_PATH_LENGTH, MAX_REGISTRY_VALUE_NAME_LENGTH

    run_id = uuid.uuid4().hex
    parent_path = "Software\\OpenKeyVAcceptance"
    registry_path = f"{parent_path}\\{run_id}"
    hive_name = "HKEY_CURRENT_USER"
    store = WindowsRegistryStore(hive=hive_name, registry_path=registry_path)

    started_at = time.monotonic()
    try:
        await store.setup()
        await store.setup()
        await store.setup_collection(collection="lifecycle")
        await store.setup_collection(collection="lifecycle")

        exact_cases = [
            ("", "", {"case": "empty"}),
            ("Users", "Key", {"case": "upper"}),
            ("users", "key", {"case": "lower"}),
            ("é", "e\u0301", {"case": "composed"}),
            ("e\u0301", "é", {"case": "decomposed"}),
            ("nul\x00path\\control\x01", "key/with?glob*", {"case": "reserved"}),
        ]
        for collection, key, value in exact_cases:
            await store.put(key, value, collection=collection)
            assert await store.get(key, collection=collection) == value
            assert await store.ttl(key, collection=collection) == (value, None)

        assert await store.get("Key", collection="Users") == {"case": "upper"}
        assert await store.get("key", collection="users") == {"case": "lower"}
        assert await store.get("key", collection="Users") is None
        assert await store.get("Key", collection="users") is None

        u64_value = {"u64": (1 << 64) - 1, "nested": [{"u64": 1 << 63}]}
        await store.put("u64", u64_value, collection="binary")
        assert await store.get("u64", collection="binary") == u64_value

        binary_path = f"{registry_path}\\{_physical_name('binary')}"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, binary_path) as binary_key:
            encoded, value_type = winreg.QueryValueEx(binary_key, _physical_name("u64"))
        assert value_type == winreg.REG_BINARY
        assert isinstance(encoded, bytes)
        assert encoded.startswith(b"OKVE1")

        raw_path = f"{registry_path}\\legacy"
        with winreg.CreateKey(winreg.HKEY_CURRENT_USER, raw_path) as raw_key:
            winreg.SetValueEx(raw_key, "raw", 0, winreg.REG_BINARY, encoded)
        assert await store.get("raw", collection="legacy") is None

        malformed_path = f"{registry_path}\\{_physical_name('malformed')}"
        with winreg.CreateKey(winreg.HKEY_CURRENT_USER, malformed_path) as malformed_key:
            winreg.SetValueEx(malformed_key, _physical_name("wrong-type"), 0, winreg.REG_SZ, "not binary")
        try:
            await store.get("wrong-type", collection="malformed")
        except ValueError as error:
            if "REG_BINARY" not in str(error):
                raise
        else:
            message = "WindowsRegistryStore accepted a non-REG_BINARY value"
            raise AssertionError(message)

        batch_keys = ["first", "second", "third"]
        batch_values = [{"position": 1}, {"position": 2}, {"position": 3}]
        await store.put_many(batch_keys, batch_values, collection="batch")
        assert await store.get_many(batch_keys, collection="batch") == batch_values
        assert await store.delete_many(["second", "missing"], collection="batch") == 1

        await store.put("expires", {"ttl": True}, collection="ttl", ttl=1.0)
        ttl_value = await store.ttl("expires", collection="ttl")
        assert ttl_value is not None
        assert ttl_value[0] == {"ttl": True}
        await asyncio.sleep(1.2)
        assert await store.get("expires", collection="ttl") is None
        assert await store.ttl("expires", collection="ttl") is None

        max_key_chars = (MAX_REGISTRY_VALUE_NAME_LENGTH - len("okv1-")) // 2
        accepted_key = "k" * max_key_chars
        rejected_key = accepted_key + "k"
        assert len(_physical_name(accepted_key)) == MAX_REGISTRY_VALUE_NAME_LENGTH
        await store.put(accepted_key, {"boundary": "value-name"}, collection="value-boundary")
        assert await store.get(accepted_key, collection="value-boundary") == {"boundary": "value-name"}
        await _expect_invalid_key(
            store.put(rejected_key, {"boundary": "rejected"}, collection="value-boundary"),
            InvalidKeyError,
        )

        fixed_path = f"{hive_name}\\{registry_path}\\okv1-"
        remaining_units = MAX_REGISTRY_KEY_PATH_LENGTH - _utf16_units(fixed_path)
        max_collection_chars = remaining_units // 2
        accepted_collection = "c" * max_collection_chars
        rejected_collection = accepted_collection + "c"
        accepted_absolute_path = f"{hive_name}\\{registry_path}\\{_physical_name(accepted_collection)}"
        rejected_absolute_path = f"{hive_name}\\{registry_path}\\{_physical_name(rejected_collection)}"
        assert _utf16_units(accepted_absolute_path) <= MAX_REGISTRY_KEY_PATH_LENGTH
        assert _utf16_units(rejected_absolute_path) > MAX_REGISTRY_KEY_PATH_LENGTH
        await store.put("boundary", {"boundary": "collection-path"}, collection=accepted_collection)
        assert await store.get("boundary", collection=accepted_collection) == {"boundary": "collection-path"}
        await _expect_invalid_key(
            store.put("boundary", {"boundary": "rejected"}, collection=rejected_collection),
            InvalidKeyError,
        )

        batch_collection = "batch-preflight"
        await store.setup_collection(collection=batch_collection)
        await _expect_invalid_key(
            store.put_many(
                ["sentinel", rejected_key],
                [{"written": False}, {"invalid": True}],
                collection=batch_collection,
            ),
            InvalidKeyError,
        )
        assert await store.get("sentinel", collection=batch_collection) is None

        elapsed = time.monotonic() - started_at
        print(
            "Windows Registry acceptance passed: "
            "REG_BINARY, native u64, exact identity, legacy non-reading, malformed rejection, "
            "batch ordering/prevalidation, TTL, 255-character path, 16,383-character value name, "
            f"and setup idempotence ({elapsed:.2f}s)."
        )
    finally:
        _delete_registry_tree(winreg, winreg.HKEY_CURRENT_USER, registry_path)
        with suppress(OSError):
            winreg.DeleteKey(winreg.HKEY_CURRENT_USER, parent_path)


def main() -> int:
    if sys.platform != "win32":
        print("This acceptance runner requires a native Windows host and the real winreg module.", file=sys.stderr)
        return 2
    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
