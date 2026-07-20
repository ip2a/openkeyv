import sys
from types import ModuleType

import pytest

from openkeyv._internal import _encode_entry
from openkeyv.errors import InvalidKeyError


class FakeRegistryKey:
    def __init__(self, hive: object, path: str) -> None:
        self.hive = hive
        self.path = path

    def __enter__(self) -> "FakeRegistryKey":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeWinreg(ModuleType):
    HKEY_CURRENT_USER = object()
    HKEY_LOCAL_MACHINE = object()
    REG_BINARY = 3
    KEY_SET_VALUE = 2

    def __init__(self) -> None:
        super().__init__("winreg")
        self.values: dict[tuple[object, str], dict[str, tuple[object, int]]] = {}
        self.query_error: Exception | None = None
        self.request_count = 0

    def CreateKey(self, hive: object, path: str) -> FakeRegistryKey:  # noqa: N802
        self.request_count += 1
        self.values.setdefault((hive, path), {})
        return FakeRegistryKey(hive, path)

    def OpenKey(self, hive: object, path: str, *, access: int | None = None) -> FakeRegistryKey:  # noqa: N802
        self.request_count += 1
        assert access in {None, self.KEY_SET_VALUE}
        if (hive, path) not in self.values:
            raise FileNotFoundError(path)
        return FakeRegistryKey(hive, path)

    def QueryValueEx(self, key: FakeRegistryKey, name: str) -> tuple[object, int]:  # noqa: N802
        self.request_count += 1
        if self.query_error is not None:
            raise self.query_error
        try:
            return self.values[(key.hive, key.path)][name]
        except KeyError as error:
            raise FileNotFoundError(name) from error

    def SetValueEx(self, key: FakeRegistryKey, name: str, reserved: int, value_type: int, value: object) -> None:  # noqa: N802
        self.request_count += 1
        assert reserved == 0
        self.values[(key.hive, key.path)][name] = (value, value_type)

    def DeleteValue(self, key: FakeRegistryKey, name: str) -> None:  # noqa: N802
        self.request_count += 1
        try:
            del self.values[(key.hive, key.path)][name]
        except KeyError as error:
            raise FileNotFoundError(name) from error


fake_winreg = FakeWinreg()
sys.modules["winreg"] = fake_winreg

from openkeyv.stores.windows_registry import WindowsRegistryStore  # noqa: E402
from openkeyv.stores.windows_registry.store import (  # noqa: E402
    MAX_REGISTRY_KEY_PATH_LENGTH,
    MAX_REGISTRY_VALUE_NAME_LENGTH,
    REGISTRY_ENCODING_PREFIX,
)


@pytest.fixture(autouse=True)
def reset_fake_registry() -> None:
    fake_winreg.values.clear()
    fake_winreg.query_error = None
    fake_winreg.request_count = 0


def physical_name(value: str) -> str:
    return REGISTRY_ENCODING_PREFIX + value.encode("utf-8").hex()


def physical_path(*, registry_path: str, collection: str) -> str:
    return f"{registry_path}\\{physical_name(collection)}"


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_roundtrip_uses_binary_entry_codec() -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")

    await store.put("key", {"nested": {"value": 1}}, collection="items")

    path = physical_path(registry_path="Software\\tests", collection="items")
    encoded, value_type = fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, path)][physical_name("key")]
    assert value_type == fake_winreg.REG_BINARY
    assert isinstance(encoded, bytes)
    assert encoded.startswith(b"OKVE1")
    assert await store.get("key", collection="items") == {"nested": {"value": 1}}
    assert await store.ttl("key", collection="items") == ({"nested": {"value": 1}}, None)
    assert await store.delete("key", collection="items") is True
    assert await store.delete("key", collection="items") is False


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_does_not_read_legacy_raw_names() -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    raw_path = "Software\\tests\\items"
    fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, raw_path)] = {
        "key": (_encode_entry({"legacy": True}, None, None), fake_winreg.REG_BINARY)
    }

    assert await store.get("key", collection="items") is None
    raw_value = fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, raw_path)]["key"][0]
    assert isinstance(raw_value, bytes)
    assert raw_value.startswith(b"OKVE1")


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_identity_preserves_exact_logical_names() -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    cases = [
        ("", "", {"case": "empty"}),
        ("Users", "Key", {"case": "upper"}),
        ("users", "key", {"case": "lower"}),
        ("é", "e\u0301", {"case": "composed"}),
        ("e\u0301", "é", {"case": "decomposed"}),
        ("nul\x00path\\control\x01", "key/with?glob*", {"case": "special"}),
        ("a:", "b", {"case": "framing-left"}),
        ("a", ":b", {"case": "framing-right"}),
    ]

    for collection, key, value in cases:
        await store.put(key, value, collection=collection)

    for collection, key, value in cases:
        assert await store.get(key, collection=collection) == value

    physical_collections = {path.rsplit("\\", maxsplit=1)[-1] for hive, path in fake_winreg.values if hive is fake_winreg.HKEY_CURRENT_USER}
    assert physical_collections == {physical_name(collection) for collection, _, _ in cases}
    assert physical_name("Users") != physical_name("users")
    assert physical_name("é") != physical_name("e\u0301")
    assert physical_name("a:") != physical_name("a")
    assert physical_name("b") != physical_name(":b")


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
@pytest.mark.parametrize(
    ("raw_value", "error", "message"),
    [
        ((b"OKVE1", 1), ValueError, "must use REG_BINARY"),
        (("not-bytes", FakeWinreg.REG_BINARY), TypeError, "must contain bytes"),
    ],
)
async def test_windows_registry_rejects_malformed_values(raw_value: tuple[object, int], error: type[Exception], message: str) -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    await store.setup_collection(collection="items")
    path = physical_path(registry_path="Software\\tests", collection="items")
    fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, path)][physical_name("key")] = raw_value

    with pytest.raises(error, match=message):
        await store._get_managed_entry(key="key", collection="items")


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_only_maps_file_not_found_to_missing() -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    await store.setup_collection(collection="items")

    assert await store._get_managed_entry(key="missing", collection="items") is None

    fake_winreg.query_error = PermissionError("denied")
    with pytest.raises(PermissionError, match="denied"):
        await store._get_managed_entry(key="key", collection="items")


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_collection_path_boundary_is_checked_before_access() -> None:
    registry_path = "S"
    absolute_prefix = f"HKEY_CURRENT_USER\\{registry_path}\\{REGISTRY_ENCODING_PREFIX}"
    accepted_bytes = (MAX_REGISTRY_KEY_PATH_LENGTH - len(absolute_prefix)) // 2
    accepted_collection = "a" * accepted_bytes
    rejected_collection = "a" * (accepted_bytes + 1)

    store = WindowsRegistryStore(registry_path=registry_path)
    await store.put("key", {"ok": True}, collection=accepted_collection)
    assert fake_winreg.request_count > 0

    fake_winreg.request_count = 0
    with pytest.raises(InvalidKeyError, match="maximum key path length"):
        await store.put("key", {"ok": False}, collection=rejected_collection)
    assert fake_winreg.request_count == 0


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_value_name_boundary_is_checked_before_access() -> None:
    accepted_key = "a" * ((MAX_REGISTRY_VALUE_NAME_LENGTH - len(REGISTRY_ENCODING_PREFIX)) // 2)
    rejected_key = accepted_key + "a"
    store = WindowsRegistryStore(registry_path="Software\\tests")

    await store.put(accepted_key, {"ok": True}, collection="items")
    fake_winreg.request_count = 0

    with pytest.raises(InvalidKeyError, match="maximum value name length"):
        await store.put(rejected_key, {"ok": False}, collection="items")
    assert fake_winreg.request_count == 0


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_invalid_batch_has_no_registry_side_effect() -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    await store.put("existing", {"value": 1}, collection="items")
    path = physical_path(registry_path="Software\\tests", collection="items")
    before = dict(fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, path)])
    fake_winreg.request_count = 0
    invalid_key = "a" * (((MAX_REGISTRY_VALUE_NAME_LENGTH - len(REGISTRY_ENCODING_PREFIX)) // 2) + 1)

    with pytest.raises(InvalidKeyError):
        await store.put_many(
            ["new", invalid_key],
            [{"value": 2}, {"value": 3}],
            collection="items",
        )

    assert fake_winreg.request_count == 0
    assert fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, path)] == before


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
@pytest.mark.parametrize("operation", ["get_many", "ttl_many", "delete_many"])
async def test_windows_registry_invalid_batch_reads_and_deletes_have_no_registry_side_effect(operation: str) -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    invalid_key = "a" * (((MAX_REGISTRY_VALUE_NAME_LENGTH - len(REGISTRY_ENCODING_PREFIX)) // 2) + 1)
    fake_winreg.request_count = 0

    if operation == "get_many":
        operation_call = store.get_many(["valid", invalid_key], collection="items")
    elif operation == "ttl_many":
        operation_call = store.ttl_many(["valid", invalid_key], collection="items")
    else:
        operation_call = store.delete_many(["valid", invalid_key], collection="items")

    with pytest.raises(InvalidKeyError):
        await operation_call

    assert fake_winreg.request_count == 0


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
@pytest.mark.parametrize("operation", ["get", "ttl", "put", "delete"])
async def test_windows_registry_single_invalid_identity_has_no_registry_side_effect(operation: str) -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    invalid_key = "a" * (((MAX_REGISTRY_VALUE_NAME_LENGTH - len(REGISTRY_ENCODING_PREFIX)) // 2) + 1)
    fake_winreg.request_count = 0

    if operation == "put":
        operation_call = store.put(invalid_key, {"value": 1}, collection="items")
    elif operation == "get":
        operation_call = store.get(invalid_key, collection="items")
    elif operation == "ttl":
        operation_call = store.ttl(invalid_key, collection="items")
    else:
        operation_call = store.delete(invalid_key, collection="items")

    with pytest.raises(InvalidKeyError):
        await operation_call

    assert fake_winreg.request_count == 0


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
@pytest.mark.parametrize("value", [b"bytes", "text", 42, 2**63, 1.5, True, None, ["nested", 1]])
async def test_windows_registry_roundtrips_all_store_values(value: object) -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")

    await store.put("key", value, collection="items")  # type: ignore[arg-type]

    assert await store.get("key", collection="items") == value
