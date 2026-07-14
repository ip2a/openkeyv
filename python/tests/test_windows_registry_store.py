import sys
from types import ModuleType

import pytest

from openkeyv._internal import _encode_entry


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

    def CreateKey(self, hive: object, path: str) -> FakeRegistryKey:  # noqa: N802
        self.values.setdefault((hive, path), {})
        return FakeRegistryKey(hive, path)

    def OpenKey(self, hive: object, path: str, *, access: int | None = None) -> FakeRegistryKey:  # noqa: N802
        assert access in {None, self.KEY_SET_VALUE}
        if (hive, path) not in self.values:
            raise FileNotFoundError(path)
        return FakeRegistryKey(hive, path)

    def QueryValueEx(self, key: FakeRegistryKey, name: str) -> tuple[object, int]:  # noqa: N802
        if self.query_error is not None:
            raise self.query_error
        try:
            return self.values[(key.hive, key.path)][name]
        except KeyError as error:
            raise FileNotFoundError(name) from error

    def SetValueEx(self, key: FakeRegistryKey, name: str, reserved: int, value_type: int, value: object) -> None:  # noqa: N802
        assert reserved == 0
        self.values[(key.hive, key.path)][name] = (value, value_type)

    def DeleteValue(self, key: FakeRegistryKey, name: str) -> None:  # noqa: N802
        try:
            del self.values[(key.hive, key.path)][name]
        except KeyError as error:
            raise FileNotFoundError(name) from error


fake_winreg = FakeWinreg()
sys.modules["winreg"] = fake_winreg

from openkeyv.stores.windows_registry import WindowsRegistryStore  # noqa: E402


@pytest.fixture(autouse=True)
def reset_fake_registry() -> None:
    fake_winreg.values.clear()
    fake_winreg.query_error = None


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
async def test_windows_registry_roundtrip_uses_binary_entry_codec() -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")

    await store.put("key", {"nested": {"value": 1}}, collection="items")

    path = "Software\\tests\\items"
    encoded, value_type = fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, path)]["key"]
    assert value_type == fake_winreg.REG_BINARY
    assert isinstance(encoded, bytes)
    assert encoded.startswith(b"OKVE1")
    assert await store.get("key", collection="items") == {"nested": {"value": 1}}
    assert await store.ttl("key", collection="items") == ({"nested": {"value": 1}}, None)
    assert await store.delete("key", collection="items") is True
    assert await store.delete("key", collection="items") is False


@pytest.mark.filterwarnings("ignore:A configured store is unstable")
@pytest.mark.parametrize(
    ("raw_value", "error", "message"),
    [
        ((b"OKVE1", 1), ValueError, "must use REG_BINARY"),
        (("not-bytes", FakeWinreg.REG_BINARY), TypeError, "must contain bytes"),
        ((_encode_entry(["not-a-dict"], None, None), FakeWinreg.REG_BINARY), TypeError, "must be a dict"),
    ],
)
async def test_windows_registry_rejects_malformed_values(raw_value: tuple[object, int], error: type[Exception], message: str) -> None:
    store = WindowsRegistryStore(registry_path="Software\\tests")
    await store.setup_collection(collection="items")
    fake_winreg.values[(fake_winreg.HKEY_CURRENT_USER, "Software\\tests\\items")]["key"] = raw_value

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
