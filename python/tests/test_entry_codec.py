import pytest

from openkeyv._internal import _decode_entry, _encode_entry


def test_entry_codec_roundtrips_nested_values_and_timestamps() -> None:
    value = {
        "bytes": b"payload",
        "list": ["text", 42, 1.5, True, None],
        "dict": {"nested": "value"},
    }

    encoded = _encode_entry(value, 1_700_000_000_000, 1_700_000_010_000)

    assert _decode_entry(encoded) == (value, 1_700_000_000_000, 1_700_000_010_000)


def test_entry_codec_is_deterministic_for_dict_order() -> None:
    left = _encode_entry({"b": 2, "a": 1}, None, None)
    right = _encode_entry({"a": 1, "b": 2}, None, None)

    assert left == right


@pytest.mark.parametrize("encoded", [b"", b"{}", b"OKVE1", b"not-openkeyv"])
def test_entry_codec_rejects_malformed_and_json_data(encoded: bytes) -> None:
    with pytest.raises(ValueError, match="deserialization failed"):
        _decode_entry(encoded)


@pytest.mark.parametrize("value", [{1: "bad"}, {"nested": object()}, 2**63])
def test_entry_codec_rejects_values_outside_the_boundary(value: object) -> None:
    with pytest.raises(TypeError):
        _encode_entry(value, None, None)
