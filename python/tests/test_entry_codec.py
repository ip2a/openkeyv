import pytest

from openkeyv._internal import _decode_entry, _encode_entry


class _IntegerLike:
    def __index__(self) -> int:
        return 1


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


@pytest.mark.parametrize("value", [{1: "bad"}, {"nested": object()}, _IntegerLike()])
def test_entry_codec_rejects_values_outside_the_boundary(value: object) -> None:
    with pytest.raises(TypeError):
        _encode_entry(value, None, None)


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**64])
def test_entry_codec_rejects_integers_outside_the_supported_range(value: int) -> None:
    with pytest.raises(OverflowError):
        _encode_entry(value, None, None)


@pytest.mark.parametrize("value", [-(2**63), 0, 2**63 - 1, 2**63, 2**64 - 1])
def test_entry_codec_roundtrips_top_level_integer_boundaries(value: int) -> None:
    encoded = _encode_entry(value, None, None)

    assert _decode_entry(encoded) == (value, None, None)


def test_entry_codec_uses_signed_then_unsigned_integer_tags() -> None:
    assert _encode_entry(0, None, None)[5] == 2
    assert _encode_entry(2**63 - 1, None, None)[5] == 2
    assert _encode_entry(2**63, None, None)[5] == 7
    assert _encode_entry(2**64 - 1, None, None)[5] == 7


def test_entry_codec_roundtrips_nested_unsigned_integer_boundaries() -> None:
    value = {
        "list": [2**63, 2**64 - 1],
        "dict": {"signed": 2**63 - 1, "unsigned": 2**63},
    }

    encoded = _encode_entry(value, None, None)

    assert _decode_entry(encoded) == (value, None, None)


@pytest.mark.parametrize("value", [[-(2**63) - 1], {"overflow": 2**64}])
def test_entry_codec_rejects_nested_integers_outside_the_supported_range(value: object) -> None:
    with pytest.raises(OverflowError):
        _encode_entry(value, None, None)


def test_entry_codec_preserves_bool_before_integer_handling() -> None:
    top_level, _, _ = _decode_entry(_encode_entry(True, None, None))
    nested, _, _ = _decode_entry(_encode_entry([False], None, None))

    assert top_level is True
    assert isinstance(nested, list)
    assert nested[0] is False


def test_entry_codec_rejects_truncated_unsigned_integer_payload() -> None:
    encoded = bytearray(_encode_entry(2**63, None, None))
    encoded[7:15] = (7).to_bytes(8, "little")
    del encoded[-1]

    with pytest.raises(ValueError, match="invalid unsigned integer payload length"):
        _decode_entry(bytes(encoded))
