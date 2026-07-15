import pytest

from openkeyv._utils.compound import (
    collection_prefix,
    compound_key,
    get_collections_from_compound_keys,
    get_keys_from_compound_keys,
    uncompound_key,
)


def test_canonical_compound_identity_roundtrips_exact_strings() -> None:
    values = [
        ("", ""),
        ("default", "key"),
        ("a:b", "c/d"),
        ("集合", "键🔑"),
        ("*?[\\]", ":/::"),
    ]

    for collection, key in values:
        identity = compound_key(collection=collection, key=key)
        assert uncompound_key(identity) == (collection, key)


def test_canonical_compound_identity_uses_utf8_byte_length() -> None:
    assert collection_prefix("集合") == "6:集合"
    assert compound_key("集合", "key") == "6:集合key"


def test_canonical_compound_identity_distinguishes_collision_pairs() -> None:
    left = compound_key("a:b", "c")
    right = compound_key("a", "b:c")

    assert left != right
    assert uncompound_key(left) == ("a:b", "c")
    assert uncompound_key(right) == ("a", "b:c")


def test_canonical_compound_identity_preserves_case_and_normalization() -> None:
    assert compound_key("Users", "Key") != compound_key("users", "Key")
    assert compound_key("é", "key") != compound_key("e\u0301", "key")


@pytest.mark.parametrize(
    "identity",
    [
        "",
        ":key",
        "x:key",
        "01:akey",
        "2:a",
        "1:ékey",
        "999999999999999999999999999999999999999999999999999999999999999999999999999999:key",
    ],
)
def test_malformed_canonical_compound_identity_is_rejected(identity: str) -> None:
    with pytest.raises(TypeError):
        uncompound_key(identity)


def test_compound_collection_filter_strictly_decodes_all_keys() -> None:
    identities = [compound_key("items", "one"), compound_key("other", "two"), compound_key("items", "three")]

    assert get_keys_from_compound_keys(identities, collection="items") == ["one", "three"]
    assert set(get_collections_from_compound_keys(identities)) == {"items", "other"}

    with pytest.raises(TypeError):
        get_keys_from_compound_keys([identities[0], "malformed"], collection="items")
