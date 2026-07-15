from openkeyv._utils.sanitization import (
    AlwaysHashStrategy,
    HashExcessLengthStrategy,
    HashFragmentMode,
    HybridSanitizationStrategy,
    PassthroughStrategy,
)


def test_passthrough_strategy_preserves_exact_identity() -> None:
    strategy = PassthroughStrategy()
    identities = ["", "Users", "users", "é", "e\u0301", "a:b", "a/b", "\x00"]

    for identity in identities:
        strategy.validate(identity)
        assert strategy.sanitize(identity) == identity
        assert strategy.try_unsanitize(identity) == identity


def test_hashing_strategies_do_not_claim_reversibility() -> None:
    always_hash = AlwaysHashStrategy()
    excess_hash = HashExcessLengthStrategy(max_length=16)

    assert always_hash.try_unsanitize(always_hash.sanitize("logical-key")) is None
    assert excess_hash.try_unsanitize(excess_hash.sanitize("logical-key-that-is-too-long")) is None


def test_explicit_lossy_strategy_can_merge_logical_names() -> None:
    strategy = HybridSanitizationStrategy(
        allowed_characters="abcdefghijklmnopqrstuvwxyz",
        replacement_character="_",
        hash_fragment_mode=HashFragmentMode.NEVER,
    )

    assert strategy.sanitize("a:b") == strategy.sanitize("a/b")
