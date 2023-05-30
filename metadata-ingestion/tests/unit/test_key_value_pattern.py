from datahub.configuration.common import KeyValuePattern


def test_empty_pattern() -> None:
    pattern = KeyValuePattern.all()
    assert pattern.value("foo") == []


def test_basic_pattern() -> None:
    pattern = KeyValuePattern(rules={"foo": ["bar", "baz"]})
    assert pattern.value("foo") == ["bar", "baz"]
    assert pattern.value("bar") == []


def test_regex_pattern() -> None:
    pattern = KeyValuePattern(rules={"foo.*": ["bar", "baz"]})
    assert pattern.value("foo") == ["bar", "baz"]
    assert pattern.value("foo.bar") == ["bar", "baz"]
    assert pattern.value("bar") == []


def test_no_fallthrough_pattern() -> None:
    pattern = KeyValuePattern(rules={"foo.*": ["bar", "baz"], ".*": ["qux"]})
    assert pattern.value("foo") == ["bar", "baz"]
    assert pattern.value("foo.bar") == ["bar", "baz"]
    assert pattern.value("bar") == ["qux"]


def test_fallthrough_pattern() -> None:
    pattern = KeyValuePattern(
        rules={"foo.*": ["bar", "baz"], ".*": ["qux"]}, first_match_only=False
    )
    assert pattern.value("foo") == ["bar", "baz", "qux"]
    assert pattern.value("foo.bar") == ["bar", "baz", "qux"]
    assert pattern.value("bar") == ["qux"]


def test_fullmatch_pattern() -> None:
    pattern = KeyValuePattern(rules={"^foo$": ["bar", "baz"]})
    assert pattern.value("foo") == ["bar", "baz"]
    assert pattern.value("foo.bar") == []


def test_fullmatch_mix_pattern() -> None:
    pattern = KeyValuePattern(
        rules={
            "^aggregate.player_segment$": ["urn:li:tag:Player360"],
            "table_a": ["urn:li:tag:tag_a"],
            ".*marketing.*": ["urn:li:tag:marketing"],
        },
        first_match_only=False,
    )
    assert pattern.value("aggregate.player_segment") == ["urn:li:tag:Player360"]
    assert pattern.value("aggregate.player_segment_2") == []
    assert pattern.value("marketing.table_a") == [
        # The matches implicitly have a ^ at the beginning, so table_a is not a match.
        "urn:li:tag:marketing",
    ]
    assert pattern.value("table_a.marketing") == [
        "urn:li:tag:tag_a",
        "urn:li:tag:marketing",
    ]
