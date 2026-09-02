"""Tests for utils/version_util.py"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from release_variables import (
    DEFAULT_BRANCH,
    SLACK_RELEASE_CHANNEL_PREFIX,
    VERSION_SUFFIX,
)
from utils import version_util as vu

# Representative tags that must always be ignored: a custom suffix and a
# 4-component version (x.x.x.x plus VERSION_SUFFIX).
CUSTOM_SUFFIX_TAG = "v0.3.18.1-cx42-06"
FOUR_COMPONENT_TAG = f"v0.3.18.1{VERSION_SUFFIX}"


def _official(version: str) -> str:
    return f"v{version}{VERSION_SUFFIX}"


def _rc(version: str, n: int) -> str:
    return f"v{version}rc{n}{VERSION_SUFFIX}"


def _channel(major: int, minor: int) -> str:
    return f"{SLACK_RELEASE_CHANNEL_PREFIX}{major}_{minor}_0"


def test_classification_rc_official_custom():
    assert vu.is_rc(_rc("1.1.0", 7))
    assert not vu.is_official(_rc("1.1.0", 7))

    assert vu.is_official(_official("1.0.0"))
    assert vu.is_official(_official("1.0.2"))
    assert not vu.is_rc(_official("1.0.0"))

    # Custom suffix and 4-component versions are neither RC nor official.
    for tag in (CUSTOM_SUFFIX_TAG, FOUR_COMPONENT_TAG):
        assert vu.is_custom(tag)
        assert not vu.is_rc(tag)
        assert not vu.is_official(tag)


def test_parse_official_three_components_only():
    assert vu.parse_official(_official("1.2.3")) == vu.Version(1, 2, 3)
    # 4-component and custom-suffix tags do not parse.
    assert vu.parse_official(FOUR_COMPONENT_TAG) is None
    assert vu.parse_official(CUSTOM_SUFFIX_TAG) is None
    assert vu.parse_official(_rc("1.1.0", 7)) is None


def test_parse_version_str():
    assert vu.parse_version_str("1.0.0") == vu.Version(1, 0, 0)
    assert vu.parse_version_str("12.3.45") == vu.Version(12, 3, 45)
    # Rejects a 'v' prefix, suffixes, and wrong component counts.
    assert vu.parse_version_str("v1.0.0") is None
    if VERSION_SUFFIX:
        assert vu.parse_version_str(f"1.0.0{VERSION_SUFFIX}") is None
    assert vu.parse_version_str("1.0") is None
    assert vu.parse_version_str("1.0.0.1") is None


def test_parse_rc():
    assert vu.parse_rc(_rc("1.1.0", 7)) == vu.RcVersion(vu.Version(1, 1, 0), 7)
    assert vu.parse_rc(_official("1.1.0")) is None
    assert vu.parse_rc(FOUR_COMPONENT_TAG) is None


def test_official_tags_sorted_skips_custom_and_rc():
    tags = [
        _official("1.0.0"),
        CUSTOM_SUFFIX_TAG,
        FOUR_COMPONENT_TAG,
        _official("1.0.2"),
        _rc("1.1.0", 1),  # RC, excluded
        _official("1.0.1"),
        "garbage",
    ]
    assert vu.official_tags_sorted(tags) == [
        _official("1.0.0"),
        _official("1.0.1"),
        _official("1.0.2"),
    ]


def test_last_official_before_orders_by_version():
    tags = [_official("1.0.0"), _official("1.0.1"), _official("1.1.0"), FOUR_COMPONENT_TAG]
    assert vu.last_official_before(tags, vu.Version(1, 1, 0)) == _official("1.0.1")
    assert vu.last_official_before(tags, vu.Version(1, 0, 1)) == _official("1.0.0")


def test_last_official_before_returns_none_when_no_predecessor():
    tags = [_official("2.0.0"), CUSTOM_SUFFIX_TAG]
    assert vu.last_official_before(tags, vu.Version(1, 0, 0)) is None


def test_skipped_tags_never_selected_even_if_highest_lexical():
    # These would sort high lexically but must be ignored entirely.
    tags = [_official("1.0.0"), CUSTOM_SUFFIX_TAG, FOUR_COMPONENT_TAG]
    assert vu.last_official_before(tags, vu.Version(9, 9, 9)) == _official("1.0.0")


def test_release_channel_from_various_forms():
    # Bare version, official tag, and RC tag all resolve to the line channel.
    assert vu.release_channel("1.1.0") == _channel(1, 1)
    assert vu.release_channel("v1.1.0") == _channel(1, 1)
    assert vu.release_channel(_official("1.1.3")) == _channel(1, 1)
    assert vu.release_channel(_rc("1.1.0", 7)) == _channel(1, 1)
    assert vu.release_channel(_official("2.0.5")) == _channel(2, 0)


def test_release_channel_returns_none_for_unparseable():
    assert vu.release_channel(CUSTOM_SUFFIX_TAG) is None
    assert vu.release_channel(FOUR_COMPONENT_TAG) is None
    assert vu.release_channel("garbage") is None


def test_release_channel_from_ref_strips_branch_prefixes():
    # Release/hotfix branches map to their line's channel; patches share the line.
    assert vu.release_channel_from_ref("releases/v2.0.0") == _channel(2, 0)
    assert vu.release_channel_from_ref("hotfixes/v2.1.3") == _channel(2, 1)
    # Release tags and bare versions pass straight through.
    assert vu.release_channel_from_ref(_official("2.0.0")) == _channel(2, 0)
    # Non-release refs (e.g. the default branch) have no release channel.
    assert vu.release_channel_from_ref(DEFAULT_BRANCH) is None
