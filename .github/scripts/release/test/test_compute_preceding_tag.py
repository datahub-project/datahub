"""Tests for compute_preceding_tag.py"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import compute_preceding_tag as cpt
from release_variables import VERSION_SUFFIX

CUSTOM_SUFFIX_TAG = "v0.3.18.1-cx42-06"
FOUR_COMPONENT_TAG = f"v0.3.18.1{VERSION_SUFFIX}"


def _official(version: str) -> str:
    return f"v{version}{VERSION_SUFFIX}"


def _rc(version: str, n: int) -> str:
    return f"v{version}rc{n}{VERSION_SUFFIX}"


def test_rc_returns_previous_rc():
    tags = [_rc("1.1.0", 1), _rc("1.1.0", 2), _official("1.0.0")]
    assert cpt.compute_preceding_tag(_rc("1.1.0", 3), tags) == _rc("1.1.0", 2)


def test_rc_skips_deleted_previous_rc():
    # rc2 was deleted; rc3 should fall back to the highest existing prior RC (rc1).
    tags = [_rc("1.1.0", 1), _official("1.0.0")]
    assert cpt.compute_preceding_tag(_rc("1.1.0", 3), tags) == _rc("1.1.0", 1)


def test_rc1_falls_back_to_last_official():
    tags = [_official("1.0.0"), _official("1.0.1"), CUSTOM_SUFFIX_TAG, FOUR_COMPONENT_TAG]
    assert cpt.compute_preceding_tag(_rc("1.1.0", 1), tags) == _official("1.0.1")


def test_official_returns_last_official_before_it():
    tags = [_official("1.0.0"), _official("1.0.1"), _rc("1.1.0", 1), FOUR_COMPONENT_TAG]
    assert cpt.compute_preceding_tag(_official("1.1.0"), tags) == _official("1.0.1")


def test_first_ever_release_has_no_predecessor():
    assert cpt.compute_preceding_tag(_official("1.0.0"), [CUSTOM_SUFFIX_TAG]) is None


@pytest.mark.parametrize("bad_tag", [CUSTOM_SUFFIX_TAG, FOUR_COMPONENT_TAG])
def test_unrecognized_tag_being_cut_is_rejected(bad_tag):
    with pytest.raises(SystemExit):
        cpt.compute_preceding_tag(bad_tag, [_official("1.0.0")])
