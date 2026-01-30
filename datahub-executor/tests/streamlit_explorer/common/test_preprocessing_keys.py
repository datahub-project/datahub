"""Tests for preprocessing key helpers."""

from scripts.streamlit_explorer.common.preprocessing_keys import (
    build_preprocessing_key,
    display_preprocessing_id,
    split_preprocessing_key,
)


def test_build_and_split_roundtrip_with_assertion_urn() -> None:
    short_id = "daily_no_anomalies"
    assertion_urn = "urn:li:assertion:my_monitor_id"

    key = build_preprocessing_key(short_id, assertion_urn)
    parsed_short, parsed_urn = split_preprocessing_key(key)

    assert parsed_short == short_id
    assert parsed_urn == assertion_urn


def test_build_returns_short_id_when_assertion_missing() -> None:
    assert build_preprocessing_key("x", None) == "x"
    assert build_preprocessing_key("x", "") == "x"


def test_split_legacy_key_returns_none_assertion() -> None:
    short, urn = split_preprocessing_key("legacy_id")
    assert short == "legacy_id"
    assert urn is None


def test_display_prefers_metadata_short_id() -> None:
    key = "some_id@@urn%3Ali%3Aassertion%3Aabc"
    assert (
        display_preprocessing_id(key, metadata={"preprocessing_short_id": "pretty"})
        == "pretty"
    )


def test_display_falls_back_to_parsed_short_id() -> None:
    key = "pretty@@urn%3Ali%3Aassertion%3Aabc"
    assert display_preprocessing_id(key, metadata=None) == "pretty"
