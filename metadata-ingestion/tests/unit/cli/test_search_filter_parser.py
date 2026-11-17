"""Tests for the filter-string parser that backs ``datahub search --where``.

The broad grammar suite lives in
``datahub-agent-context/tests/unit/mcp_tools/test_search_filter_parser.py``,
which exercises the same parser through the MCP re-export. This file covers the
lenient-value handling directly against the canonical module.
"""

import pytest

from datahub.cli.search_filter_parser import parse_filter_string
from datahub.sdk.search_filters import FilterDsl as F

# ---------------------------------------------------------------------------
# Unquoted multi-word values are joined rather than rejected
# ---------------------------------------------------------------------------


def test_unquoted_multi_word_value_is_joined():
    assert parse_filter_string("subtype = Fact Sheet") == F.entity_subtype("Fact Sheet")
    assert parse_filter_string("subtype = Vendor Q&A") == F.entity_subtype("Vendor Q&A")


def test_unquoted_multi_word_values_in_in_list_are_joined_per_member():
    assert parse_filter_string(
        "subtype IN (Fact Sheet, Semantic Anchor)"
    ) == F.entity_subtype(["Fact Sheet", "Semantic Anchor"])
    assert parse_filter_string(
        "subtype IN (Known Issues / Limitations, Fact Sheet)"
    ) == F.entity_subtype(["Known Issues / Limitations", "Fact Sheet"])


def test_keywords_are_absorbed_into_in_list_values():
    # No boolean logic exists between the parens of an IN list, so AND/OR/NOT are
    # ordinary words there and belong to the value.
    assert parse_filter_string(
        "subtype IN (Terms AND Conditions, Fact Sheet)"
    ) == F.entity_subtype(["Terms AND Conditions", "Fact Sheet"])


def test_joining_stops_at_conjunctions():
    # Outside an IN list the same keywords are clause boundaries.
    assert parse_filter_string(
        "subtype = Product Documentation OR subtype = How-To Guides"
    ) == F.or_(
        F.entity_subtype("Product Documentation"), F.entity_subtype("How-To Guides")
    )
    assert parse_filter_string("subtype = Runbook AND platform = notion") == F.and_(
        F.entity_subtype("Runbook"), F.platform("notion")
    )


def test_joining_collapses_runs_of_whitespace():
    assert parse_filter_string("subtype =  Fact   Sheet ") == F.entity_subtype(
        "Fact Sheet"
    )


def test_unquoted_multi_word_urn_is_joined():
    assert parse_filter_string("tag = urn:li:tag:my special tag") == F.tag(
        "urn:li:tag:my special tag"
    )


def test_comma_still_delimits_unless_quoted():
    assert parse_filter_string("subtype IN (Redwood, CA)") == F.entity_subtype(
        ["Redwood", "CA"]
    )
    assert parse_filter_string('subtype IN ("Redwood, CA")') == F.entity_subtype(
        "Redwood, CA"
    )


def test_not_equals_and_comparisons_use_the_same_value_rule():
    assert parse_filter_string("subtype != Fact Sheet") == F.not_(
        F.entity_subtype("Fact Sheet")
    )
    assert parse_filter_string("columnCount > 5") == F.custom_filter(
        "columnCount", "GREATER_THAN", ["5"]
    )


def test_joining_does_not_mask_a_missing_conjunction():
    with pytest.raises(ValueError, match="Unexpected token"):
        parse_filter_string("subtype = Fact Sheet platform = notion")


# ---------------------------------------------------------------------------
# Over-escaped quoting (\"value\") is read as ordinary quoting
# ---------------------------------------------------------------------------


def test_over_escaped_quotes_in_in_list():
    # Quotes escaped a second time while building JSON arrive with literal
    # backslashes. This used to fail in the tokenizer with "Unterminated string".
    assert parse_filter_string(
        r"subtype IN (\"Known Issues / Limitations\", Insight, \"Vendor Q&A\")"
    ) == F.entity_subtype(["Known Issues / Limitations", "Insight", "Vendor Q&A"])


def test_over_escaped_quotes_single_value():
    assert parse_filter_string(r"subtype = \"Fact Sheet\"") == F.entity_subtype(
        "Fact Sheet"
    )
    assert parse_filter_string(r"subtype = \'Fact Sheet\'") == F.entity_subtype(
        "Fact Sheet"
    )


def test_over_escaped_opening_with_bare_closing_quote():
    assert parse_filter_string(r'subtype = \"Fact Sheet"') == F.entity_subtype(
        "Fact Sheet"
    )


def test_over_escaped_unterminated_still_raises():
    with pytest.raises(ValueError, match="Unterminated string"):
        parse_filter_string(r"subtype = \"Fact Sheet")


def test_escapes_inside_a_normally_quoted_string_keep_their_meaning():
    assert parse_filter_string(r'custom_field = "snow\"flake"') == F.custom_filter(
        "custom_field", "EQUAL", ['snow"flake']
    )
    assert parse_filter_string('platform = "snowflake"') == F.platform("snowflake")


def test_unterminated_plain_string_still_raises():
    with pytest.raises(ValueError, match="Unterminated string"):
        parse_filter_string('platform = "snowflake')
