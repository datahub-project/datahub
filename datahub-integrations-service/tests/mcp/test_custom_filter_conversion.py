"""Test custom filter format conversion for MCP server."""

import pytest

from datahub_integrations.mcp.mcp_server import _convert_custom_filter_format


def test_convert_custom_filter_format_real_example() -> None:
    """Test the custom filter format conversion using real example from acryl-8 evaluation failure."""

    # Real failing filter from acryl-8 evaluation that caused validation error
    failing_filter = {
        "and": [
            {"entity_type": ["DOMAIN"]},
            {
                "custom": {
                    "field": "urn",
                    "condition": "EQUAL",
                    "values": ["urn:li:domain:rsNR3xWYmipUCFQnynPBm2"],
                }
            },
        ]
    }

    # Expected result after conversion
    expected_result = {
        "and": [
            {"entity_type": ["DOMAIN"]},
            {
                "field": "urn",
                "condition": "EQUAL",
                "values": ["urn:li:domain:rsNR3xWYmipUCFQnynPBm2"],
            },
        ]
    }

    # Test the conversion
    result = _convert_custom_filter_format(failing_filter)
    assert result == expected_result, f"Expected {expected_result}, got {result}"


def test_convert_custom_filter_simple_case() -> None:
    """Test simple custom filter conversion."""

    # Simple custom filter case
    input_filter = {
        "custom": {"field": "name", "condition": "CONTAIN", "values": ["test"]}
    }
    expected = {"field": "name", "condition": "CONTAIN", "values": ["test"]}

    result = _convert_custom_filter_format(input_filter)
    assert result == expected


def test_convert_custom_filter_no_conversion_needed() -> None:
    """Test that non-custom filters are left unchanged."""

    # Regular filter that shouldn't be changed
    regular_filter = {"entity_type": ["dataset"], "platform": ["snowflake"]}

    result = _convert_custom_filter_format(regular_filter)
    assert result == regular_filter


def test_convert_custom_filter_preserves_structure() -> None:
    """Test that the conversion preserves overall filter structure."""

    # Complex filter with custom nested inside AND
    complex_filter = {
        "and": [
            {"entity_type": ["dataset"]},
            {"platform": ["snowflake"]},
            {
                "custom": {
                    "field": "hasUsageStats",
                    "condition": "EQUAL",
                    "values": ["true"],
                }
            },
        ]
    }

    expected = {
        "and": [
            {"entity_type": ["dataset"]},
            {"platform": ["snowflake"]},
            {"field": "hasUsageStats", "condition": "EQUAL", "values": ["true"]},
        ]
    }

    result = _convert_custom_filter_format(complex_filter)
    assert result == expected


def test_convert_custom_filter_with_discriminated_union() -> None:
    """Test that converted filters can be loaded by the discriminated union validator."""
    from datahub.sdk.search_filters import load_filters

    # Real failing example that should now work
    failing_filter = {
        "and": [
            {"entity_type": ["DOMAIN"]},
            {
                "custom": {
                    "field": "urn",
                    "condition": "EQUAL",
                    "values": ["urn:li:domain:test"],
                }
            },
        ]
    }

    # Convert and validate it loads without error
    converted = _convert_custom_filter_format(failing_filter)

    try:
        loaded_filter = load_filters(converted)
        assert loaded_filter is not None
    except Exception as e:
        pytest.fail(f"Converted filter failed discriminated union validation: {e}")


def test_convert_custom_condition_wrapper() -> None:
    """Test that custom_condition wrapper is also converted (from acryl-6 failure case)."""

    # Real failing example from acryl-6 using "custom_condition" instead of "custom"
    custom_condition_filter = {
        "custom_condition": {
            "field": "owners",
            "condition": "EQUAL",
            "values": ["urn:li:corpuser:john.joyce@acryl.io"],
        }
    }

    expected = {
        "field": "owners",
        "condition": "EQUAL",
        "values": ["urn:li:corpuser:john.joyce@acryl.io"],
    }

    result = _convert_custom_filter_format(custom_condition_filter)
    assert result == expected


def test_convert_custom_filter_malformed() -> None:
    """Test that malformed custom filters are left unchanged."""

    # Malformed custom filter (missing required fields)
    malformed = {"custom": {"invalid": "structure"}}

    # Should be left unchanged since it doesn't have "field"
    result = _convert_custom_filter_format(malformed)
    assert result == malformed
