from typing import Dict, List, Optional
from unittest.mock import patch

import pytest
from pydantic import BaseModel, ValidationError

from acryl_datahub_cloud.graphql_utils import parse_extra_properties_for_model


# Test models
class SimpleModel(BaseModel):
    urn: str  # Required field
    name: str = ""  # Optional with default
    tags: List[str] = []  # Optional list


class ComplexModel(BaseModel):
    id: str
    owners: List[str] = []
    metadata: Optional[str] = None
    count: int = 0


def test_parse_json_encoded_strings():
    """Test parsing JSON-encoded string values."""
    extra_properties = [
        {"name": "urn", "value": '"urn:li:dataset:123"'},
        {"name": "name", "value": '"My Dataset"'},
    ]

    result = parse_extra_properties_for_model(extra_properties, SimpleModel)

    assert result == {
        "urn": "urn:li:dataset:123",
        "name": "My Dataset",
    }


def test_parse_json_encoded_lists():
    """Test parsing JSON-encoded list values."""
    extra_properties = [
        {"name": "urn", "value": '"urn:li:dataset:123"'},
        {"name": "tags", "value": '["tag1", "tag2", "tag3"]'},
    ]

    result = parse_extra_properties_for_model(extra_properties, SimpleModel)

    assert result == {
        "urn": "urn:li:dataset:123",
        "tags": ["tag1", "tag2", "tag3"],
    }


def test_parse_empty_lists():
    """Test parsing empty JSON arrays."""
    extra_properties = [
        {"name": "urn", "value": '"urn:li:dataset:123"'},
        {"name": "tags", "value": "[]"},
    ]

    result = parse_extra_properties_for_model(extra_properties, SimpleModel)

    assert result == {
        "urn": "urn:li:dataset:123",
        "tags": [],
    }


def test_filter_fields_not_in_model():
    """Test that fields not defined in the model are skipped."""
    extra_properties = [
        {"name": "urn", "value": '"urn:li:dataset:123"'},
        {"name": "name", "value": '"My Dataset"'},
        # scrollId is not in SimpleModel - should be filtered out
        {"name": "scrollId", "value": "eyJzb3J0IjpbMS4wXX0="},
        # extraField is not in SimpleModel - should be filtered out
        {"name": "extraField", "value": '"should be ignored"'},
    ]

    result = parse_extra_properties_for_model(extra_properties, SimpleModel)

    # Should only contain fields that are in the model
    assert result == {
        "urn": "urn:li:dataset:123",
        "name": "My Dataset",
    }
    assert "scrollId" not in result
    assert "extraField" not in result


def test_malformed_json_skips_field_with_warning():
    """Test that malformed JSON is handled gracefully with logging."""
    extra_properties = [
        {"name": "id", "value": '"valid-id"'},
        {"name": "owners", "value": "this-is-not-json"},  # Malformed JSON
        {"name": "count", "value": "42"},  # Valid JSON number
    ]

    with patch("acryl_datahub_cloud.graphql_utils.logger") as mock_logger:
        result = parse_extra_properties_for_model(extra_properties, ComplexModel)

        # Should have logged a warning
        assert mock_logger.warning.call_count == 1
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "Failed to parse field 'owners' as JSON" in warning_msg
        assert "this-is-not-json" in warning_msg

    # Result should contain valid fields, malformed field skipped
    assert result == {
        "id": "valid-id",
        "count": 42,
    }
    # owners should not be in result (will use default [])
    assert "owners" not in result


def test_model_instantiation_after_parsing():
    """Test that parsed values can be used to instantiate the model."""
    extra_properties = [
        {"name": "urn", "value": '"urn:li:dataset:123"'},
        {"name": "name", "value": '"Test Dataset"'},
        {"name": "tags", "value": '["alpha", "beta"]'},
    ]

    parsed = parse_extra_properties_for_model(extra_properties, SimpleModel)
    model = SimpleModel(**parsed)

    assert model.urn == "urn:li:dataset:123"
    assert model.name == "Test Dataset"
    assert model.tags == ["alpha", "beta"]


def test_missing_required_field_causes_validation_error():
    """Test that missing required fields cause Pydantic validation errors."""
    extra_properties = [
        # Missing 'urn' which is required
        {"name": "name", "value": '"Test Dataset"'},
    ]

    parsed = parse_extra_properties_for_model(extra_properties, SimpleModel)

    # Should be able to parse, but model instantiation should fail
    with pytest.raises(ValidationError) as exc_info:
        SimpleModel(**parsed)

    # Check that it's complaining about the missing 'urn' field
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("urn",)
    assert errors[0]["type"] == "missing"


def test_optional_fields_use_defaults():
    """Test that optional fields use default values when not provided."""
    extra_properties = [
        {"name": "urn", "value": '"urn:li:dataset:123"'},
        # name and tags not provided - should use defaults
    ]

    parsed = parse_extra_properties_for_model(extra_properties, SimpleModel)
    model = SimpleModel(**parsed)

    assert model.urn == "urn:li:dataset:123"
    assert model.name == ""  # Default value
    assert model.tags == []  # Default value


def test_empty_extra_properties():
    """Test handling of empty extra properties list."""
    extra_properties: List[Dict[str, str]] = []

    result = parse_extra_properties_for_model(extra_properties, SimpleModel)

    assert result == {}


def test_complex_nested_json():
    """Test parsing more complex nested JSON structures."""
    extra_properties = [
        {"name": "id", "value": '"complex-id"'},
        {
            "name": "owners",
            "value": '["urn:li:corpuser:user1", "urn:li:corpGroup:group1"]',
        },
        {"name": "metadata", "value": '"{\\"key\\": \\"value\\"}"'},  # Escaped JSON
        {"name": "count", "value": "999"},
    ]

    result = parse_extra_properties_for_model(extra_properties, ComplexModel)

    assert result == {
        "id": "complex-id",
        "owners": ["urn:li:corpuser:user1", "urn:li:corpGroup:group1"],
        "metadata": '{"key": "value"}',  # Double-encoded string
        "count": 999,
    }


def test_null_values():
    """Test parsing JSON null values."""
    extra_properties = [
        {"name": "id", "value": '"test-id"'},
        {"name": "metadata", "value": "null"},  # JSON null
    ]

    result = parse_extra_properties_for_model(extra_properties, ComplexModel)

    assert result == {
        "id": "test-id",
        "metadata": None,
    }


def test_multiple_malformed_fields():
    """Test handling multiple malformed fields with multiple warnings."""
    extra_properties = [
        {"name": "id", "value": '"valid-id"'},
        {"name": "owners", "value": "malformed1"},
        {"name": "metadata", "value": "malformed2"},
        {"name": "count", "value": "123"},
    ]

    with patch("acryl_datahub_cloud.graphql_utils.logger") as mock_logger:
        result = parse_extra_properties_for_model(extra_properties, ComplexModel)

        # Should have logged two warnings
        assert mock_logger.warning.call_count == 2

    assert result == {
        "id": "valid-id",
        "count": 123,
    }


def test_real_world_scrollid_scenario():
    """Test the real-world scenario that caused the bug: scrollId field."""
    # This simulates the actual GraphQL response that was causing failures
    extra_properties = [
        {"name": "urn", "value": '"urn:li:dataset:(urn:li:dataPlatform:bigquery,...)"'},
        {"name": "owners", "value": '["urn:li:corpuser:anna.everhart@acryl.io"]'},
        {"name": "tags", "value": "[]"},
        # scrollId is NOT in the model and is NOT JSON-encoded (base64 string)
        {
            "name": "scrollId",
            "value": "eyJzb3J0IjpbMS45NSwidXJuOmxpOmRhdGFzZXQ6KHVybjpsaTpkYXRhUGxhdGZvcm06YmlncXVlcnksYWNyeWwtc3RhZ2luZy0yLnNtb2tlX3Rlc3RfZGJfMy5jbG91ZGF1ZGl0X2dvb2dsZWFwaXNfY29tX2RhdGFfYWNjZXNzLFBST0QpIiwxLjk1LCJ1cm46bGk6ZGF0YXNldDoodXJuOmxpOmRhdGFQbGF0Zm9ybTpiaWdxdWVyeSxhY3J5bC1zdGFnaW5nLTIuc21va2VfdGVzdF9kYl8zLmNsb3VkYXVkaXRfZ29vZ2xlYXBpc19jb21fZGF0YV9hY2Nlc3MsUFJPRCkiXSwicGl0SWQiOm51bGwsImV4cGlyYXRpb25UaW1lIjowfQ==",
        },
    ]

    # This should NOT crash, even though scrollId is not JSON-encoded
    result = parse_extra_properties_for_model(extra_properties, SimpleModel)
    model = SimpleModel(**result)

    # scrollId should be completely ignored
    assert "scrollId" not in result
    assert model.urn == "urn:li:dataset:(urn:li:dataPlatform:bigquery,...)"
    assert model.tags == []
