from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.links import make_url_for_urn

from datahub_integrations.mcp import mcp_server
from datahub_integrations.mcp.mcp_server import (
    clean_get_entity_response,
    clean_gql_response,
    inject_urls_for_urns,
    maybe_convert_to_schema_field_urn,
    truncate_descriptions,
    truncate_query,
)


def test_inject_urls_for_urns() -> None:
    mock_graph = Mock()
    mock_graph.url_for.side_effect = lambda urn: make_url_for_urn(
        "https://xyz.com", urn
    )

    with patch(
        "datahub_integrations.mcp.mcp_server._is_datahub_cloud", return_value=True
    ):
        response = {
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
                        "name": "users",
                    }
                },
                {
                    "entity": {
                        "urn": "urn:li:chart:(looker,baz)",
                        "name": "baz",
                    }
                },
            ]
        }

        inject_urls_for_urns(mock_graph, response, ["searchResults[].entity"])

        expected_response = {
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
                        "url": "https://xyz.com/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Canalytics_db.raw_schema.users%2CPROD%29/",
                        "name": "users",
                    }
                },
                {
                    "entity": {
                        "urn": "urn:li:chart:(looker,baz)",
                        "url": "https://xyz.com/chart/urn%3Ali%3Achart%3A%28looker%2Cbaz%29/",
                        "name": "baz",
                    }
                },
            ]
        }

        assert response == expected_response
        assert mock_graph.url_for.call_count == 2


def test_maybe_convert_to_schema_field_urn_with_column() -> None:
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)"
    column = "user_id"

    result = maybe_convert_to_schema_field_urn(dataset_urn, column)

    assert (
        result
        == "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD),user_id)"
    )


def test_maybe_convert_to_schema_field_urn_without_column() -> None:
    original_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)"

    result = maybe_convert_to_schema_field_urn(original_urn, None)

    assert result == original_urn


def test_maybe_convert_to_schema_field_urn_with_incorrect_entity() -> None:
    chart_urn = "urn:li:chart:(looker,baz)"

    # Ok if no column is provided
    result = maybe_convert_to_schema_field_urn(chart_urn, None)
    assert result == chart_urn

    # Fail if column is provided
    column = "user_id"
    with pytest.raises(ValueError):
        maybe_convert_to_schema_field_urn(chart_urn, column)


def test_clean_gql_response_with_dict() -> None:
    response: dict = {
        "__typename": "Dataset",
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
        "name": "users",
        "description": None,
        "tags": [],
        "properties": {"__typename": "Properties", "key1": "value1", "key2": None},
    }

    result = clean_gql_response(response)

    expected_result = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
        "name": "users",
        "properties": {"key1": "value1"},
    }

    assert result == expected_result


def test_clean_gql_response_with_nested_empty_objects() -> None:
    response = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
        "name": "users",
        "empty_object": {},
        "empty_array": [],
        "nested_object": {
            "empty_object": {},
            "empty_array": [],
            "valid_data": "value",
            "null_value": None,
        },
        "array_of_objects": [
            {"valid": "data", "empty_object": {}, "empty_array": [], "null_value": None}
        ],
    }

    result = clean_gql_response(response)

    expected_result = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
        "name": "users",
        "nested_object": {"valid_data": "value"},
        "array_of_objects": [{"valid": "data"}],
    }

    assert result == expected_result


def test_clean_get_entity_response_with_schema_metadata() -> None:
    raw_response = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
        "name": "users",
        "schemaMetadata": {
            "platformSchema": {
                "schema": ""  # Empty schema should be removed
            },
            "fields": [
                {
                    "fieldPath": "user_id",
                    "recursive": False,  # Should be removed
                    "isPartOfKey": False,  # Should be removed
                    "type": "STRING",
                },
                {
                    "fieldPath": "email",
                    "recursive": True,  # Should be kept
                    "isPartOfKey": True,  # Should be kept
                    "type": "STRING",
                },
                {
                    "fieldPath": "created_at",
                    "recursive": None,  # Should be removed
                    "isPartOfKey": None,  # Should be removed
                    "type": "TIMESTAMP",
                },
            ],
        },
    }

    result = clean_get_entity_response(raw_response)

    expected_result = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
        "name": "users",
        "schemaMetadata": {
            "fields": [
                {"fieldPath": "user_id", "type": "STRING"},
                {
                    "fieldPath": "email",
                    "recursive": True,
                    "isPartOfKey": True,
                    "type": "STRING",
                },
                {
                    "fieldPath": "created_at",
                    "type": "TIMESTAMP",
                },
            ]
        },
    }

    assert result == expected_result


def test_truncate_query_long() -> None:
    """Test that long queries are truncated correctly."""
    with patch.object(mcp_server, "QUERY_LENGTH_HARD_LIMIT", 50):
        long_query = "SELECT * FROM very_long_table_name_that_goes_on_and_on " * 100
        result = truncate_query(long_query)

        assert (
            len(result) == 50
        )  # 35 + "... [truncated]" (truncate_with_ellipsis accounts for suffix length)
        assert result.endswith("... [truncated]")
        assert result.startswith("SELECT * FROM very_long_table_name_")


def test_truncate_query_short() -> None:
    """Test that short queries are not truncated."""
    short_query = "SELECT short_query"
    result = truncate_query(short_query)

    assert result == short_query
    assert not result.endswith("... [truncated]")


def test_truncate_descriptions() -> None:
    result = {
        "downstreams": {
            "searchResults": [
                {
                    "entity": {
                        "description": "Description with ![image](data:image/png;base64,encoded_data) and more content that exceeds the limit",
                        "properties": {
                            "description": "Description with image <img src='data:image/png;base64,encoded_data' /> and more content that exceeds the limit"
                        },
                        "fields": [
                            {
                                "fieldPath": "description",
                                "description": "Description with image <img src='data:image/png;base64,encoded_data' /> and more content that exceeds the limit",
                            },
                            {
                                "fieldPath": "description",
                                "description": "Simple description",
                            },
                        ],
                    }
                }
            ]
        }
    }

    truncate_descriptions(result, 50)

    assert result == {
        "downstreams": {
            "searchResults": [
                {
                    "entity": {
                        "description": "Description with image and more content that exceeds the limit",
                        "properties": {
                            "description": "Description with image  and more content that exceeds the limit"
                        },
                        "fields": [
                            {
                                "fieldPath": "description",
                                "description": "Description with image  and more content that exceeds the limit",
                            },
                            {
                                "fieldPath": "description",
                                "description": "Simple description",
                            },
                        ],
                    }
                }
            ]
        }
    }
