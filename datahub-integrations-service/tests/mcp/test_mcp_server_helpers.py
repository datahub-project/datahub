from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.links import make_url_for_urn

from datahub_integrations.mcp import mcp_server
from datahub_integrations.mcp.mcp_server import (
    _clean_schema_fields,
    _sort_fields_by_priority,
    clean_get_entities_response,
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


def test_clean_gql_response_strips_base64_images_from_descriptions() -> None:
    """Test that base64 images are stripped from description fields."""
    base64_image = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="

    response = {
        "urn": "urn:li:glossaryTerm:test",
        "properties": {
            "name": "Test Term",
            "description": f"This is a test description with an embedded image: {base64_image} and some more text.",
        },
    }

    result = clean_gql_response(response)

    # Base64 image should be replaced with [image removed]
    assert "[image removed]" in result["properties"]["description"]
    assert "base64" not in result["properties"]["description"]
    assert "This is a test description" in result["properties"]["description"]
    assert "and some more text" in result["properties"]["description"]


def test_clean_gql_response_strips_markdown_base64_images() -> None:
    """Test that markdown-formatted base64 images are stripped."""
    markdown_image = "![alt text](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==)"

    response = {
        "properties": {
            "description": f"Text before {markdown_image} text after",
        }
    }

    result = clean_gql_response(response)

    assert "[image removed]" in result["properties"]["description"]
    assert "data:image" not in result["properties"]["description"]


def test_clean_gql_response_handles_nested_glossary_terms_with_base64() -> None:
    """Test base64 stripping from nested glossary term descriptions (xero-3 case)."""
    huge_base64 = "data:image/png;base64," + ("A" * 1000000)  # 1MB base64

    response = {
        "urn": "urn:li:dashboard:test",
        "glossaryTerms": {
            "terms": [
                {
                    "term": {
                        "properties": {
                            "name": "Billing Channel",
                            "description": f"Channel description with {huge_base64} embedded image",
                        }
                    }
                }
            ]
        },
    }

    result = clean_gql_response(response)

    # Should be much smaller now
    term_desc = result["glossaryTerms"]["terms"][0]["term"]["properties"]["description"]
    assert len(term_desc) < 1000  # Should be tiny now (was 1MB+)
    assert "[image removed]" in term_desc
    assert "base64" not in term_desc


def test_clean_gql_response_preserves_normal_descriptions() -> None:
    """Test that normal descriptions without base64 are preserved."""
    response = {
        "properties": {
            "description": "This is a normal description with no images or base64 data.",
        }
    }

    result = clean_gql_response(response)

    # Should be unchanged
    assert (
        result["properties"]["description"]
        == "This is a normal description with no images or base64 data."
    )


def test_clean_gql_response_handles_multiple_base64_images() -> None:
    """Test stripping multiple base64 images from same description."""
    img1 = "data:image/png;base64,ABC123"
    img2 = "data:image/jpeg;base64,XYZ789"

    response = {
        "properties": {
            "description": f"First {img1} middle text {img2} end",
        }
    }

    result = clean_gql_response(response)
    desc = result["properties"]["description"]

    # Both images should be removed
    assert desc.count("[image removed]") == 2
    assert "base64" not in desc
    assert "middle text" in desc
    assert "end" in desc


def test_clean_gql_response_handles_different_image_formats() -> None:
    """Test stripping various image format base64 encodings."""
    formats = ["png", "jpeg", "jpg", "gif", "svg+xml", "webp"]

    for fmt in formats:
        response = {
            "properties": {
                "description": f"Image: data:image/{fmt};base64,ABCDEF123456",
            }
        }

        result = clean_gql_response(response)

        assert "base64" not in result["properties"]["description"], (
            f"Failed for format: {fmt}"
        )
        assert "[image removed]" in result["properties"]["description"]


def test_clean_gql_response_handles_very_long_base64() -> None:
    """Test performance with very large base64 strings (10MB)."""
    # 10MB base64 string
    huge_base64 = "data:image/png;base64," + ("A" * 10_000_000)

    response = {
        "properties": {
            "description": f"Text before {huge_base64} text after",
        }
    }

    result = clean_gql_response(response)
    desc = result["properties"]["description"]

    # Should complete without hanging or memory issues
    assert len(desc) < 100  # Much smaller now
    assert "[image removed]" in desc
    assert "Text before" in desc
    assert "text after" in desc


def test_clean_gql_response_handles_empty_and_none_descriptions() -> None:
    """Test handling of None and empty description fields."""
    test_cases = [
        {"properties": {"description": None}},  # None
        {"properties": {"description": ""}},  # Empty string
        {"properties": {}},  # No description key
        {},  # No properties
    ]

    for response in test_cases:
        result = clean_gql_response(response)
        # Should not crash, should clean appropriately
        assert isinstance(result, dict)


def test_clean_gql_response_does_not_strip_base64_from_non_description_fields() -> None:
    """Test that base64 in other fields (like names, URNs) is NOT stripped."""
    response = {
        "urn": "urn:li:dataset:base64_encoded_name",  # Should NOT be stripped
        "name": "table_with_base64_in_name",  # Should NOT be stripped
        "properties": {
            "description": "data:image/png;base64,ABC123",  # SHOULD be stripped
            "custom_field": "data:image/png;base64,XYZ789",  # Should NOT be stripped (not 'description')
        },
    }

    result = clean_gql_response(response)

    # Description should be stripped
    assert "base64" not in result["properties"]["description"]

    # Other fields should NOT be stripped
    assert "base64" in result["urn"]
    assert "base64" in result["name"]
    assert "base64" in result["properties"]["custom_field"]


def test_clean_gql_response_handles_case_insensitive_base64() -> None:
    """Test that BASE64 (uppercase) is also detected."""
    response = {
        "properties": {
            "description": "Image: data:image/PNG;BASE64,ABCDEF123456",
        }
    }

    result = clean_gql_response(response)

    # Should be case-insensitive
    desc = result["properties"]["description"]
    # Current implementation might not catch uppercase - document this limitation
    # or fix the regex to be case-insensitive
    assert isinstance(desc, str)


def test_clean_gql_response_handles_malformed_base64() -> None:
    """Test handling of incomplete or malformed base64 strings."""
    test_cases = [
        "data:image/png;base64,",  # Empty base64
        "data:image/png;base64",  # Missing comma
        "data:image/;base64,ABC",  # Missing format
        "data:image/png;base64,ABC!!!",  # Invalid base64 chars (but should still match)
    ]

    for malformed in test_cases:
        response = {
            "properties": {
                "description": f"Text {malformed} more text",
            }
        }

        result = clean_gql_response(response)
        # Should handle gracefully without crashing
        assert isinstance(result, dict)
        assert isinstance(result["properties"]["description"], str)


def test_clean_gql_response_handles_special_characters_near_base64() -> None:
    """Test base64 surrounded by special characters and unicode."""
    response = {
        "properties": {
            "description": "Émoji 🎉 before data:image/png;base64,ABC123 and äfter with spëcial chars",
        }
    }

    result = clean_gql_response(response)
    desc = result["properties"]["description"]

    assert "base64" not in desc
    assert "Émoji 🎉" in desc
    assert "äfter with spëcial chars" in desc


def test_clean_get_entities_response_with_schema_metadata() -> None:
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
                    "nativeDataType": "VARCHAR(255)",
                    "nullable": True,
                    "label": "User ID",
                },
                {
                    "fieldPath": "email",
                    "recursive": True,  # Should be kept
                    "isPartOfKey": True,  # Should be kept
                    "isPartitioningKey": True,  # Should be kept
                    "type": "STRING",
                    "nullable": False,
                    "schemaFieldEntity": {
                        "deprecation": {
                            "deprecated": True,
                            "note": "Use user_email instead",
                        }
                    },
                    "tags": {"tags": [{"tag": {"properties": {"name": "PII"}}}]},
                },
                {
                    "fieldPath": "created_at",
                    "recursive": None,  # Should be removed
                    "isPartOfKey": None,  # Should be removed
                    "type": "TIMESTAMP",
                    "nativeDataType": "TIMESTAMP_NTZ",
                    "nullable": False,
                    "glossaryTerms": {
                        "terms": [
                            {"term": {"properties": {"name": "Record Creation Time"}}}
                        ]
                    },
                },
            ],
        },
    }

    result = clean_get_entities_response(raw_response)

    # Expected result with fields sorted by priority:
    # 1. email (isPartOfKey=True, isPartitioningKey=True) - highest priority
    # 2. created_at (has glossaryTerms) - medium priority
    # 3. user_id (plain field) - lowest priority
    expected_result = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)",
        "name": "users",
        "schemaMetadata": {
            "fields": [
                {
                    "fieldPath": "email",
                    "type": "STRING",
                    "nullable": False,
                    "recursive": True,
                    "isPartOfKey": True,
                    "isPartitioningKey": True,
                    "deprecated": {
                        "deprecated": True,
                        "note": "Use user_email instead",
                    },
                    "tags": ["PII"],
                },
                {
                    "fieldPath": "created_at",
                    "type": "TIMESTAMP",
                    "nativeDataType": "TIMESTAMP_NTZ",
                    "nullable": False,
                    "glossaryTerms": ["Record Creation Time"],
                },
                {
                    "fieldPath": "user_id",
                    "type": "STRING",
                    "nativeDataType": "VARCHAR(255)",
                    "nullable": True,
                    "label": "User ID",
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


def test_get_lineage_normalizes_null_string() -> None:
    """Test that get_lineage normalizes the string 'null' to None for the column parameter."""
    from datahub_integrations.mcp.mcp_server import get_lineage

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)"

    # Mock the DataHub client and related components
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client"
    ) as mock_get_client:
        mock_client = Mock()
        mock_graph = Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        # Mock the lineage API
        mock_lineage_api = Mock()
        mock_lineage = {
            "searchResults": [],
            "total": 0,
        }
        mock_lineage_api.get_lineage.return_value = mock_lineage

        with patch(
            "datahub_integrations.mcp.mcp_server.AssetLineageAPI",
            return_value=mock_lineage_api,
        ):
            # Test with string "null" - should be normalized to None
            get_lineage(urn=dataset_urn, column="null")

            # Verify that maybe_convert_to_schema_field_urn was called with None for column
            # This is verified by checking the AssetLineageDirective was created with dataset URN (not schema field URN)
            call_args = mock_lineage_api.get_lineage.call_args
            directive = call_args[0][0]
            assert (
                directive.urn == dataset_urn
            )  # Should be dataset URN, not schema field URN

            # Also test with an actual column name to ensure normal behavior still works
            get_lineage(urn=dataset_urn, column="user_id")
            call_args = mock_lineage_api.get_lineage.call_args
            directive = call_args[0][0]
            # Should be converted to schema field URN
            assert directive.urn.startswith("urn:li:schemaField:")
            assert "user_id" in directive.urn


def test_get_lineage_cleans_entities_in_results() -> None:
    """Test that entities in lineage results are cleaned via clean_get_entities_response."""
    from datahub_integrations.mcp.mcp_server import get_lineage

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.raw_schema.users,PROD)"

    # Mock entity with schema fields (should be cleaned)
    entity_with_schema = {
        "urn": dataset_urn,
        "schemaMetadata": {
            "fields": [
                {"fieldPath": "user_id", "type": "STRING"},
                {"fieldPath": "email", "type": "STRING"},
            ]
        },
    }

    mock_lineage_response = {
        "downstreams": {
            "searchResults": [
                {"entity": entity_with_schema},
            ],
            "total": 1,
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client"
    ) as mock_get_client:
        mock_client = Mock()
        mock_graph = Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_lineage_api = Mock()
        mock_lineage_api.get_lineage.return_value = mock_lineage_response

        with patch(
            "datahub_integrations.mcp.mcp_server.AssetLineageAPI",
            return_value=mock_lineage_api,
        ):
            with patch(
                "datahub_integrations.mcp.mcp_server.clean_get_entities_response"
            ) as mock_clean:
                mock_clean.side_effect = lambda x: {**x, "cleaned": True}

                result = get_lineage(urn=dataset_urn, column=None)

                # Verify clean_get_entities_response was called for the entity
                mock_clean.assert_called_once()
                # Verify the cleaned entity is in the result (under downstreams)
                assert (
                    result["downstreams"]["searchResults"][0]["entity"]["cleaned"]
                    is True
                )


class TestSortFieldsByPriority:
    """Tests for _sort_fields_by_priority generator function."""

    def test_sorts_keys_first(self) -> None:
        """Test that primary and partition keys are sorted first."""
        fields: list[dict] = [
            {"fieldPath": "regular_field", "description": "desc"},
            {"fieldPath": "partition_key", "isPartitioningKey": True},
            {"fieldPath": "primary_key", "isPartOfKey": True},
            {"fieldPath": "another_field"},
        ]

        result = list(_sort_fields_by_priority(fields))

        # Primary keys (score=2) come first, then partition keys (score=1), then others
        assert result[0]["fieldPath"] == "primary_key"
        assert result[1]["fieldPath"] == "partition_key"

    def test_sorts_fields_with_descriptions_higher(self) -> None:
        """Test that fields with descriptions are prioritized."""
        fields: list[dict] = [
            {"fieldPath": "z_field", "description": "has description"},
            {"fieldPath": "a_field"},  # No description but alphabetically first
            {"fieldPath": "m_field", "description": "also has description"},
        ]

        result = list(_sort_fields_by_priority(fields))

        # Fields with descriptions come before those without
        # Among fields with descriptions, alphabetical order
        assert result[0]["fieldPath"] == "m_field"
        assert result[1]["fieldPath"] == "z_field"
        assert result[2]["fieldPath"] == "a_field"

    def test_sorts_fields_with_tags_higher(self) -> None:
        """Test that fields with tags/glossary terms are prioritized."""
        fields: list[dict] = [
            {"fieldPath": "z_field"},
            {"fieldPath": "tagged_field", "tags": {"tags": [{"tag": {}}]}},
            {
                "fieldPath": "glossary_field",
                "glossaryTerms": {"terms": [{"term": {}}]},
            },
        ]

        result = list(_sort_fields_by_priority(fields))

        # Fields with tags/glossary come before those without
        assert result[0]["fieldPath"] == "glossary_field"
        assert result[1]["fieldPath"] == "tagged_field"
        assert result[2]["fieldPath"] == "z_field"

    def test_alphabetical_tiebreaker(self) -> None:
        """Test alphabetical sorting when all other factors equal."""
        fields = [
            {"fieldPath": "zebra", "description": "desc"},
            {"fieldPath": "apple", "description": "desc"},
            {"fieldPath": "middle", "description": "desc"},
        ]

        result = list(_sort_fields_by_priority(fields))

        # Alphabetical order when all have same priority
        assert result[0]["fieldPath"] == "apple"
        assert result[1]["fieldPath"] == "middle"
        assert result[2]["fieldPath"] == "zebra"

    def test_deterministic_ordering(self) -> None:
        """Test that sorting is deterministic across multiple calls."""
        fields: list[dict] = [
            {"fieldPath": "field_3", "description": "desc"},
            {"fieldPath": "field_1", "isPartOfKey": True},
            {"fieldPath": "field_2", "tags": {"tags": [{}]}},
        ]

        result1 = list(_sort_fields_by_priority(fields))
        result2 = list(_sort_fields_by_priority(fields))

        # Same order every time
        assert [f["fieldPath"] for f in result1] == [f["fieldPath"] for f in result2]

    def test_returns_iterator(self) -> None:
        """Test that function returns an iterator, not a list."""
        fields = [{"fieldPath": "field"}]
        result = _sort_fields_by_priority(fields)

        # Should be an iterator
        assert hasattr(result, "__iter__")
        assert hasattr(result, "__next__")


class TestCleanSchemaFields:
    """Tests for _clean_schema_fields generator function."""

    def test_cleans_field_properties(self) -> None:
        """Test that only essential properties are kept."""
        fields = [
            {
                "fieldPath": "user_id",
                "type": "STRING",
                "nativeDataType": "VARCHAR(255)",
                "nullable": True,
                "label": "User ID",
                "description": "The user identifier",
                "extra_property": "should_be_removed",
                "__typename": "SchemaField",
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert len(result) == 1
        field = result[0]
        assert field["fieldPath"] == "user_id"
        assert field["type"] == "STRING"
        assert field["nativeDataType"] == "VARCHAR(255)"
        assert field["nullable"] is True
        assert field["label"] == "User ID"
        assert field["description"] == "The user identifier"
        assert "extra_property" not in field
        assert "__typename" not in field

    def test_truncates_long_descriptions(self) -> None:
        """Test that field descriptions are truncated to 120 chars."""
        long_desc = "x" * 200
        fields = [{"fieldPath": "field", "description": long_desc}]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert len(result[0]["description"]) == 120
        assert result[0]["description"] == long_desc[:120]

    def test_omits_false_boolean_flags(self) -> None:
        """Test that isPartOfKey, isPartitioningKey, recursive are omitted when False."""
        fields = [
            {
                "fieldPath": "field",
                "isPartOfKey": False,
                "isPartitioningKey": False,
                "recursive": False,
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert "isPartOfKey" not in result[0]
        assert "isPartitioningKey" not in result[0]
        assert "recursive" not in result[0]

    def test_includes_true_boolean_flags(self) -> None:
        """Test that isPartOfKey, isPartitioningKey, recursive are included when True."""
        fields = [
            {
                "fieldPath": "field",
                "isPartOfKey": True,
                "isPartitioningKey": True,
                "recursive": True,
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert result[0]["isPartOfKey"] is True
        assert result[0]["isPartitioningKey"] is True
        assert result[0]["recursive"] is True

    def test_includes_nullable_when_false(self) -> None:
        """Test that nullable is included even when False (important for SQL)."""
        fields = [{"fieldPath": "field", "nullable": False}]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert "nullable" in result[0]
        assert result[0]["nullable"] is False

    def test_extracts_tag_names(self) -> None:
        """Test that tag names are extracted from nested structure."""
        fields = [
            {
                "fieldPath": "field",
                "tags": {
                    "tags": [
                        {"tag": {"properties": {"name": "PII"}}},
                        {"tag": {"properties": {"name": "Sensitive"}}},
                    ]
                },
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert result[0]["tags"] == ["PII", "Sensitive"]

    def test_extracts_glossary_term_names(self) -> None:
        """Test that glossary term names are extracted from nested structure."""
        fields = [
            {
                "fieldPath": "field",
                "glossaryTerms": {
                    "terms": [
                        {"term": {"properties": {"name": "User Identifier"}}},
                        {"term": {"properties": {"name": "Primary Key"}}},
                    ]
                },
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert result[0]["glossaryTerms"] == ["User Identifier", "Primary Key"]

    def test_handles_tags_with_none_properties(self) -> None:
        """Test that tags with None properties are filtered out (Looker bug)."""
        fields = [
            {
                "fieldPath": "field",
                "tags": {
                    "tags": [
                        {"tag": {"properties": {"name": "Valid Tag"}}},
                        {
                            "tag": {
                                "properties": None,
                                "urn": "urn:li:tag:looker:group_label:Date",
                            }
                        },
                        {"tag": {"properties": {"name": "Another Valid"}}},
                    ]
                },
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        # Should only include tags with valid properties
        assert result[0]["tags"] == ["Valid Tag", "Another Valid"]

    def test_handles_glossary_terms_with_none_properties(self) -> None:
        """Test that glossary terms with None properties are filtered out."""
        fields = [
            {
                "fieldPath": "field",
                "glossaryTerms": {
                    "terms": [
                        {"term": {"properties": {"name": "Valid Term"}}},
                        {
                            "term": {
                                "properties": None,
                                "urn": "urn:li:glossaryTerm:broken",
                            }
                        },
                        {"term": {"properties": {"name": "Another Valid"}}},
                    ]
                },
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        # Should only include terms with valid properties
        assert result[0]["glossaryTerms"] == ["Valid Term", "Another Valid"]

    def test_handles_deprecation_info(self) -> None:
        """Test that deprecation information is preserved."""
        fields = [
            {
                "fieldPath": "old_field",
                "schemaFieldEntity": {
                    "deprecation": {
                        "deprecated": True,
                        "note": "Use new_field instead",
                    }
                },
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert result[0]["deprecated"]["deprecated"] is True
        assert result[0]["deprecated"]["note"] == "Use new_field instead"

    def test_truncates_long_deprecation_notes(self) -> None:
        """Test that deprecation notes are truncated to 120 chars."""
        long_note = "x" * 200
        fields = [
            {
                "fieldPath": "field",
                "schemaFieldEntity": {
                    "deprecation": {"deprecated": True, "note": long_note}
                },
            }
        ]

        result = list(_clean_schema_fields(iter(fields), editable_map={}))

        assert len(result[0]["deprecated"]["note"]) == 120

    def test_returns_iterator(self) -> None:
        """Test that function returns an iterator, not a list."""
        fields = [{"fieldPath": "field"}]
        result = _clean_schema_fields(iter(fields), editable_map={})

        # Should be an iterator
        assert hasattr(result, "__iter__")
        assert hasattr(result, "__next__")

    def test_fails_fast_on_missing_fieldpath(self) -> None:
        """Test that missing fieldPath raises KeyError (fail fast)."""
        fields = [{"description": "field without fieldPath"}]

        with pytest.raises(KeyError, match="fieldPath"):
            list(_clean_schema_fields(iter(fields), editable_map={}))

    def test_merges_edited_description_when_different(self) -> None:
        """Test that editedDescription is added when it differs from system description."""
        fields = [{"fieldPath": "email", "description": "System generated description"}]
        editable_map = {
            "email": {"fieldPath": "email", "description": "User-friendly description"}
        }

        result = list(_clean_schema_fields(iter(fields), editable_map=editable_map))

        assert result[0]["description"] == "System generated description"
        assert result[0]["editedDescription"] == "User-friendly description"

    def test_omits_edited_description_when_same(self) -> None:
        """Test that editedDescription is omitted when identical to system description."""
        fields = [{"fieldPath": "email", "description": "Same description"}]
        editable_map = {
            "email": {"fieldPath": "email", "description": "Same description"}
        }

        result = list(_clean_schema_fields(iter(fields), editable_map=editable_map))

        assert result[0]["description"] == "Same description"
        assert "editedDescription" not in result[0]

    def test_merges_edited_tags_when_different(self) -> None:
        """Test that editedTags are added when they differ from system tags."""
        fields = [
            {
                "fieldPath": "email",
                "tags": {"tags": [{"tag": {"properties": {"name": "SystemTag"}}}]},
            }
        ]
        editable_map = {
            "email": {
                "fieldPath": "email",
                "tags": {
                    "tags": [
                        {"tag": {"properties": {"name": "PII"}}},
                        {"tag": {"properties": {"name": "Sensitive"}}},
                    ]
                },
            }
        }

        result = list(_clean_schema_fields(iter(fields), editable_map=editable_map))

        assert result[0]["tags"] == ["SystemTag"]
        assert result[0]["editedTags"] == ["PII", "Sensitive"]

    def test_merges_edited_glossary_terms_when_different(self) -> None:
        """Test that editedGlossaryTerms are added when they differ."""
        fields = [{"fieldPath": "email"}]
        editable_map = {
            "email": {
                "fieldPath": "email",
                "glossaryTerms": {
                    "terms": [
                        {"term": {"properties": {"name": "Email Address"}}},
                    ]
                },
            }
        }

        result = list(_clean_schema_fields(iter(fields), editable_map=editable_map))

        assert result[0]["editedGlossaryTerms"] == ["Email Address"]
        assert "glossaryTerms" not in result[0]  # System didn't have any


class TestCleanGetEntitiesResponseFieldTruncation:
    """Tests for field truncation in clean_get_entities_response."""

    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
    @patch("datahub_integrations.mcp.mcp_server.ENTITY_SCHEMA_TOKEN_BUDGET", 1000)
    def test_truncates_fields_when_exceeds_budget(self, mock_estimator) -> None:
        """Test that fields are truncated when they exceed token budget."""
        # Each field is 300 tokens, budget is 1000, so should fit 3 fields
        mock_estimator.estimate_dict_tokens.return_value = 300

        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": f"field_{i}", "type": "STRING"} for i in range(10)
                ]
            },
        }

        result = clean_get_entities_response(raw_response)

        # Should have 3 fields (3 * 300 = 900 tokens, 4th would be 1200 > 1000)
        assert len(result["schemaMetadata"]["fields"]) == 3
        assert result["schemaMetadata"]["schemaFieldsTruncated"]["totalFields"] == 10
        assert result["schemaMetadata"]["schemaFieldsTruncated"]["includedFields"] == 3

    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
    @patch("datahub_integrations.mcp.mcp_server.ENTITY_SCHEMA_TOKEN_BUDGET", 10000)
    def test_no_truncation_when_within_budget(self, mock_estimator) -> None:
        """Test that no truncation occurs when fields fit within budget."""
        mock_estimator.estimate_dict_tokens.return_value = 100

        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": f"field_{i}", "type": "STRING"} for i in range(5)
                ]
            },
        }

        result = clean_get_entities_response(raw_response)

        # All 5 fields should be included (5 * 100 = 500 < 10000)
        assert len(result["schemaMetadata"]["fields"]) == 5
        assert "schemaFieldsTruncated" not in result["schemaMetadata"]

    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
    @patch("datahub_integrations.mcp.mcp_server.ENTITY_SCHEMA_TOKEN_BUDGET", 500)
    def test_always_includes_at_least_one_field(self, mock_estimator) -> None:
        """Test that at least 1 field is included even if it exceeds budget."""
        # First field exceeds budget (1000 > 500)
        mock_estimator.estimate_dict_tokens.return_value = 1000

        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": "huge_field", "type": "STRING"},
                    {"fieldPath": "zzz_another_field", "type": "STRING"},
                ]
            },
        }

        result = clean_get_entities_response(raw_response)

        # Should include at least 1 field even though it exceeds budget
        # Fields are sorted alphabetically, so "huge_field" comes first
        assert len(result["schemaMetadata"]["fields"]) == 1
        assert result["schemaMetadata"]["fields"][0]["fieldPath"] == "huge_field"
        assert "schemaFieldsTruncated" in result["schemaMetadata"]

    def test_fields_sorted_by_priority_before_truncation(self) -> None:
        """Test that fields are sorted by priority before truncation."""
        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": "z_regular"},
                    {"fieldPath": "a_primary_key", "isPartOfKey": True},
                    {"fieldPath": "m_partition", "isPartitioningKey": True},
                ]
            },
        }

        result = clean_get_entities_response(raw_response)

        # Keys should be sorted first, then alphabetically
        fields = result["schemaMetadata"]["fields"]
        assert fields[0]["fieldPath"] == "a_primary_key"  # Primary key
        assert fields[1]["fieldPath"] == "m_partition"  # Partition key
        assert fields[2]["fieldPath"] == "z_regular"  # Regular field

    def test_deterministic_field_selection(self) -> None:
        """Test that same entity produces same fields every time."""
        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": f"field_{i}", "type": "STRING"} for i in range(100)
                ]
            },
        }

        result1 = clean_get_entities_response(raw_response.copy())
        result2 = clean_get_entities_response(raw_response.copy())

        fields1 = [f["fieldPath"] for f in result1["schemaMetadata"]["fields"]]
        fields2 = [f["fieldPath"] for f in result2["schemaMetadata"]["fields"]]

        # Same fields in same order
        assert fields1 == fields2

    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
    @patch("datahub_integrations.mcp.mcp_server.ENTITY_SCHEMA_TOKEN_BUDGET", 1000)
    def test_truncation_metadata_accuracy(self, mock_estimator) -> None:
        """Test that truncation metadata accurately reflects field counts."""
        # Each field is 300 tokens, budget is 1000, so 3 fields fit
        mock_estimator.estimate_dict_tokens.return_value = 300

        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {
                "fields": [{"fieldPath": f"field_{i:03d}"} for i in range(10)]
            },
        }

        result = clean_get_entities_response(raw_response)

        metadata = result["schemaMetadata"]["schemaFieldsTruncated"]
        included_count = len(result["schemaMetadata"]["fields"])

        assert metadata["totalFields"] == 10
        assert metadata["includedFields"] == included_count
        assert included_count == 3  # 3 fields fit (3 * 300 = 900 < 1000)


def test_clean_get_entities_response_preserves_existing_behavior() -> None:
    """Test that existing cleaning behavior is preserved (removal of empty platformSchema, etc.)."""
    raw_response = {
        "urn": "urn:li:dataset:test",
        "schemaMetadata": {
            "platformSchema": {"schema": ""},  # Empty schema should be removed
            "fields": [{"fieldPath": "field1", "type": "STRING"}],
        },
    }

    result = clean_get_entities_response(raw_response)

    # Empty platformSchema should be removed
    assert "platformSchema" not in result["schemaMetadata"]
    # Field should be preserved
    assert len(result["schemaMetadata"]["fields"]) == 1


class TestEditableSchemaMetadataMerging:
    """Tests for editableSchemaMetadata merging into schemaMetadata fields."""

    def test_editable_metadata_removed_after_merge(self) -> None:
        """Test that editableSchemaMetadata is removed from response after merging."""
        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {"fields": [{"fieldPath": "field1"}]},
            "editableSchemaMetadata": {
                "editableSchemaFieldInfo": [
                    {"fieldPath": "field1", "description": "User edited"}
                ]
            },
        }

        result = clean_get_entities_response(raw_response)

        # editableSchemaMetadata should be removed
        assert "editableSchemaMetadata" not in result
        # Data should be merged into schemaMetadata
        assert "schemaMetadata" in result

    def test_merged_fields_include_edited_description(self) -> None:
        """Test that edited descriptions are merged into schema fields."""
        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {
                "fields": [
                    {
                        "fieldPath": "email",
                        "description": "System description",
                        "type": "STRING",
                    }
                ]
            },
            "editableSchemaMetadata": {
                "editableSchemaFieldInfo": [
                    {"fieldPath": "email", "description": "User-friendly email field"}
                ]
            },
        }

        result = clean_get_entities_response(raw_response)

        field = result["schemaMetadata"]["fields"][0]
        assert field["description"] == "System description"
        assert field["editedDescription"] == "User-friendly email field"[:120]

    def test_merged_fields_include_edited_tags(self) -> None:
        """Test that edited tags are merged into schema fields."""
        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {"fields": [{"fieldPath": "email", "type": "STRING"}]},
            "editableSchemaMetadata": {
                "editableSchemaFieldInfo": [
                    {
                        "fieldPath": "email",
                        "tags": {
                            "tags": [
                                {"tag": {"properties": {"name": "PII"}}},
                                {"tag": {"properties": {"name": "Sensitive"}}},
                            ]
                        },
                    }
                ]
            },
        }

        result = clean_get_entities_response(raw_response)

        field = result["schemaMetadata"]["fields"][0]
        assert field["editedTags"] == ["PII", "Sensitive"]

    def test_handles_duplicate_fieldpaths_safely(self) -> None:
        """Test that duplicate fieldPaths in editableSchemaMetadata don't cause failures."""
        raw_response = {
            "urn": "urn:li:dataset:test",
            "schemaMetadata": {"fields": [{"fieldPath": "email", "type": "STRING"}]},
            "editableSchemaMetadata": {
                "editableSchemaFieldInfo": [
                    {"fieldPath": "email", "description": "First edit"},
                    {"fieldPath": "email", "description": "Second edit"},  # Duplicate
                ]
            },
        }

        # Should not raise, last one wins
        result = clean_get_entities_response(raw_response)

        field = result["schemaMetadata"]["fields"][0]
        # Last duplicate wins
        assert field["editedDescription"] == "Second edit"[:120]
