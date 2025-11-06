"""Tests for list_columns function."""

from unittest.mock import Mock, patch

import pytest
from datahub.errors import ItemNotFoundError

from datahub_integrations.mcp.mcp_server import async_background

pytestmark = pytest.mark.anyio


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    client = Mock()
    client._graph = Mock()
    return client


@pytest.fixture
def sample_dataset_with_schema():
    """Sample dataset response with schema fields."""
    return {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        "schemaMetadata": {
            "fields": [
                {
                    "fieldPath": "user_id",
                    "type": "STRING",
                    "description": "Unique identifier for user",
                    "label": "User ID",
                },
                {
                    "fieldPath": "user_email",
                    "type": "STRING",
                    "description": "User email address",
                    "tags": {"tags": [{"tag": {"properties": {"name": "PII"}}}]},
                },
                {
                    "fieldPath": "user_name",
                    "type": "STRING",
                    "description": "Full name of the user",
                },
                {
                    "fieldPath": "order_id",
                    "type": "STRING",
                    "description": "Unique identifier for order",
                },
                {
                    "fieldPath": "created_at",
                    "type": "TIMESTAMP",
                    "description": "Record creation timestamp",
                },
            ]
        },
    }


class TestListSchemaFields:
    """Tests for list_schema_fields function."""

    async def test_returns_columns_matching_keywords(
        self, mock_client, sample_dataset_with_schema
    ):
        """Test that columns matching keywords are returned first."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": sample_dataset_with_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                result = await async_background(list_schema_fields)(
                    urn=urn, keywords=["user", "email"]
                )

                # Should return fields
                assert result["urn"] == urn
                assert result["totalFields"] == 5
                assert result["matchingCount"] >= 2  # user_email, user_id, user_name

                # Matching fields should be first
                fields = result["fields"]
                first_field_paths = [f["fieldPath"] for f in fields[:3]]
                assert "user_email" in first_field_paths
                assert "user_id" in first_field_paths
                assert "user_name" in first_field_paths

    async def test_handles_single_keyword_string(
        self, mock_client, sample_dataset_with_schema
    ):
        """Test that single keyword as string is handled correctly."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": sample_dataset_with_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                result = await async_background(list_schema_fields)(
                    urn=urn, keywords="email"
                )

                # Should work with string keyword
                assert result["totalFields"] == 5
                assert any("email" in f["fieldPath"] for f in result["fields"])

    async def test_respects_limit(self, mock_client, sample_dataset_with_schema):
        """Test that limit parameter is enforced."""
        # Create dataset with many columns
        large_schema = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": f"field_{i:03d}", "type": "STRING"}
                    for i in range(200)
                ]
            },
        }

        urn = large_schema["urn"]

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": large_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                result = await async_background(list_schema_fields)(
                    urn=urn, keywords=["field"], limit=50
                )

                # Should limit to 50
                assert len(result["fields"]) == 50
                assert result["totalFields"] == 200
                assert result["returned"] == 50
                assert result["remainingCount"] > 0  # More fields available
                assert result["offset"] == 0

    async def test_pagination_with_offset(self, mock_client):
        """Test that offset parameter works for pagination."""
        large_schema = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": f"field_{i:03d}", "type": "STRING"}
                    for i in range(200)
                ]
            },
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": large_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                # First page
                page1 = await async_background(list_schema_fields)(
                    urn=large_schema["urn"], limit=50, offset=0
                )

                # Second page
                page2 = await async_background(list_schema_fields)(
                    urn=large_schema["urn"], limit=50, offset=50
                )

                # Pages should be different
                assert (
                    page1["fields"][0]["fieldPath"] != page2["fields"][0]["fieldPath"]
                )
                assert page1["offset"] == 0
                assert page2["offset"] == 50
                assert page1["remainingCount"] > 0  # More fields available
                assert page2["remainingCount"] > 0  # More fields available

    async def test_last_page_has_more_false(self, mock_client):
        """Test that hasMore is False on last page."""
        small_schema = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": f"field_{i}", "type": "STRING"} for i in range(50)
                ]
            },
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": small_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                # Request all 50 with limit=100
                result = await async_background(list_schema_fields)(
                    urn=small_schema["urn"], limit=100, offset=0
                )

                # Should return all 50, hasMore=False
                assert len(result["fields"]) == 50
                assert result["remainingCount"] == 0  # No more fields

    async def test_works_without_keywords(
        self, mock_client, sample_dataset_with_schema
    ):
        """Test that tool works without keywords (returns all in priority order)."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": sample_dataset_with_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                result = await async_background(list_schema_fields)(urn=urn)

                # Should return all fields
                assert result["totalFields"] == 5
                assert len(result["fields"]) == 5
                assert result["matchingCount"] is None  # No keywords
                assert result["remainingCount"] == 0  # No more fields

    async def test_empty_keywords_list_uses_priority_order(self, mock_client):
        """Test that empty keywords list uses priority ordering (same as None)."""
        schema_with_keys = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": "z_regular_field", "type": "STRING"},
                    {
                        "fieldPath": "a_primary_key",
                        "type": "STRING",
                        "isPartOfKey": True,
                    },
                    {
                        "fieldPath": "m_partition",
                        "type": "STRING",
                        "isPartitioningKey": True,
                    },
                ]
            },
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": schema_with_keys}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                result = await async_background(list_schema_fields)(
                    urn=schema_with_keys["urn"], keywords=[]
                )

                # Should use priority ordering (keys first)
                fields = result["fields"]
                assert fields[0]["fieldPath"] == "a_primary_key"  # isPartOfKey
                assert fields[1]["fieldPath"] == "m_partition"  # isPartitioningKey
                assert fields[2]["fieldPath"] == "z_regular_field"  # Regular
                assert result["matchingCount"] is None  # No keyword filtering

    async def test_handles_entity_not_found(self, mock_client):
        """Test that entity not found raises ItemNotFoundError."""
        urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.nonexistent,PROD)"
        )

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = False

            from datahub_integrations.mcp.mcp_server import list_schema_fields

            with pytest.raises(ItemNotFoundError, match="Entity .* not found"):
                await async_background(list_schema_fields)(urn=urn, keywords=["test"])

    async def test_handles_entity_without_schema(self, mock_client):
        """Test handling of entity without schemaMetadata."""
        entity_without_schema = {
            "urn": "urn:li:dashboard:(looker,dashboard)",
            "name": "Dashboard",
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": entity_without_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                result = await async_background(list_schema_fields)(
                    urn=entity_without_schema["urn"], keywords=["test"]
                )

                assert result["fields"] == []
                assert result["totalFields"] == 0
                assert result["remainingCount"] == 0  # No more fields

    async def test_case_insensitive_keyword_matching(
        self, mock_client, sample_dataset_with_schema
    ):
        """Test that keyword matching is case-insensitive."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": sample_dataset_with_schema}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                # Use uppercase keywords
                result = await async_background(list_schema_fields)(
                    urn=urn, keywords=["EMAIL", "USER"]
                )

                # Should still match lowercase fields
                assert result["matchingCount"] > 0
                assert any("email" in f["fieldPath"].lower() for f in result["fields"])

    async def test_matches_against_tags_and_glossary_terms(self, mock_client):
        """Test that keywords match against tags and glossary terms."""
        dataset_with_tags = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "schemaMetadata": {
                "fields": [
                    {
                        "fieldPath": "ssn",
                        "type": "STRING",
                        "tags": {
                            "tags": [
                                {"tag": {"properties": {"name": "PII"}}},
                                {"tag": {"properties": {"name": "Sensitive"}}},
                            ]
                        },
                    },
                    {
                        "fieldPath": "address",
                        "type": "STRING",
                        "glossaryTerms": {
                            "terms": [
                                {
                                    "term": {
                                        "properties": {"name": "Contact Information"}
                                    }
                                }
                            ]
                        },
                    },
                    {
                        "fieldPath": "age",
                        "type": "INTEGER",
                    },
                ]
            },
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": dataset_with_tags}

                from datahub_integrations.mcp.mcp_server import list_schema_fields

                # Search for PII
                result = await async_background(list_schema_fields)(
                    urn=dataset_with_tags["urn"], keywords=["pii", "contact"]
                )

                # Should match ssn (has PII tag) and address (has Contact glossary term)
                assert result["matchingCount"] == 2
                field_paths = [f["fieldPath"] for f in result["fields"]]
                assert "ssn" in field_paths
                assert "address" in field_paths
