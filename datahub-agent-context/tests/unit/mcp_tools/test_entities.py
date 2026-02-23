"""Tests for entity tools."""

from unittest.mock import Mock

import pytest

from datahub.errors import ItemNotFoundError
from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.entities import get_entities, list_schema_fields


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.execute_graphql = Mock()
    mock.graph.exists = Mock()
    return mock


@pytest.fixture
def sample_entity_response():
    """Sample entity response from GraphQL."""
    return {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        "type": "DATASET",
        "name": "table",
        "description": "A sample table",
        "properties": {"created": "2024-01-01"},
    }


class TestGetEntitiesSingleURN:
    """Tests for get_entities with single URN."""

    def test_single_urn_as_string(self, mock_client, sample_entity_response):
        """Test passing a single URN as string returns a single dict."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_entity_response
        }

        with DataHubContext(mock_client):
            result = get_entities(urn)
        assert isinstance(result, dict)
        assert result["urn"] == urn
        assert result["name"] == "table"
        mock_client._graph.exists.assert_called_once_with(urn)

    def test_single_urn_not_found_raises_error(self, mock_client):
        """Test that single URN not found raises ItemNotFoundError."""
        urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.nonexistent,PROD)"
        )

        mock_client._graph.exists.return_value = False

        with pytest.raises(ItemNotFoundError, match="Entity .* not found"):
            with DataHubContext(mock_client):
                get_entities(urn)

    def test_single_urn_graphql_error_raises(self, mock_client):
        """Test that GraphQL error for single URN raises exception."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(Exception, match="GraphQL error"):
            with DataHubContext(mock_client):
                get_entities(urn)


class TestGetEntitiesMultipleURNs:
    """Tests for get_entities with multiple URNs."""

    def test_multiple_urns_as_list(self, mock_client, sample_entity_response):
        """Test passing multiple URNs as list returns array."""
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
        ]

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_entity_response
        }

        with DataHubContext(mock_client):
            result = get_entities(urns)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(r, dict) for r in result)

    def test_multiple_urns_partial_failure(self, mock_client, sample_entity_response):
        """Test that partial failures return error objects in array."""
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
        ]

        # First exists, second doesn't
        mock_client._graph.exists.side_effect = [True, False]
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_entity_response
        }

        with DataHubContext(mock_client):
            result = get_entities(urns)
        assert isinstance(result, list)
        assert len(result) == 2
        assert "urn" in result[0]
        assert "error" in result[1]


class TestGetEntitiesJSONParsing:
    """Tests for JSON array parsing in get_entities."""

    def test_json_array_string(self, mock_client, sample_entity_response):
        """Test parsing JSON array string."""
        urns_json = '["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)", "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)"]'

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_entity_response
        }

        with DataHubContext(mock_client):
            result = get_entities(urns_json)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_malformed_json_treated_as_single_urn(
        self, mock_client, sample_entity_response
    ):
        """Test malformed JSON is repaired by json_repair and treated as list."""
        urns_malformed = '["incomplete'

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_entity_response
        }

        with DataHubContext(mock_client):
            result = get_entities(urns_malformed)
        # json_repair successfully repairs this to ["incomplete"], so treated as list
        assert isinstance(result, list)


class TestGetEntitiesQueryEntity:
    """Tests for Query entity handling."""

    def test_query_entity_uses_different_gql(self, mock_client):
        """Test that Query entities use query_entity_gql."""
        urn = "urn:li:query:test123"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.side_effect = [
            {
                "entity": {
                    "urn": urn,
                    "type": "QUERY",
                    "properties": {"statement": {"value": "SELECT * FROM table"}},
                }
            },
            {"entity": {}},
        ]

        with DataHubContext(mock_client):
            result = get_entities(urn)
        assert result["urn"] == urn
        # Verify the correct operation name was used
        assert mock_client._graph.execute_graphql.call_count == 2
        call_args_list = mock_client._graph.execute_graphql.call_args_list
        assert call_args_list[0].kwargs.get("operation_name") == "GetQueryEntity"
        assert call_args_list[1].kwargs.get("operation_name") == "getRelatedDocuments"


class TestGetEntitiesRelatedDocuments:
    """Tests for related documents fetching in get_entities."""

    def test_related_documents_included_when_present(self, mock_client):
        """Test that relatedDocuments are added to result when returned by GraphQL."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.side_effect = [
            {
                "entity": {
                    "urn": urn,
                    "type": "DATASET",
                    "name": "table",
                }
            },
            {
                "entity": {
                    "relatedDocuments": {
                        "total": 1,
                        "searchResults": [
                            {"entity": {"urn": "urn:li:post:doc1", "title": "Doc 1"}}
                        ],
                    }
                }
            },
        ]

        with DataHubContext(mock_client):
            result = get_entities(urn)
        assert "relatedDocuments" in result
        assert result["relatedDocuments"]["total"] == 1

    def test_related_documents_absent_when_empty(self, mock_client):
        """Test that relatedDocuments key is not added when response has no relatedDocuments."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.side_effect = [
            {"entity": {"urn": urn, "type": "DATASET", "name": "table"}},
            {"entity": {}},
        ]

        with DataHubContext(mock_client):
            result = get_entities(urn)
        assert "relatedDocuments" not in result

    def test_related_documents_absent_when_entity_missing(self, mock_client):
        """Test that relatedDocuments key is not added when response has no entity."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.side_effect = [
            {"entity": {"urn": urn, "type": "DATASET", "name": "table"}},
            {},
        ]

        with DataHubContext(mock_client):
            result = get_entities(urn)
        assert "relatedDocuments" not in result

    def test_related_documents_failure_does_not_raise(self, mock_client):
        """Test that a failure fetching related documents does not fail the whole call."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.side_effect = [
            {"entity": {"urn": urn, "type": "DATASET", "name": "table"}},
            Exception("related docs query failed"),
        ]

        with DataHubContext(mock_client):
            result = get_entities(urn)
        assert result["urn"] == urn
        assert "relatedDocuments" not in result

    def test_related_documents_uses_correct_operation_name(self, mock_client):
        """Test that related documents query uses the getRelatedDocuments operation name."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.side_effect = [
            {"entity": {"urn": urn, "type": "DATASET", "name": "table"}},
            {"entity": {}},
        ]

        with DataHubContext(mock_client):
            get_entities(urn)
        assert mock_client._graph.execute_graphql.call_count == 2
        call_args_list = mock_client._graph.execute_graphql.call_args_list
        assert call_args_list[0].kwargs.get("operation_name") == "GetEntity"
        assert call_args_list[1].kwargs.get("operation_name") == "getRelatedDocuments"


class TestListSchemaFields:
    """Tests for list_schema_fields."""

    @pytest.fixture
    def sample_dataset_with_schema(self):
        """Sample dataset with schema fields."""
        return {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "schemaMetadata": {
                "fields": [
                    {
                        "fieldPath": "user_id",
                        "type": "INTEGER",
                        "description": "User identifier",
                    },
                    {
                        "fieldPath": "email",
                        "type": "STRING",
                        "description": "User email address",
                    },
                    {
                        "fieldPath": "created_at",
                        "type": "TIMESTAMP",
                        "description": "Creation timestamp",
                    },
                ]
            },
        }

    def test_list_all_fields(self, mock_client, sample_dataset_with_schema):
        """Test listing all fields without filtering."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_dataset_with_schema
        }

        with DataHubContext(mock_client):
            result = list_schema_fields(urn)
        assert result["urn"] == urn
        assert result["totalFields"] == 3
        assert result["returned"] == 3
        assert len(result["fields"]) == 3
        assert result["matchingCount"] is None

    def test_list_fields_with_keyword_filter(
        self, mock_client, sample_dataset_with_schema
    ):
        """Test filtering fields by keyword."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_dataset_with_schema
        }

        with DataHubContext(mock_client):
            result = list_schema_fields(urn, keywords="user")
        assert result["urn"] == urn
        assert result["totalFields"] == 3
        assert result["matchingCount"] == 2  # user_id and email (from "User")
        # Fields matching keyword should come first
        assert (
            "user" in result["fields"][0]["fieldPath"].lower()
            or "user" in result["fields"][0].get("description", "").lower()
        )

    def test_list_fields_with_multiple_keywords(
        self, mock_client, sample_dataset_with_schema
    ):
        """Test filtering with multiple keywords (OR logic)."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_dataset_with_schema
        }

        with DataHubContext(mock_client):
            result = list_schema_fields(urn, keywords=["email", "timestamp"])
        assert result["urn"] == urn
        assert result["matchingCount"] >= 2  # email and created_at

    def test_list_fields_with_pagination(self, mock_client, sample_dataset_with_schema):
        """Test pagination with limit and offset."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": sample_dataset_with_schema
        }

        with DataHubContext(mock_client):
            result = list_schema_fields(urn, limit=2, offset=0)
        assert result["returned"] <= 2
        assert result["offset"] == 0

    def test_list_fields_entity_not_found(self, mock_client):
        """Test that non-existent entity raises error."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.fake,PROD)"

        mock_client._graph.exists.return_value = False

        with pytest.raises(ItemNotFoundError):
            with DataHubContext(mock_client):
                list_schema_fields(urn)

    def test_list_fields_empty_schema(self, mock_client):
        """Test handling of dataset with no schema fields."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        mock_client._graph.exists.return_value = True
        mock_client._graph.execute_graphql.return_value = {
            "entity": {"urn": urn, "schemaMetadata": {"fields": []}}
        }

        with DataHubContext(mock_client):
            result = list_schema_fields(urn)
        assert result["totalFields"] == 0
        assert result["returned"] == 0
        assert result["fields"] == []
