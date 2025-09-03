# type: ignore

from unittest import TestCase
from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    INGESTION_SOURCES_BATCH_SIZE,
    DataHubIngestionSourceConnectionProvider,
)


class TestDataHubIngestionSourceConnectionProvider(TestCase):
    """Test cases for DataHubIngestionSourceConnectionProvider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_graph = Mock(spec=DataHubGraph)
        self.mock_secret_stores = [Mock(), Mock()]
        self.provider = DataHubIngestionSourceConnectionProvider(
            self.mock_graph, self.mock_secret_stores
        )

    def test_list_ingestion_sources_with_common_filter(self):
        """Test that list_ingestion_sources applies the common type filter correctly."""
        # Mock the execute_graphql method
        mock_response = {
            "listIngestionSources": {
                "start": 0,
                "count": 2,
                "total": 2,
                "ingestionSources": [
                    {
                        "urn": "urn:li:dataHubIngestionSource:test1",
                        "type": "mysql",
                        "name": "Test MySQL Source",
                        "config": {
                            "recipe": '{"source": {"type": "mysql"}}',
                            "executorId": "test-executor",
                            "version": "0.8.0",
                            "debugMode": False,
                            "extraArgs": [],
                        },
                    },
                    {
                        "urn": "urn:li:dataHubIngestionSource:test2",
                        "type": "snowflake",
                        "name": "Test Snowflake Source",
                        "config": {
                            "recipe": '{"source": {"type": "snowflake"}}',
                            "executorId": "test-executor",
                            "version": "0.8.0",
                            "debugMode": False,
                            "extraArgs": [],
                        },
                    },
                ],
            }
        }
        self.mock_graph.execute_graphql.return_value = mock_response

        # Call the method
        result = self.provider.list_ingestion_sources()

        # Verify execute_graphql was called with correct parameters
        self.mock_graph.execute_graphql.assert_called_once()
        call_args = self.mock_graph.execute_graphql.call_args

        # Check the query
        self.assertIn("listIngestionSources", str(call_args.args[0]))
        self.assertIn("ListIngestionSourcesInput", str(call_args.args[0]))

        # Check the variables
        variables = call_args.kwargs["variables"]
        self.assertIn("input", variables)
        self.assertIn("start", variables["input"])
        self.assertIn("count", variables["input"])
        self.assertIn("filters", variables["input"])

        # Check the filter structure
        filters = variables["input"]["filters"]
        self.assertEqual(len(filters), 1)

        type_filter = filters[0]
        self.assertEqual(type_filter["field"], "type")
        self.assertEqual(type_filter["condition"], "EXISTS")

        # Check other parameters
        self.assertEqual(variables["input"]["start"], 0)
        self.assertEqual(variables["input"]["count"], INGESTION_SOURCES_BATCH_SIZE)

        # Verify the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["urn"], "urn:li:dataHubIngestionSource:test1")
        self.assertEqual(result[0]["type"], "mysql")
        self.assertEqual(result[1]["urn"], "urn:li:dataHubIngestionSource:test2")
        self.assertEqual(result[1]["type"], "snowflake")

    def test_list_ingestion_sources_handles_empty_results(self):
        """Test that list_ingestion_sources handles empty results correctly."""
        mock_response = {
            "listIngestionSources": {
                "start": 0,
                "count": 0,
                "total": 0,
                "ingestionSources": [],
            }
        }
        self.mock_graph.execute_graphql.return_value = mock_response

        result = self.provider.list_ingestion_sources()

        self.assertEqual(result, [])
        self.mock_graph.execute_graphql.assert_called_once()

    def test_list_ingestion_sources_handles_error_response(self):
        """Test that list_ingestion_sources handles error responses correctly."""
        mock_response = {"error": "GraphQL error occurred"}
        self.mock_graph.execute_graphql.return_value = mock_response

        result = self.provider.list_ingestion_sources()

        # Should return empty list on error
        self.assertEqual(result, [])
        self.mock_graph.execute_graphql.assert_called_once()

    def test_list_ingestion_sources_handles_missing_data(self):
        """Test that list_ingestion_sources handles missing data correctly."""
        mock_response = {
            "listIngestionSources": {
                # Missing ingestionSources key
                "start": 0,
                "count": 0,
                "total": 0,
            }
        }
        self.mock_graph.execute_graphql.return_value = mock_response

        result = self.provider.list_ingestion_sources()

        # Should return empty list when data is missing
        self.assertEqual(result, [])
        self.mock_graph.execute_graphql.assert_called_once()

    def test_list_ingestion_sources_handles_partial_data(self):
        """Test that list_ingestion_sources handles partial data correctly."""
        mock_response = {
            "listIngestionSources": {
                "start": 0,
                "count": 1,
                "total": 1,
                "ingestionSources": [
                    {
                        "urn": "urn:li:dataHubIngestionSource:test1",
                        "type": "mysql",
                        "name": "Test MySQL Source",
                        "config": {
                            "recipe": '{"source": {"type": "mysql"}}',
                            "executorId": "test-executor",
                            "version": "0.8.0",
                            "debugMode": False,
                            "extraArgs": [],
                        },
                    }
                ],
            }
        }
        self.mock_graph.execute_graphql.return_value = mock_response

        result = self.provider.list_ingestion_sources()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["urn"], "urn:li:dataHubIngestionSource:test1")

    def test_common_filter_is_always_applied(self):
        """Test that the common filter is always applied in list_ingestion_sources."""
        mock_response = {
            "listIngestionSources": {
                "start": 0,
                "count": 0,
                "total": 0,
                "ingestionSources": [],
            }
        }
        self.mock_graph.execute_graphql.return_value = mock_response

        # Call the method multiple times
        self.provider.list_ingestion_sources()
        self.provider.list_ingestion_sources()

        # Verify the filter is applied consistently
        calls = self.mock_graph.execute_graphql.call_args_list
        for call in calls:
            variables = call.kwargs["variables"]
            filters = variables["input"]["filters"]
            type_filter = filters[0]
            self.assertEqual(type_filter["field"], "type")
            self.assertEqual(type_filter["condition"], "EXISTS")

    def test_filter_structure_matches_graphql_schema(self):
        """Test that the filter structure matches the expected GraphQL schema."""
        mock_response = {
            "listIngestionSources": {
                "start": 0,
                "count": 0,
                "total": 0,
                "ingestionSources": [],
            }
        }
        self.mock_graph.execute_graphql.return_value = mock_response

        self.provider.list_ingestion_sources()

        # Verify the filter structure matches FacetFilterInput schema
        call_args = self.mock_graph.execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        filters = variables["input"]["filters"]

        # Should have exactly one filter
        self.assertEqual(len(filters), 1)

        type_filter = filters[0]

        # Check required fields
        self.assertIn("field", type_filter)
        self.assertIn("condition", type_filter)

        # Check field value
        self.assertEqual(type_filter["field"], "type")

        # Check condition value (EXISTS is a valid FilterOperator)
        self.assertEqual(type_filter["condition"], "EXISTS")

        # Should not have values field for EXISTS condition
        self.assertNotIn("values", type_filter)

        # Should not have negated field
        self.assertNotIn("negated", type_filter)

    def test_batch_size_constant_is_used(self):
        """Test that the correct batch size constant is used."""
        mock_response = {
            "listIngestionSources": {
                "start": 0,
                "count": 0,
                "total": 0,
                "ingestionSources": [],
            }
        }
        self.mock_graph.execute_graphql.return_value = mock_response

        self.provider.list_ingestion_sources()

        call_args = self.mock_graph.execute_graphql.call_args
        variables = call_args.kwargs["variables"]

        # Should use the constant for count
        self.assertEqual(variables["input"]["count"], INGESTION_SOURCES_BATCH_SIZE)
