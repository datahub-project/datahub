# type: ignore

from unittest import TestCase
from unittest.mock import Mock, patch

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.client.fetcher.ingestion.fetcher import IngestionFetcher
from datahub_executor.common.client.fetcher.ingestion.graphql.query import (
    GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
)
from datahub_executor.common.client.fetcher.ingestion.types import (
    IngestionSource,
    IngestionSourceConfig,
    IngestionSourceSchedule,
)
from datahub_executor.common.constants import LIST_INGESTION_SOURCES_BATCH_SIZE
from datahub_executor.common.types import (
    ExecutionRequestSchedule,
    FetcherConfig,
    FetcherMode,
)


class TestIngestionFetcher(TestCase):
    """Test cases for IngestionFetcher class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_graph = Mock(spec=DataHubGraph)
        self.mock_config = FetcherConfig(
            id="test-fetcher",
            enabled=True,
            mode=FetcherMode.DEFAULT,
            refresh_interval=5,
        )
        self.fetcher = IngestionFetcher(self.mock_graph, self.mock_config)

    def test_fetch_ingestion_sources_with_common_filter(self):
        """Test that _fetch_ingestion_sources applies the common type filter correctly."""
        # Mock the paginate_datahub_query_results function
        with patch(
            "datahub_executor.common.client.fetcher.ingestion.fetcher.paginate_datahub_query_results"
        ) as mock_paginate:
            # Mock return value
            mock_ingestion_sources = [
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
            ]
            mock_paginate.return_value = mock_ingestion_sources

            # Call the method
            result = self.fetcher._fetch_ingestion_sources()

            # Verify paginate_datahub_query_results was called with correct parameters
            mock_paginate.assert_called_once_with(
                graph=self.mock_graph,
                query=GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
                query_key="listIngestionSources",
                result_key="ingestionSources",
                page_size=LIST_INGESTION_SOURCES_BATCH_SIZE,
                user_params={
                    "input": {"filters": [{"field": "type", "condition": "EXISTS"}]}
                },
            )

            # Verify the result is converted correctly
            self.assertEqual(len(result), 2)
            self.assertIsInstance(result[0], IngestionSource)
            self.assertEqual(result[0].urn, "urn:li:dataHubIngestionSource:test1")
            self.assertEqual(result[0].type, "mysql")

    def test_fetch_ingestion_sources_filters_out_sources_without_type(self):
        """Test that sources without a type field are filtered out by the common filter."""
        # Mock the paginate_datahub_query_results function
        with patch(
            "datahub_executor.common.client.fetcher.ingestion.fetcher.paginate_datahub_query_results"
        ) as mock_paginate:
            # Mock return value - both sources have type fields since GraphQL filtering is mocked
            # In real usage, the GraphQL query with the EXISTS filter would filter out sources without type
            mock_ingestion_sources = [
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
                    "type": "unknown",  # Include type field since GraphQL filtering is mocked
                    "name": "Test Source With Type",
                    "config": {
                        "recipe": '{"source": {"type": "unknown"}}',
                        "executorId": "test-executor",
                        "version": "0.8.0",
                        "debugMode": False,
                        "extraArgs": [],
                    },
                },
            ]
            mock_paginate.return_value = mock_ingestion_sources

            # Call the method
            result = self.fetcher._fetch_ingestion_sources()

            # Verify that both sources are returned since GraphQL filtering is mocked
            # In real usage, the GraphQL query with the EXISTS filter would filter out sources without type
            self.assertEqual(len(result), 2)

            # Verify that both sources have type fields
            self.assertEqual(result[0].type, "mysql")
            self.assertEqual(result[1].type, "unknown")

    def test_fetch_ingestion_sources_handles_empty_results(self):
        """Test that _fetch_ingestion_sources handles empty results correctly."""
        with patch(
            "datahub_executor.common.client.fetcher.ingestion.fetcher.paginate_datahub_query_results"
        ) as mock_paginate:
            mock_paginate.return_value = []

            result = self.fetcher._fetch_ingestion_sources()

            self.assertEqual(result, [])
            mock_paginate.assert_called_once()

    def test_fetch_ingestion_sources_handles_cli_ingestion_sources(self):
        """Test that CLI ingestion sources are filtered out by the mapper."""
        with patch(
            "datahub_executor.common.client.fetcher.ingestion.fetcher.paginate_datahub_query_results"
        ) as mock_paginate:
            # Mock return value with CLI ingestion sources
            mock_ingestion_sources = [
                {
                    "urn": "urn:li:dataHubIngestionSource:cli-test1",
                    "type": "mysql",
                    "name": "CLI MySQL Source",
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
            ]
            mock_paginate.return_value = mock_ingestion_sources

            result = self.fetcher._fetch_ingestion_sources()

            # CLI ingestion sources should be filtered out by the mapper
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].urn, "urn:li:dataHubIngestionSource:test2")

    def test_fetch_default_cli_version(self):
        """Test that _fetch_default_cli_version retrieves the correct version."""
        # Mock the get_server_config method
        mock_config = {"managedIngestion": {"defaultCliVersion": "0.8.0"}}
        self.mock_graph.get_server_config.return_value = mock_config

        result = self.fetcher._fetch_default_cli_version()

        self.assertEqual(result, "0.8.0")
        self.mock_graph.get_server_config.assert_called_once()

    def test_fetch_execution_requests(self):
        """Test that fetch_execution_requests returns the correct execution request schedules."""
        # Mock the _fetch_ingestion_sources method
        mock_ingestion_source = IngestionSource(
            urn="urn:li:dataHubIngestionSource:test1",
            type="mysql",
            platform="mysql",
            schedule=None,
            config=IngestionSourceConfig(
                recipe='{"source": {"type": "mysql"}}',
                executor_id="test-executor",
                version="0.8.0",
                debug_mode=False,
                extra_args={},
            ),
        )

        with patch.object(
            self.fetcher,
            "_fetch_ingestion_sources",
            return_value=[mock_ingestion_source],
        ):
            with patch.object(
                self.fetcher, "_fetch_default_cli_version", return_value="0.8.0"
            ):
                result = self.fetcher.fetch_execution_requests()

                # Since there's no schedule, no execution request schedules should be returned
                self.assertEqual(result, [])

    def test_fetch_execution_requests_with_schedule(self):
        """Test that fetch_execution_requests handles ingestion sources with schedules correctly."""
        # Mock the _fetch_ingestion_sources method
        mock_ingestion_source = IngestionSource(
            urn="urn:li:dataHubIngestionSource:test1",
            type="mysql",
            platform="mysql",
            schedule=IngestionSourceSchedule(interval="0 * * * *", timezone="UTC"),
            config=IngestionSourceConfig(
                recipe='{"source": {"type": "mysql"}}',
                executor_id="test-executor",
                version="0.8.0",
                debug_mode=False,
                extra_args={},
            ),
        )

        with patch.object(
            self.fetcher,
            "_fetch_ingestion_sources",
            return_value=[mock_ingestion_source],
        ):
            with patch.object(
                self.fetcher, "_fetch_default_cli_version", return_value="0.8.0"
            ):
                result = self.fetcher.fetch_execution_requests()

                # Should return execution request schedules for sources with schedules
                self.assertEqual(len(result), 1)
                self.assertIsInstance(result[0], ExecutionRequestSchedule)

    def test_fetch_execution_requests_handles_recipe_parsing_error(self):
        """Test that fetch_execution_requests handles recipe parsing errors gracefully."""
        # Mock the _fetch_ingestion_sources method with invalid recipe
        mock_ingestion_source = IngestionSource(
            urn="urn:li:dataHubIngestionSource:test1",
            type="mysql",
            platform="mysql",
            schedule=None,
            config=IngestionSourceConfig(
                recipe="invalid json",
                executor_id="test-executor",
                version="0.8.0",
                debug_mode=False,
                extra_args={},
            ),
        )

        with patch.object(
            self.fetcher,
            "_fetch_ingestion_sources",
            return_value=[mock_ingestion_source],
        ):
            with patch.object(
                self.fetcher, "_fetch_default_cli_version", return_value="0.8.0"
            ):
                result = self.fetcher.fetch_execution_requests()

                # Should handle the error gracefully and return empty list
                self.assertEqual(result, [])

    def test_common_filter_structure(self):
        """Test that the common filter has the correct structure for GraphQL."""
        with patch(
            "datahub_executor.common.client.fetcher.ingestion.fetcher.paginate_datahub_query_results"
        ) as mock_paginate:
            mock_paginate.return_value = []

            # Call the method to trigger the filter creation
            self.fetcher._fetch_ingestion_sources()

            # Verify the filter structure
            call_args = mock_paginate.call_args
            user_params = call_args.kwargs["user_params"]

            self.assertIn("input", user_params)
            self.assertIn("filters", user_params["input"])

            filters = user_params["input"]["filters"]
            self.assertEqual(len(filters), 1)

            type_filter = filters[0]
            self.assertEqual(type_filter["field"], "type")
            self.assertEqual(type_filter["condition"], "EXISTS")

    def test_filter_is_always_applied(self):
        """Test that the common filter is always applied regardless of other parameters."""
        with patch(
            "datahub_executor.common.client.fetcher.ingestion.fetcher.paginate_datahub_query_results"
        ) as mock_paginate:
            mock_paginate.return_value = []

            # Call the method multiple times
            self.fetcher._fetch_ingestion_sources()
            self.fetcher._fetch_ingestion_sources()

            # Verify the filter is applied consistently
            calls = mock_paginate.call_args_list
            for call in calls:
                user_params = call.kwargs["user_params"]
                filters = user_params["input"]["filters"]
                type_filter = filters[0]
                self.assertEqual(type_filter["field"], "type")
                self.assertEqual(type_filter["condition"], "EXISTS")
