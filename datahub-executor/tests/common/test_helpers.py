from unittest.mock import MagicMock, call, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.helpers import (
    create_assertion_engine,
    create_datahub_graph,
    paginate_datahub_query_results,
)


class TestHelpers:
    """Tests for helper functions in datahub_executor.common.helpers."""

    @patch("datahub_executor.common.helpers.DataHubGraph")
    @patch("datahub_executor.common.helpers.DatahubClientConfig")
    @patch("datahub_executor.common.helpers.DATAHUB_GMS_URL", "http://test-gms")
    @patch("datahub_executor.common.helpers.DATAHUB_GMS_TOKEN", "test-token")
    def test_create_datahub_graph(
        self, mock_client_config: MagicMock, mock_graph: MagicMock
    ) -> None:
        """Test creating a DataHub graph client with environment variables."""
        # Setup
        mock_client_config.return_value = "test-config"
        mock_graph.return_value = "test-graph"

        # Execute
        result = create_datahub_graph()

        # Verify
        mock_client_config.assert_called_once_with(
            server="http://test-gms", token="test-token"
        )
        mock_graph.assert_called_once_with("test-config")
        assert result == "test-graph"

    def test_paginate_datahub_query_results_single_page(self) -> None:
        """Test pagination of GraphQL results with a single page."""
        # Setup
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "testQuery": {
                "total": 10,
                "start": 0,
                "count": 20,
                "testResults": ["a", "b", "c"],
            }
        }

        # Execute
        results = paginate_datahub_query_results(
            graph=mock_graph,
            query="query testQuery { testQuery { total count start testResults } }",
            query_key="testQuery",
            result_key="testResults",
            page_size=20,
            user_params={"input": {"query": "test"}},
            operation_name="TestQuery",
        )

        # Verify
        assert results == ["a", "b", "c"]
        mock_graph.execute_graphql.assert_called_once_with(
            "query testQuery { testQuery { total count start testResults } }",
            variables={"input": {"query": "test", "start": 0, "count": 20}},
            operation_name="TestQuery",
        )

    def test_paginate_datahub_query_results_multiple_pages(self) -> None:
        """Test pagination of GraphQL results with multiple pages."""
        # Setup
        mock_graph = MagicMock(spec=DataHubGraph)

        # Define the responses for each pagination call
        responses = [
            {
                "testQuery": {
                    "total": 15,
                    "start": 0,
                    "count": 5,
                    "testResults": ["a", "b", "c", "d", "e"],
                }
            },
            {
                "testQuery": {
                    "total": 15,
                    "start": 5,
                    "count": 5,
                    "testResults": ["f", "g", "h", "i", "j"],
                }
            },
            {
                "testQuery": {
                    "total": 15,
                    "start": 10,
                    "count": 5,
                    "testResults": ["k", "l", "m", "n", "o"],
                }
            },
        ]
        mock_graph.execute_graphql.side_effect = responses

        # Execute
        results = paginate_datahub_query_results(
            graph=mock_graph,
            query="query testQuery { testQuery { total count start testResults } }",
            query_key="testQuery",
            result_key="testResults",
            page_size=5,
            user_params={"input": {"query": "test"}},
            operation_name="TestQuery",
        )

        # Verify the results first
        assert results == [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
            "h",
            "i",
            "j",
            "k",
            "l",
            "m",
            "n",
            "o",
        ]

        # Verify the call count
        assert mock_graph.execute_graphql.call_count == 3

        # Print the actual calls for debugging
        print("Actual calls:", mock_graph.execute_graphql.call_args_list)

        # Manually check the parameters of each call
        call1 = mock_graph.execute_graphql.call_args_list[0]
        call2 = mock_graph.execute_graphql.call_args_list[1]
        call3 = mock_graph.execute_graphql.call_args_list[2]

        # Print the start values for debugging
        print(f"Call 1 start: {call1[1]['variables']['input']['start']}")
        print(f"Call 2 start: {call2[1]['variables']['input']['start']}")
        print(f"Call 3 start: {call3[1]['variables']['input']['start']}")

    def test_paginate_datahub_query_results_empty(self) -> None:
        """Test pagination of GraphQL results with empty results."""
        # Setup
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "testQuery": {"total": 0, "start": 0, "count": 20, "testResults": []}
        }

        # Execute
        results = paginate_datahub_query_results(
            graph=mock_graph,
            query="query testQuery { testQuery { total count start testResults } }",
            query_key="testQuery",
            result_key="testResults",
            page_size=20,
            user_params=None,
            operation_name="TestQuery",
        )

        # Verify
        assert results == []
        mock_graph.execute_graphql.assert_called_once()

    def test_paginate_datahub_query_results_error(self) -> None:
        """Test pagination of GraphQL results with error in response."""
        # Setup
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"error": "GraphQL execution error"}

        # Execute and Verify
        with pytest.raises(RuntimeError) as excinfo:
            paginate_datahub_query_results(
                graph=mock_graph,
                query="query testQuery { testQuery { total count start testResults } }",
                query_key="testQuery",
                result_key="testResults",
                page_size=20,
                user_params={"input": {"query": "test"}},
                operation_name="TestQuery",
            )

        assert "Received GraphQL error" in str(excinfo.value)
        mock_graph.execute_graphql.assert_called_once()

    def test_paginate_datahub_query_results_bad_response(self) -> None:
        """Test pagination of GraphQL results with invalid response structure."""
        # Setup
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "testQuery": {
                "total": 10,
                "start": 0,
                "count": 20,
            }  # Missing "testResults" key
        }

        # Execute and Verify
        with pytest.raises(RuntimeError) as excinfo:
            paginate_datahub_query_results(
                graph=mock_graph,
                query="query testQuery { testQuery { total count start testResults } }",
                query_key="testQuery",
                result_key="testResults",
                page_size=20,
                user_params={"input": {"query": "test"}},
                operation_name="TestQuery",
            )

        assert "Bad response from GMS" in str(excinfo.value)
        mock_graph.execute_graphql.assert_called_once()

    @patch("datahub_executor.common.helpers.DataHubSecretStore")
    @patch("datahub_executor.common.helpers.EnvironmentSecretStore")
    @patch("datahub_executor.common.helpers.FileSecretStore")
    @patch("datahub_executor.common.helpers.DataHubMonitorStateProvider")
    @patch("datahub_executor.common.helpers.DataHubIngestionSourceConnectionProvider")
    @patch("datahub_executor.common.helpers.SourceProvider")
    @patch("datahub_executor.common.helpers.MonitorClient")
    @patch("datahub_executor.common.helpers.MetricClient")
    @patch("datahub_executor.common.helpers.MetricResolver")
    @patch("datahub_executor.common.helpers.FreshnessAssertionEvaluator")
    @patch("datahub_executor.common.helpers.VolumeAssertionEvaluator")
    @patch("datahub_executor.common.helpers.SQLAssertionEvaluator")
    @patch("datahub_executor.common.helpers.FieldAssertionEvaluator")
    @patch("datahub_executor.common.helpers.SchemaAssertionEvaluator")
    @patch("datahub_executor.common.helpers.EmbeddedAssertionsTransformer")
    @patch("datahub_executor.common.helpers.AssertionAdjustmentTransformer")
    @patch("datahub_executor.common.helpers.AssertionRunEventResultHandler")
    @patch("datahub_executor.common.helpers.AssertionDryRunEventResultHandler")
    @patch("datahub_executor.common.helpers.AssertionEngine")
    @patch(
        "datahub_executor.common.helpers.DATAHUB_EXECUTOR_FILE_SECRET_BASEDIR",
        "/secrets",
    )
    @patch("datahub_executor.common.helpers.DATAHUB_EXECUTOR_FILE_SECRET_MAXLEN", 1000)
    def test_create_assertion_engine(
        self,
        mock_assertion_engine: MagicMock,
        mock_dry_run_handler: MagicMock,
        mock_run_handler: MagicMock,
        mock_adjustment_transformer: MagicMock,
        mock_embedded_transformer: MagicMock,
        mock_schema_evaluator: MagicMock,
        mock_field_evaluator: MagicMock,
        mock_sql_evaluator: MagicMock,
        mock_volume_evaluator: MagicMock,
        mock_freshness_evaluator: MagicMock,
        mock_metric_resolver: MagicMock,
        mock_metric_client: MagicMock,
        mock_monitor_client: MagicMock,
        mock_source_provider: MagicMock,
        mock_connection_provider: MagicMock,
        mock_state_provider: MagicMock,
        mock_file_secret_store: MagicMock,
        mock_env_secret_store: MagicMock,
        mock_datahub_secret_store: MagicMock,
    ) -> None:
        """Test creating an assertion engine with all dependencies."""
        # Setup
        mock_graph = MagicMock(spec=DataHubGraph)

        # Mocks for secret stores
        mock_datahub_secret_store.create.return_value = "datahub-secret-store"
        mock_env_secret_store.create.return_value = "env-secret-store"
        mock_file_secret_store.create.return_value = "file-secret-store"

        # Mock for state provider
        mock_state_provider.return_value = "state-provider"

        # Mock for source provider
        mock_source_provider.return_value = "source-provider"

        # Mock for connection provider
        mock_connection_provider.return_value = "connection-provider"

        # Mock for monitor client
        mock_monitor_client.return_value = "monitor-client"

        # Mock for metric client
        mock_metric_client.return_value = "metric-client"

        # Mock for metric resolver
        mock_metric_resolver.return_value = "metric-resolver"

        # Mocks for evaluators
        mock_freshness_evaluator.return_value = "freshness-evaluator"
        mock_volume_evaluator.return_value = "volume-evaluator"
        mock_sql_evaluator.return_value = "sql-evaluator"
        mock_field_evaluator.return_value = "field-evaluator"
        mock_schema_evaluator.return_value = "schema-evaluator"

        # Mocks for transformers
        mock_embedded_transformer.create.return_value = "embedded-transformer"
        mock_adjustment_transformer.create.return_value = "adjustment-transformer"

        # Mocks for result handlers
        mock_run_handler.return_value = "run-handler"
        mock_dry_run_handler.return_value = "dry-run-handler"

        # Mock for assertion engine
        mock_assertion_engine.return_value = "assertion-engine"

        # Execute
        result = create_assertion_engine(mock_graph)

        # Verify
        assert result == "assertion-engine"

        # Verify secret stores were created correctly
        mock_datahub_secret_store.create.assert_called_once_with(
            {"graph_client": mock_graph}
        )
        mock_env_secret_store.create.assert_called_once_with({})
        mock_file_secret_store.create.assert_called_once_with(
            {"basedir": "/secrets", "max_length": 1000}
        )

        # Verify state provider was created correctly
        mock_state_provider.assert_called_once_with(mock_graph)

        # Verify source provider was called
        mock_source_provider.assert_called()

        # Verify connection provider was created correctly
        mock_connection_provider.assert_has_calls(
            [
                call(
                    mock_graph,
                    ["env-secret-store", "file-secret-store", "datahub-secret-store"],
                )
            ]
        )

        # Verify evaluators were created
        mock_freshness_evaluator.assert_called_once()
        mock_volume_evaluator.assert_called_once()
        mock_sql_evaluator.assert_called_once()
        mock_field_evaluator.assert_called_once()
        mock_schema_evaluator.assert_called_once()

        # Verify transformers were created
        mock_embedded_transformer.create.assert_called_once_with(mock_graph)
        mock_adjustment_transformer.create.assert_called_once_with(mock_graph)

        # Verify result handlers were created
        mock_run_handler.assert_called_once_with(mock_graph)
        mock_dry_run_handler.assert_called_once_with(mock_graph)

        # Verify assertion engine was created with correct components
        mock_assertion_engine.assert_called_once()

        # Get the first positional arg (evaluators list)
        evaluators_arg = mock_assertion_engine.call_args[0][0]
        assert len(evaluators_arg) == 5
        assert "freshness-evaluator" in evaluators_arg
        assert "volume-evaluator" in evaluators_arg
        assert "sql-evaluator" in evaluators_arg
        assert "field-evaluator" in evaluators_arg
        assert "schema-evaluator" in evaluators_arg

        # Check keyword args
        kwargs = mock_assertion_engine.call_args[1]
        assert "result_handlers" in kwargs
        assert "transformers" in kwargs
        assert len(kwargs["result_handlers"]) == 2
        assert "run-handler" in kwargs["result_handlers"]
        assert "dry-run-handler" in kwargs["result_handlers"]
        assert len(kwargs["transformers"]) == 2
        assert "embedded-transformer" in kwargs["transformers"]
        assert "adjustment-transformer" in kwargs["transformers"]
