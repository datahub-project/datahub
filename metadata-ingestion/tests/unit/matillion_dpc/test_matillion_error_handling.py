"""Unit tests for MatillionSource error handling and edge cases."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from requests import HTTPError, Timeout
from requests.exceptions import ConnectionError
from requests.models import Response

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.matillion_dpc.config import MatillionSourceConfig
from datahub.ingestion.source.matillion_dpc.matillion import MatillionSource


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    return MatillionSourceConfig.model_validate(
        {
            "api_config": {
                "client_id": "test_client",
                "client_secret": "test_secret",
                "region": "US1",
            }
        }
    )


@pytest.fixture
def mock_context():
    """Create a mock pipeline context."""
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None
    ctx.pipeline_name = "test_pipeline"
    return ctx


@pytest.fixture
def matillion_source(mock_config, mock_context):
    """Create a Matillion source instance."""
    with patch("datahub.ingestion.source.matillion_dpc.matillion.MatillionAPIClient"):
        return MatillionSource(config=mock_config, ctx=mock_context)


class TestConnectionErrors:
    """Test connection error handling."""

    def test_test_connection_401_unauthorized(self, matillion_source):
        """Test handling of 401 authentication error."""
        with patch.object(
            matillion_source.api_client, "get_projects"
        ) as mock_get_projects:
            response = Mock(spec=Response)
            response.status_code = 401
            response.text = "Unauthorized"
            http_error = HTTPError(response=response)
            mock_get_projects.side_effect = http_error

            test_report = matillion_source.test_connection()

            assert test_report.basic_connectivity.capable is False
            assert (
                "Authentication failed" in test_report.basic_connectivity.failure_reason
            )
            assert (
                "OAuth2 client ID and client secret"
                in test_report.basic_connectivity.failure_reason
            )

    def test_test_connection_403_forbidden(self, matillion_source):
        """Test handling of 403 authorization error."""
        with patch.object(
            matillion_source.api_client, "get_projects"
        ) as mock_get_projects:
            response = Mock(spec=Response)
            response.status_code = 403
            response.text = "Forbidden"
            http_error = HTTPError(response=response)
            mock_get_projects.side_effect = http_error

            test_report = matillion_source.test_connection()

            assert test_report.basic_connectivity.capable is False
            assert (
                "Authorization failed" in test_report.basic_connectivity.failure_reason
            )
            assert "pipeline-execution" in test_report.basic_connectivity.failure_reason

    def test_test_connection_404_not_found(self, matillion_source):
        """Test handling of 404 endpoint not found error."""
        with patch.object(
            matillion_source.api_client, "get_projects"
        ) as mock_get_projects:
            response = Mock(spec=Response)
            response.status_code = 404
            response.text = "Not Found"
            http_error = HTTPError(response=response)
            mock_get_projects.side_effect = http_error

            test_report = matillion_source.test_connection()

            assert test_report.basic_connectivity.capable is False
            assert (
                "API endpoint not found"
                in test_report.basic_connectivity.failure_reason
            )
            assert "US1" in test_report.basic_connectivity.failure_reason

    def test_test_connection_500_server_error(self, matillion_source):
        """Test handling of generic HTTP error."""
        with patch.object(
            matillion_source.api_client, "get_projects"
        ) as mock_get_projects:
            response = Mock(spec=Response)
            response.status_code = 500
            response.text = "Internal Server Error"
            http_error = HTTPError(response=response)
            mock_get_projects.side_effect = http_error

            test_report = matillion_source.test_connection()

            assert test_report.basic_connectivity.capable is False
            assert "HTTP error 500" in test_report.basic_connectivity.failure_reason

    def test_test_connection_network_error(self, matillion_source):
        """Test handling of connection error."""
        with patch.object(
            matillion_source.api_client, "get_projects"
        ) as mock_get_projects:
            mock_get_projects.side_effect = ConnectionError("Network unreachable")

            test_report = matillion_source.test_connection()

            assert test_report.basic_connectivity.capable is False
            assert (
                "Network connection failed"
                in test_report.basic_connectivity.failure_reason
            )

    def test_test_connection_timeout(self, matillion_source):
        """Test handling of timeout error."""
        with patch.object(
            matillion_source.api_client, "get_projects"
        ) as mock_get_projects:
            mock_get_projects.side_effect = Timeout("Request timed out")

            test_report = matillion_source.test_connection()

            assert test_report.basic_connectivity.capable is False
            assert (
                "Network connection failed"
                in test_report.basic_connectivity.failure_reason
            )


class TestSQLAggregatorCreation:
    """Test SQL aggregator creation logic."""

    def test_sql_aggregator_no_platform(self, matillion_source):
        """Test SQL aggregator returns None when no platform specified."""
        result = matillion_source._get_sql_aggregator_for_platform("", None, "PROD")
        assert result is None

    def test_sql_aggregator_no_graph(self, matillion_source):
        """Test SQL aggregator returns None when no DataHub graph available."""
        matillion_source.ctx.graph = None
        result = matillion_source._get_sql_aggregator_for_platform(
            "snowflake", None, "PROD"
        )
        assert result is None

    def test_sql_aggregator_disabled_config(self, mock_config, mock_context):
        """Test SQL aggregator returns None when SQL parsing is disabled."""
        mock_config.parse_sql_for_lineage = False
        with patch(
            "datahub.ingestion.source.matillion_dpc.matillion.MatillionAPIClient"
        ):
            source = MatillionSource(config=mock_config, ctx=mock_context)
            result = source._get_sql_aggregator_for_platform("snowflake", None, "PROD")
            assert result is None

    def test_sql_aggregator_returns_cached(self, matillion_source):
        """Test SQL aggregator returns cached instance."""
        # Set graph and config to get past early returns
        matillion_source.ctx.graph = Mock()
        matillion_source.config.parse_sql_for_lineage = True

        # Simulate cached aggregator
        mock_aggregator = Mock()
        matillion_source._sql_aggregators["snowflake"] = mock_aggregator

        result = matillion_source._get_sql_aggregator_for_platform(
            "snowflake", None, "PROD"
        )
        assert result is mock_aggregator

    def test_sql_aggregator_creation_error_handled(self, matillion_source):
        """Test SQL aggregator creation error is handled gracefully."""
        # Provide a graph to get past early returns
        matillion_source.ctx.graph = Mock()
        matillion_source.config.parse_sql_for_lineage = True

        with patch(
            "datahub.ingestion.source.matillion_dpc.matillion.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator_class.side_effect = ValueError("Invalid platform")

            result = matillion_source._get_sql_aggregator_for_platform(
                "invalid_platform", None, "PROD"
            )

            # Should return None and cache None
            assert result is None
            assert matillion_source._sql_aggregators.get("invalid_platform") is None


class TestSQLParsingWithGraph:
    """Test SQL parsing logic with mocked DataHub graph."""

    def test_sql_aggregator_created_with_graph(self, mock_config, mock_context):
        """Test that SQL aggregator is created when graph is available."""
        # Mock the graph
        mock_graph = Mock()
        mock_context.graph = mock_graph

        with (
            patch(
                "datahub.ingestion.source.matillion_dpc.matillion.MatillionAPIClient"
            ),
            patch(
                "datahub.ingestion.source.matillion_dpc.matillion.SqlParsingAggregator"
            ) as mock_aggregator_class,
        ):
            mock_aggregator_instance = Mock()
            mock_aggregator_class.return_value = mock_aggregator_instance

            source = MatillionSource(config=mock_config, ctx=mock_context)
            source.config.parse_sql_for_lineage = True

            # Call to create aggregator
            result = source._get_sql_aggregator_for_platform(
                "snowflake", "prod_instance", "PROD"
            )

            # Verify SqlParsingAggregator was instantiated
            mock_aggregator_class.assert_called_once()
            call_kwargs = mock_aggregator_class.call_args.kwargs
            assert call_kwargs["platform"] == "snowflake"
            assert call_kwargs["platform_instance"] == "prod_instance"
            assert call_kwargs["env"] == "PROD"
            assert call_kwargs["graph"] == mock_graph
            assert call_kwargs["generate_lineage"] is True
            assert call_kwargs["generate_queries"] is False

            # Verify it's returned and cached
            assert result is mock_aggregator_instance
            assert source._sql_aggregators["snowflake"] is mock_aggregator_instance
