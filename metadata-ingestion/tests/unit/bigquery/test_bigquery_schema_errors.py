from unittest.mock import MagicMock, patch

from google.api_core.exceptions import BadRequest

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQuerySchemaApiPerfReport,
    BigQueryV2Report,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigQuerySchemaApi


class TestBigQuerySchemaErrorHandling:
    """Test cases for BigQuery schema error handling."""

    def test_skip_schema_errors_config_default(self):
        """Test that skip_schema_errors defaults to True."""
        config = BigQueryV2Config()
        assert config.skip_schema_errors is True

    def test_skip_schema_errors_config_can_be_disabled(self):
        """Test that skip_schema_errors can be set to False."""
        config = BigQueryV2Config(skip_schema_errors=False)
        assert config.skip_schema_errors is False

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQuerySchemaApi.get_query_result"
    )
    def test_get_columns_for_dataset_schema_error_handling_skip_true(
        self, mock_get_query_result
    ):
        """Test schema error handling in get_columns_for_dataset method with skip_schema_errors=True."""
        # Create a real BigQuerySchemaApi instance with mocked dependencies
        mock_client = MagicMock()
        mock_projects_client = MagicMock()
        mock_report = MagicMock(spec=BigQueryV2Report)

        api = BigQuerySchemaApi(
            report=BigQuerySchemaApiPerfReport(),
            client=mock_client,
            projects_client=mock_projects_client,
        )

        # Mock get_query_result to raise schema error
        schema_error = BadRequest("Table does not have a schema")
        mock_get_query_result.side_effect = schema_error

        result = api.get_columns_for_dataset(
            project_id="test-project",
            dataset_name="test_dataset",
            column_limit=1000,
            report=mock_report,
            skip_schema_errors=True,
        )

        # Should return None but log a warning
        assert result is None
        mock_report.warning.assert_called()
        warning_call = mock_report.warning.call_args
        assert "Dataset schema unavailable" in warning_call[1]["title"]

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQuerySchemaApi.get_query_result"
    )
    def test_get_columns_for_dataset_schema_error_handling_skip_false(
        self, mock_get_query_result
    ):
        """Test schema error handling in get_columns_for_dataset method with skip_schema_errors=False."""
        # Create a real BigQuerySchemaApi instance with mocked dependencies
        mock_client = MagicMock()
        mock_projects_client = MagicMock()
        mock_report = MagicMock(spec=BigQueryV2Report)

        api = BigQuerySchemaApi(
            report=BigQuerySchemaApiPerfReport(),
            client=mock_client,
            projects_client=mock_projects_client,
        )

        # Mock get_query_result to raise schema error
        schema_error = BadRequest("Table does not have a schema")
        mock_get_query_result.side_effect = schema_error

        result = api.get_columns_for_dataset(
            project_id="test-project",
            dataset_name="test_dataset",
            column_limit=1000,
            report=mock_report,
            skip_schema_errors=False,
        )

        # Should return None but log a failure
        assert result is None
        mock_report.failure.assert_called()
        failure_call = mock_report.failure.call_args
        assert "Dataset schema unavailable" in failure_call[1]["title"]
        assert "skip_schema_errors is False" in failure_call[1]["message"]

    def test_schema_error_detection(self):
        """Test that various schema error messages are correctly detected."""
        schema_error_messages = [
            "Table does not have a schema",
            "Table not found",
            "Dataset not found",
            "Invalid table name",
            "table credit-prod:Occupier.* does not have a schema because the latest matching table credit-prod:Occupier.CD_Invoices doesn't have a schema",
        ]

        for error_msg in schema_error_messages:
            error_str = error_msg.lower()
            is_schema_error = (
                "does not have a schema" in error_str
                or "table not found" in error_str
                or "dataset not found" in error_str
                or "invalid table name" in error_str
            )
            assert is_schema_error, f"Should detect '{error_msg}' as a schema error"

    def test_non_schema_error_detection(self):
        """Test that non-schema error messages are not detected as schema errors."""
        non_schema_error_messages = [
            "Permission denied: bigquery.datasets.get",
            "Quota exceeded",
            "Internal server error",
            "Authentication failed",
        ]

        for error_msg in non_schema_error_messages:
            error_str = error_msg.lower()
            is_schema_error = (
                "does not have a schema" in error_str
                or "table not found" in error_str
                or "dataset not found" in error_str
                or "invalid table name" in error_str
            )
            assert not is_schema_error, (
                f"Should not detect '{error_msg}' as a schema error"
            )

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQuerySchemaApi.get_query_result"
    )
    def test_non_schema_errors_still_cause_warnings(self, mock_get_query_result):
        """Test that non-schema errors still cause warnings regardless of skip_schema_errors setting."""
        # Create a real BigQuerySchemaApi instance with mocked dependencies
        mock_client = MagicMock()
        mock_projects_client = MagicMock()
        mock_report = MagicMock(spec=BigQueryV2Report)

        api = BigQuerySchemaApi(
            report=BigQuerySchemaApiPerfReport(),
            client=mock_client,
            projects_client=mock_projects_client,
        )

        # Mock get_query_result to raise a non-schema error
        permission_error = BadRequest("Permission denied: bigquery.datasets.get")
        mock_get_query_result.side_effect = permission_error

        result = api.get_columns_for_dataset(
            project_id="test-project",
            dataset_name="test_dataset",
            column_limit=1000,
            report=mock_report,
            skip_schema_errors=True,
        )

        # Should return None and log a warning (not failure)
        assert result is None
        mock_report.warning.assert_called()
        warning_call = mock_report.warning.call_args
        assert "Failed to retrieve columns for dataset" in warning_call[1]["title"]
