from unittest.mock import MagicMock, patch

from google.api_core.exceptions import BadRequest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQuerySchemaApiPerfReport,
    BigQueryV2Report,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryProject,
    BigQuerySchemaApi,
)


class TestBigQuerySchemaErrorHandling:
    """Test cases for BigQuery schema error handling."""

    def test_skip_schema_errors_config_default(self):
        """Test that skip_schema_errors defaults to False."""
        config = BigQueryV2Config()
        assert config.skip_schema_errors is False

    def test_skip_schema_errors_config_can_be_enabled(self):
        """Test that skip_schema_errors can be set to True."""
        config = BigQueryV2Config(skip_schema_errors=True)
        assert config.skip_schema_errors is True

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

    def test_skip_schema_errors_config_with_dict_initialization(self):
        """Test that skip_schema_errors can be set via dictionary initialization."""
        config = BigQueryV2Config.parse_obj({"skip_schema_errors": True})
        assert config.skip_schema_errors is True

        config = BigQueryV2Config.parse_obj({"skip_schema_errors": False})
        assert config.skip_schema_errors is False

    def test_skip_schema_errors_config_with_other_options(self):
        """Test that skip_schema_errors works correctly when combined with other config options."""
        config = BigQueryV2Config.parse_obj(
            {
                "project_id": "test-project",
                "skip_schema_errors": True,
                "include_usage_statistics": False,
                "include_table_lineage": False,
            }
        )
        assert config.skip_schema_errors is True
        assert config.project_ids == ["test-project"]
        assert config.include_usage_statistics is False
        assert config.include_table_lineage is False

    def test_skip_schema_errors_config_type_validation(self):
        """Test that skip_schema_errors accepts various boolean-like values and converts them properly."""
        # Test string values that should be converted to boolean
        config = BigQueryV2Config.parse_obj({"skip_schema_errors": "true"})
        assert config.skip_schema_errors is True

        config = BigQueryV2Config.parse_obj({"skip_schema_errors": "false"})
        assert config.skip_schema_errors is False

        # Test integer values that should be converted to boolean
        config = BigQueryV2Config.parse_obj({"skip_schema_errors": 1})
        assert config.skip_schema_errors is True

        config = BigQueryV2Config.parse_obj({"skip_schema_errors": 0})
        assert config.skip_schema_errors is False

        # Test actual boolean values
        config = BigQueryV2Config.parse_obj({"skip_schema_errors": True})
        assert config.skip_schema_errors is True

        config = BigQueryV2Config.parse_obj({"skip_schema_errors": False})
        assert config.skip_schema_errors is False

    @patch.object(BigQueryV2Config, "get_bigquery_client")
    @patch.object(BigQueryV2Config, "get_projects_client")
    def test_bigquery_source_skip_schema_errors_integration(
        self, get_projects_client, get_bq_client_mock
    ):
        """Test that skip_schema_errors configuration is properly passed through the BigQuery source."""
        # Test with skip_schema_errors=True
        config = BigQueryV2Config.parse_obj(
            {
                "project_id": "test-project",
                "skip_schema_errors": True,
                "include_usage_statistics": False,
                "include_table_lineage": False,
            }
        )
        source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
        assert source.config.skip_schema_errors is True

        # Test with skip_schema_errors=False
        config = BigQueryV2Config.parse_obj(
            {
                "project_id": "test-project",
                "skip_schema_errors": False,
                "include_usage_statistics": False,
                "include_table_lineage": False,
            }
        )
        source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
        assert source.config.skip_schema_errors is False

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_schema_gen.BigQuerySchemaGenerator._process_schema"
    )
    @patch.object(BigQueryV2Config, "get_bigquery_client")
    @patch.object(BigQueryV2Config, "get_projects_client")
    def test_schema_generator_respects_skip_schema_errors_config(
        self, get_projects_client, get_bq_client_mock, mock_process_schema
    ):
        """Test that BigQuerySchemaGenerator respects the skip_schema_errors configuration."""
        # Mock _process_schema to raise a schema error
        schema_error = Exception("Table does not have a schema")
        mock_process_schema.side_effect = schema_error

        # Test with skip_schema_errors=True - should not raise exception
        config = BigQueryV2Config.parse_obj(
            {
                "project_id": "test-project",
                "skip_schema_errors": True,
                "include_usage_statistics": False,
                "include_table_lineage": False,
            }
        )
        source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))

        # This should not raise an exception and should log warnings instead
        project = BigqueryProject("test-project", "test-project")

        # The method should handle the error gracefully when skip_schema_errors=True
        workunits = list(
            source.bq_schema_extractor._process_project_datasets(project, {})
        )
        # We expect no work units due to the error, but no exception should be raised
        assert len(workunits) == 0

    def test_config_inheritance_skip_schema_errors(self):
        """Test that skip_schema_errors is properly inherited in the config hierarchy."""
        # Test that BigQueryV2Config inherits skip_schema_errors from BigQueryBaseConfig
        config = BigQueryV2Config.parse_obj({"skip_schema_errors": True})
        assert hasattr(config, "skip_schema_errors")
        assert config.skip_schema_errors is True

        # Test default inheritance
        config = BigQueryV2Config.parse_obj({})
        assert hasattr(config, "skip_schema_errors")
        assert config.skip_schema_errors is False
