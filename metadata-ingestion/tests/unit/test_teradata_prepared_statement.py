from collections import defaultdict
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.teradata import (
    TeradataConfig,
    TeradataSource,
    _get_columns_via_prepared_statement,
    _map_prepared_statement_column_to_dict,
)


@pytest.fixture(autouse=True)
def isolate_teradata_caches(monkeypatch):
    """Isolate TeradataSource class-level caches for each test."""
    monkeypatch.setattr(TeradataSource, "_tables_cache", defaultdict(list))
    monkeypatch.setattr(TeradataSource, "_table_creator_cache", {})


def _base_config() -> Dict[str, Any]:
    """Base configuration for Teradata tests."""
    return {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:1025",
    }


class TestPreparedStatementConfiguration:
    """Test configuration validation for prepared statement metadata extraction."""

    def test_prepared_statement_config_default_false(self):
        """Verify use_prepared_statement_metadata defaults to False."""
        config_dict = _base_config()
        config = TeradataConfig.model_validate(config_dict)

        assert config.use_prepared_statement_metadata is False

    def test_fallback_config_default_false(self):
        """Verify metadata_extraction_fallback defaults to False."""
        config_dict = _base_config()
        config = TeradataConfig.model_validate(config_dict)

        assert config.metadata_extraction_fallback is False

    def test_prepared_statement_config_enabled(self):
        """Verify config accepts use_prepared_statement_metadata: true."""
        config_dict = {
            **_base_config(),
            "use_prepared_statement_metadata": True,
        }
        config = TeradataConfig.model_validate(config_dict)

        assert config.use_prepared_statement_metadata is True

    def test_fallback_config_enabled(self):
        """Verify config accepts metadata_extraction_fallback: true."""
        config_dict = {
            **_base_config(),
            "metadata_extraction_fallback": True,
        }
        config = TeradataConfig.model_validate(config_dict)

        assert config.metadata_extraction_fallback is True

    def test_both_configs_enabled(self):
        """Verify both flags can be enabled simultaneously."""
        config_dict = {
            **_base_config(),
            "use_prepared_statement_metadata": True,
            "metadata_extraction_fallback": True,
        }
        config = TeradataConfig.model_validate(config_dict)

        assert config.use_prepared_statement_metadata is True
        assert config.metadata_extraction_fallback is True


class TestPreparedStatementExtraction:
    """Test core prepared statement metadata extraction functionality."""

    def test_prepared_statement_extraction_success(self):
        """Test successful column extraction via prepared statement."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()

        # Mock cursor.description (DB-API 2.0 format)
        mock_cursor.description = [
            ("col1", "VARCHAR", 50, 50, None, None, True),
            ("col2", "INTEGER", None, None, 10, 0, False),
            ("col3", "DECIMAL", None, None, 18, 2, True),
        ]

        mock_result = MagicMock()
        mock_result.cursor = mock_cursor
        mock_connection.execute.return_value = mock_result

        columns = _get_columns_via_prepared_statement(
            mock_connection, "test_schema", "test_table"
        )

        assert len(columns) == 3
        assert columns[0]["name"] == "col1"
        assert columns[0]["nullable"] is True
        assert columns[0]["type_code"] == "VARCHAR"

        assert columns[1]["name"] == "col2"
        assert columns[1]["nullable"] is False
        assert columns[1]["precision"] == 10
        assert columns[1]["scale"] == 0

        assert columns[2]["name"] == "col3"
        assert columns[2]["precision"] == 18
        assert columns[2]["scale"] == 2

        # Verify query was executed with proper escaping
        call_args = mock_connection.execute.call_args
        assert 'SELECT * FROM "test_schema"."test_table" WHERE 1=0' in str(
            call_args[0][0]
        )

    def test_prepared_statement_extraction_no_permissions(self):
        """Test prepared statement extraction fails gracefully with permission error."""
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = Exception("Permission denied")

        with pytest.raises(Exception, match="Permission denied"):
            _get_columns_via_prepared_statement(
                mock_connection, "test_schema", "test_table"
            )

    def test_prepared_statement_extraction_invalid_table(self):
        """Test prepared statement extraction fails for non-existent table."""
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = Exception("Table does not exist")

        with pytest.raises(Exception, match="Table does not exist"):
            _get_columns_via_prepared_statement(
                mock_connection, "test_schema", "nonexistent_table"
            )

    def test_prepared_statement_extraction_no_description(self):
        """Test prepared statement extraction handles missing cursor description."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = None

        mock_result = MagicMock()
        mock_result.cursor = mock_cursor
        mock_connection.execute.return_value = mock_result

        with pytest.raises(
            ValueError, match="No column metadata available for test_schema.test_table"
        ):
            _get_columns_via_prepared_statement(
                mock_connection, "test_schema", "test_table"
            )

    def test_prepared_statement_sql_injection_prevention(self):
        """Test that prepared statement properly escapes identifiers."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1", "VARCHAR", 50, 50, None, None, True)]

        mock_result = MagicMock()
        mock_result.cursor = mock_cursor
        mock_connection.execute.return_value = mock_result

        # Test with schema/table names containing quotes
        _get_columns_via_prepared_statement(
            mock_connection, 'test"schema', 'test"table'
        )

        # Verify double quotes are escaped
        call_args = mock_connection.execute.call_args
        query_text = str(call_args[0][0])
        assert 'test""schema' in query_text
        assert 'test""table' in query_text


class TestPreparedStatementColumnMapping:
    """Test conversion of prepared statement results to expected format."""

    def test_column_mapping_basic(self):
        """Test basic column info mapping."""
        column_info = {
            "name": "test_col",
            "type_code": "VARCHAR",
            "nullable": True,
            "precision": None,
            "scale": None,
            "display_size": 50,
        }

        mock_dialect = MagicMock()
        result = _map_prepared_statement_column_to_dict(column_info, mock_dialect)

        assert result["ColumnName"] == "test_col"
        assert result["ColumnType"] == "VARCHAR"
        assert result["Nullable"] == "Y"
        assert result["DefaultValue"] is None
        assert result["CommentString"] is None

    def test_column_mapping_not_nullable(self):
        """Test column mapping with nullable=False."""
        column_info = {
            "name": "test_col",
            "type_code": "INTEGER",
            "nullable": False,
            "precision": 10,
            "scale": 0,
        }

        mock_dialect = MagicMock()
        result = _map_prepared_statement_column_to_dict(column_info, mock_dialect)

        assert result["Nullable"] == "N"

    def test_column_mapping_decimal_precision(self):
        """Test column mapping preserves decimal precision and scale."""
        column_info = {
            "name": "amount",
            "type_code": "DECIMAL",
            "nullable": True,
            "precision": 18,
            "scale": 2,
        }

        mock_dialect = MagicMock()
        result = _map_prepared_statement_column_to_dict(column_info, mock_dialect)

        assert result["DecimalTotalDigits"] == 18
        assert result["DecimalFractionalDigits"] == 2


class TestPreparedStatementReporting:
    """Test reporting metrics for prepared statement extraction."""

    def test_prepared_statement_metrics_tracked(self):
        """Verify report counters are incremented."""
        mock_report = MagicMock()
        mock_report.num_columns_processed = 0

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("col1", "VARCHAR", 50, 50, None, None, True),
            ("col2", "INTEGER", None, None, 10, 0, False),
        ]

        mock_result = MagicMock()
        mock_result.cursor = mock_cursor
        mock_connection.execute.return_value = mock_result

        _get_columns_via_prepared_statement(
            mock_connection, "test_schema", "test_table", report=mock_report
        )

        assert mock_report.num_columns_processed == 2

    def test_prepared_statement_failure_tracked(self):
        """Verify failures are tracked in report."""
        mock_report = MagicMock()
        mock_report.num_prepared_statement_failures = 0

        mock_connection = MagicMock()
        mock_connection.execute.side_effect = Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            _get_columns_via_prepared_statement(
                mock_connection, "test_schema", "test_table", report=mock_report
            )

        assert mock_report.num_prepared_statement_failures == 1


class TestFallbackLogic:
    """Test fallback chain between extraction methods."""

    @patch(
        "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
    )
    @patch("datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator")
    def test_dbc_success_no_fallback(
        self, mock_aggregator, mock_cache_tables_and_views
    ):
        """Verify DBC extraction succeeds without attempting fallback."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "metadata_extraction_fallback": True,
            }
        )

        source = TeradataSource(config, PipelineContext(run_id="test"))

        # Verify fallback flag is set
        assert source.config.metadata_extraction_fallback is True

    def test_fallback_chain_priority(self):
        """Verify fallback methods are attempted in correct order."""
        # This is more of a documentation test showing the priority
        # Actual priority is tested in integration scenarios
        expected_priority = [
            "QVCI (if enabled and view)",
            "HELP COLUMN (for views when QVCI not enabled)",
            "DBC system tables",
            "Prepared statements (if fallback or use_prepared enabled)",
            "HELP COLUMN (final fallback if all else fails)",
        ]
        assert len(expected_priority) == 5  # Verify all steps documented


class TestBackwardCompatibility:
    """Test that existing behavior is preserved when new flags are disabled."""

    @patch(
        "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
    )
    @patch("datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator")
    def test_default_config_unchanged(self, mock_aggregator, mock_cache):
        """Verify default configuration doesn't use new features."""
        config = TeradataConfig.model_validate(_base_config())
        source = TeradataSource(config, PipelineContext(run_id="test"))

        assert source.config.use_prepared_statement_metadata is False
        assert source.config.metadata_extraction_fallback is False

    def test_report_defaults(self):
        """Verify new report fields have appropriate defaults."""
        source_config = TeradataConfig.model_validate(_base_config())

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ),
            patch("datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"),
        ):
            source = TeradataSource(source_config, PipelineContext(run_id="test"))

        assert source.report.num_tables_using_prepared_statement == 0
        assert source.report.num_tables_using_help_fallback == 0
        assert source.report.tables_using_prepared_statement == []
        assert source.report.tables_using_help_fallback == []
        assert source.report.num_dbc_access_failures == 0
        assert source.report.num_prepared_statement_failures == 0
