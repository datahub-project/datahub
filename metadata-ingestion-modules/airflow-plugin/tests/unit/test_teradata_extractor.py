"""Unit tests for TeradataOperatorExtractor."""

import sys
from unittest import mock

from datahub_airflow_plugin._extractors import TeradataOperatorExtractor

mock_teradata_module = mock.MagicMock()
sys.modules["airflow.providers.teradata"] = mock_teradata_module
sys.modules["airflow.providers.teradata.operators"] = mock_teradata_module
sys.modules["airflow.providers.teradata.operators.teradata"] = mock_teradata_module


class TestTeradataOperatorExtractor:
    """Test suite for TeradataOperatorExtractor."""

    def test_extractor_reads_sql_field(self):
        """Test that extractor correctly reads operator.sql field."""
        operator = mock.Mock()
        operator.dag_id = "test_dag"
        operator.task_id = "test_task"
        operator.sql = "SELECT * FROM database.table"

        extractor = TeradataOperatorExtractor(operator)
        with mock.patch(
            "datahub_airflow_plugin._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            extractor.extract()

            mock_parse.assert_called_once()
            call_args = mock_parse.call_args
            assert call_args[0][1] == "SELECT * FROM database.table"

    def test_extractor_handles_missing_sql(self):
        """Test that extractor handles None/empty SQL gracefully."""
        operator = mock.Mock()
        operator.dag_id = "test_dag"
        operator.task_id = "test_task"
        operator.sql = None

        extractor = TeradataOperatorExtractor(operator)

        result = extractor.extract()

        assert result is None

    def test_extractor_uses_teradata_platform(self):
        """Test that extractor uses 'teradata' as platform."""
        operator = mock.Mock()
        operator.dag_id = "test_dag"
        operator.task_id = "test_task"
        operator.sql = "SELECT * FROM test_table"

        extractor = TeradataOperatorExtractor(operator)

        with mock.patch(
            "datahub_airflow_plugin._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            extractor.extract()

            call_kwargs = mock_parse.call_args[1]
            assert call_kwargs["platform"] == "teradata"

    def test_extractor_sets_none_defaults_for_two_tier_architecture(self):
        """Test that default_database and default_schema are None.

        Teradata uses 2-tier naming (database.table) not 3-tier.
        Setting defaults to None prevents incorrect URN generation.
        """
        operator = mock.Mock()
        operator.dag_id = "test_dag"
        operator.task_id = "test_task"
        operator.sql = "SELECT * FROM yellow_taxi.rides"

        extractor = TeradataOperatorExtractor(operator)

        with mock.patch(
            "datahub_airflow_plugin._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            extractor.extract()

            call_kwargs = mock_parse.call_args[1]
            assert call_kwargs["default_database"] is None
            assert call_kwargs["default_schema"] is None
