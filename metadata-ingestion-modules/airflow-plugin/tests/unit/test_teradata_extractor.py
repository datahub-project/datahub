"""Unit tests for TeradataOperatorExtractor."""

import sys
from unittest import mock

import packaging.version
import pytest

from datahub_airflow_plugin._airflow_shims import AIRFLOW_VERSION

# Skip all tests for Airflow 3.x since airflow2 extractors are not used
if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
    pytestmark = pytest.mark.skip(
        reason="TeradataOperatorExtractor is only used in Airflow 2.x, not Airflow 3.x"
    )
    # Define dummy classes to avoid import errors
    ExtractorManager = None
    TeradataOperatorExtractor = None
    USE_OPENLINEAGE_PROVIDER = False
else:
    try:
        from datahub_airflow_plugin.airflow2._extractors import (
            ExtractorManager,
            TeradataOperatorExtractor,
        )
        from datahub_airflow_plugin.airflow2._openlineage_compat import (
            USE_OPENLINEAGE_PROVIDER,
        )

        mock_teradata_module = mock.MagicMock()
        sys.modules["airflow.providers.teradata"] = mock_teradata_module
        sys.modules["airflow.providers.teradata.operators"] = mock_teradata_module
        sys.modules["airflow.providers.teradata.operators.teradata"] = (
            mock_teradata_module
        )

        # Skip all tests if USE_OPENLINEAGE_PROVIDER is True, since TeradataOperatorExtractor
        # is only registered and used in Legacy OpenLineage environments
        pytestmark = pytest.mark.skipif(
            USE_OPENLINEAGE_PROVIDER,
            reason="TeradataOperatorExtractor is only used with Legacy OpenLineage, not OpenLineage Provider",
        )
    except (ImportError, ModuleNotFoundError):
        # If imports fail (e.g., openlineage not installed), skip all tests
        pytestmark = pytest.mark.skip(
            reason="airflow2 extractors not available (likely missing openlineage dependency)"
        )
        ExtractorManager = None
        TeradataOperatorExtractor = None
        USE_OPENLINEAGE_PROVIDER = False


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
            "datahub_airflow_plugin.airflow2._extractors._parse_sql_into_task_metadata"
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
            "datahub_airflow_plugin.airflow2._extractors._parse_sql_into_task_metadata"
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
            "datahub_airflow_plugin.airflow2._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            extractor.extract()

            call_kwargs = mock_parse.call_args[1]
            assert call_kwargs["default_database"] is None
            assert call_kwargs["default_schema"] is None

    def test_extractor_passes_self_as_first_arg(self):
        """Test that extractor passes itself to _parse_sql_into_task_metadata."""
        operator = mock.Mock()
        operator.dag_id = "test_dag"
        operator.task_id = "test_task"
        operator.sql = "INSERT INTO dest SELECT * FROM src"

        extractor = TeradataOperatorExtractor(operator)

        with mock.patch(
            "datahub_airflow_plugin.airflow2._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            extractor.extract()

            call_args = mock_parse.call_args[0]
            assert call_args[0] is extractor

    def test_extractor_handles_multiline_sql(self):
        """Test that extractor handles multiline SQL statements."""
        multiline_sql = """
        CREATE TABLE yellow_taxi.staging AS
        SELECT
            pickup_datetime,
            passenger_count,
            trip_distance
        FROM yellow_taxi.raw_rides
        WHERE trip_distance > 0
        """
        operator = mock.Mock()
        operator.dag_id = "etl_dag"
        operator.task_id = "transform_data"
        operator.sql = multiline_sql

        extractor = TeradataOperatorExtractor(operator)

        with mock.patch(
            "datahub_airflow_plugin.airflow2._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            extractor.extract()

            call_args = mock_parse.call_args
            assert call_args[0][1] == multiline_sql
            assert call_args[1]["platform"] == "teradata"

    def test_extractor_handles_empty_string_sql(self):
        """Test that extractor treats empty string SQL like None."""
        operator = mock.Mock()
        operator.dag_id = "test_dag"
        operator.task_id = "test_task"
        operator.sql = ""

        extractor = TeradataOperatorExtractor(operator)
        result = extractor.extract()

        # Empty string is falsy, should return None
        assert result is None

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM db1.table1",
            "INSERT INTO db2.table2 SELECT * FROM db1.table1",
            "CREATE TABLE db.new_table AS SELECT * FROM db.old_table",
            "UPDATE db.table SET col = 'value' WHERE id = 1",
            "DELETE FROM db.table WHERE status = 'archived'",
        ],
    )
    def test_extractor_handles_various_sql_statements(self, sql):
        """Test that extractor handles various SQL statement types."""
        operator = mock.Mock()
        operator.dag_id = "test_dag"
        operator.task_id = "test_task"
        operator.sql = sql

        extractor = TeradataOperatorExtractor(operator)

        with mock.patch(
            "datahub_airflow_plugin.airflow2._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            mock_parse.return_value = mock.Mock()  # Non-None result
            result = extractor.extract()

            assert result is not None
            assert mock_parse.call_args[0][1] == sql

    def test_extractor_registered_in_manager(self):
        """Test that TeradataOperator is registered with TeradataOperatorExtractor."""
        manager = ExtractorManager()

        assert "TeradataOperator" in manager.task_to_extractor.extractors
        assert (
            manager.task_to_extractor.extractors["TeradataOperator"]
            == TeradataOperatorExtractor
        )

    def test_extractor_follows_athena_pattern(self):
        """Test that TeradataOperatorExtractor follows same pattern as AthenaOperatorExtractor.

        Both handle databases with non-standard naming conventions:
        - Athena: catalog.database.table (3-tier but different from schema-based DBs)
        - Teradata: database.table (2-tier, no schema concept)
        """
        from datahub_airflow_plugin.airflow2._extractors import AthenaOperatorExtractor

        assert hasattr(TeradataOperatorExtractor, "extract")
        assert hasattr(AthenaOperatorExtractor, "extract")

        teradata_op = mock.Mock()
        teradata_op.dag_id = "dag"
        teradata_op.task_id = "task"
        teradata_op.sql = "SELECT 1"

        teradata_extractor = TeradataOperatorExtractor(teradata_op)

        with mock.patch(
            "datahub_airflow_plugin.airflow2._extractors._parse_sql_into_task_metadata"
        ) as mock_parse:
            teradata_extractor.extract()
            assert mock_parse.called
