import logging
import re
import time
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from threading import current_thread
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import (
    DatabaseError,
    OperationalError,
    TimeoutError as PoolTimeoutError,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import (
    MetadataChangeProposalWrapper,
    MetadataWorkUnit,
)
from datahub.ingestion.source.sql.teradata import (
    TeradataConfig,
    TeradataReport,
    TeradataSource,
    TeradataTable,
    _categorize_view_error,
    _engine_connect_with_retry,
    _execute_with_retry,
    _fetchmany_with_retry,
    _jittered_backoff,
    _should_retry,
    _should_retry_connect,
    get_schema_columns,
    get_schema_foreign_keys,
    get_schema_pk_constraints,
    optimized_get_columns,
    optimized_get_view_definition,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery


@pytest.fixture(autouse=True)
def isolate_teradata_caches(monkeypatch):
    """Isolate TeradataSource class-level caches for each test.

    Creates fresh cache instances before each test to prevent cross-contamination.
    """
    monkeypatch.setattr(TeradataSource, "_tables_cache", defaultdict(list))
    monkeypatch.setattr(TeradataSource, "_table_creator_cache", {})


def _base_config() -> Dict[str, Any]:
    """Base configuration for Teradata tests."""
    return {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:1025",
        "include_table_lineage": True,
        "include_usage_statistics": True,
        "include_queries": True,
    }


def _create_mock_table_entry(database, table, creator_name=None, **kwargs):
    """Helper to create mock table entries with consistent structure."""
    mock_entry = MagicMock()
    mock_entry.DataBaseName = MagicMock(strip=MagicMock(return_value=database))
    mock_entry.name = MagicMock(strip=MagicMock(return_value=table))
    mock_entry.description = kwargs.get("description")
    mock_entry.object_type = kwargs.get("object_type", "Table")
    mock_entry.CreateTimeStamp = kwargs.get("create_time", datetime(2024, 1, 1))
    mock_entry.LastAlterName = kwargs.get("alter_name")
    mock_entry.LastAlterTimeStamp = kwargs.get("alter_time")
    mock_entry.RequestText = kwargs.get("request_text")
    mock_entry.CreatorName = (
        MagicMock(strip=MagicMock(return_value=creator_name)) if creator_name else None
    )

    return mock_entry


def _mock_execute_result(rows: list) -> MagicMock:
    """Wrap *rows* in a mock that supports .fetchmany() as cache_tables_and_views expects.

    The first call returns all rows; subsequent calls return [] to signal end-of-results,
    matching the while-True / break loop in cache_tables_and_views.
    """
    result = MagicMock()
    result.fetchmany.side_effect = [rows, []]
    return result


def _create_source(extract_ownership=False):
    """Helper to create TeradataSource with mocked dependencies."""
    config = TeradataConfig.model_validate(
        {
            **_base_config(),
            "extract_ownership": extract_ownership,
        }
    )

    with patch(
        "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
    ) as mock_class:
        mock_class.return_value = MagicMock()
        with patch(
            "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
        ):
            return TeradataSource(config, PipelineContext(run_id="test"))


class TestTeradataConfig:
    """Test configuration validation and initialization."""

    def test_valid_config(self):
        """Test that valid configuration is accepted."""
        config_dict = _base_config()
        config = TeradataConfig.model_validate(config_dict)

        assert config.host_port == "localhost:1025"

    def test_max_workers_validation_valid(self):
        """Test valid max_workers configuration passes validation."""
        config_dict = {
            **_base_config(),
            "max_workers": 8,
        }
        config = TeradataConfig.model_validate(config_dict)
        assert config.max_workers == 8

    def test_hang_protection_can_be_disabled(self):
        """All hang-protection knobs accept 0 to disable."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "view_processing_timeout_seconds": 0,
                "view_processing_heartbeat_seconds": 0,
                "lineage_fetch_stall_warning_seconds": 0,
            }
        )
        assert config.view_processing_timeout_seconds == 0
        assert config.view_processing_heartbeat_seconds == 0
        assert config.lineage_fetch_stall_warning_seconds == 0

    def test_time_window_defaults_applied(self):
        """Test that BaseTimeWindowConfig defaults are automatically applied."""
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
        }

        config = TeradataConfig.model_validate(config_dict)

        assert config.start_time is not None
        assert config.end_time is not None
        assert isinstance(config.start_time, datetime)
        assert isinstance(config.end_time, datetime)
        assert config.end_time > config.start_time

    def test_incremental_lineage_config_support(self):
        """Test that incremental_lineage configuration parameter is supported."""
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "incremental_lineage": True,
        }

        config = TeradataConfig.model_validate(config_dict)

        assert config.incremental_lineage is True

        config_dict_false = {
            **_base_config(),
            "incremental_lineage": False,
        }
        config_false = TeradataConfig.model_validate(config_dict_false)
        assert config_false.incremental_lineage is False

    def test_user_original_recipe_compatibility(self):
        """Test that a user's original recipe configuration is parsed correctly."""
        user_recipe_config = {
            "host_port": "vmvantage1720:1025",
            "username": "dbc",
            "password": "dbc",
            "include_table_lineage": True,
            "include_usage_statistics": True,
            "incremental_lineage": True,
            "stateful_ingestion": {"enabled": True, "fail_safe_threshold": 90},
        }

        config = TeradataConfig.model_validate(user_recipe_config)

        assert config.host_port == "vmvantage1720:1025"
        assert config.username == "dbc"
        assert config.include_table_lineage is True
        assert config.include_usage_statistics is True
        assert config.incremental_lineage is True
        assert config.start_time is not None
        assert config.end_time is not None
        assert config.start_time < config.end_time
        assert config.stateful_ingestion is not None
        assert config.stateful_ingestion.enabled is True
        assert config.stateful_ingestion.fail_safe_threshold == 90

    @pytest.mark.parametrize(
        "override, expected",
        [
            ({}, False),
            ({"extract_ownership": True}, True),
            ({"extract_ownership": False}, False),
        ],
    )
    def test_extract_ownership_config(
        self, override: Dict[str, Any], expected: bool
    ) -> None:
        config_dict = {**_base_config(), **override}
        config = TeradataConfig.model_validate(config_dict)
        assert config.extract_ownership is expected


class TestTeradataSource:
    """Test Teradata source functionality."""

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    def test_source_initialization(self, mock_create_engine):
        """Test source initializes correctly."""
        config = TeradataConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test")

        # Mock the engine creation
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, ctx)

            assert source.config == config
            assert source.platform == "teradata"
            assert hasattr(source, "aggregator")
            assert hasattr(source, "_tables_cache")
            assert hasattr(source, "_tables_cache_lock")

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    @patch("datahub.ingestion.source.sql.teradata.inspect")
    def test_get_inspectors(self, mock_inspect, mock_create_engine):
        """Test inspector creation and database iteration."""
        # Mock database names returned by inspector
        mock_inspector = MagicMock()
        mock_inspector.get_schema_names.return_value = ["db1", "db2", "test_db"]
        mock_inspect.return_value = mock_inspector

        mock_connection = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(source, "get_metadata_engine", return_value=mock_engine):
                inspectors = list(source.get_inspectors())

                assert len(inspectors) == 3
                # Check that each inspector has the database name set
                for inspector in inspectors:
                    assert hasattr(inspector, "_datahub_database")

    def test_cache_tables_and_views_thread_safety(self):
        """Test that cache operations are thread-safe."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Use helper function for consistent mocking
            mock_entry = _create_mock_table_entry(
                "test_db", "test_table", description="Test table"
            )

            with patch.object(source, "get_metadata_engine") as mock_get_engine:
                mock_conn = MagicMock()
                mock_conn.execute.return_value = _mock_execute_result([mock_entry])
                mock_engine = MagicMock()
                mock_engine.connect.return_value = mock_conn
                mock_get_engine.return_value = mock_engine

                # Call the method after patching the engine
                source.cache_tables_and_views()

                # Verify table was added to cache
                assert "test_db" in source._tables_cache
                assert len(source._tables_cache["test_db"]) == 1
                assert source._tables_cache["test_db"][0].name == "test_table"

                # Verify engine was disposed
                mock_engine.dispose.assert_called_once()

    def test_convert_entry_to_observed_query(self):
        """Test conversion of database entries to ObservedQuery objects."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock database entry
            mock_entry = MagicMock()
            mock_entry.query_text = "SELECT * FROM table1 (NOT CASESPECIFIC)"
            mock_entry.session_id = "session123"
            mock_entry.timestamp = "2024-01-01 10:00:00"
            mock_entry.user = "test_user"
            mock_entry.default_database = "test_db"

            observed_query = source._convert_entry_to_observed_query(mock_entry)

            assert isinstance(observed_query, ObservedQuery)
            assert (
                observed_query.query == "SELECT * FROM table1 "
            )  # (NOT CASESPECIFIC) removed
            assert observed_query.session_id == "session123"
            assert observed_query.timestamp == "2024-01-01 10:00:00"
            assert isinstance(observed_query.user, CorpUserUrn)
            assert (
                observed_query.default_db is None
            )  # Fixed for Teradata two-tier architecture
            assert observed_query.default_schema == "test_db"

    def test_convert_entry_to_observed_query_with_none_user(self):
        """Test ObservedQuery conversion handles None user correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            mock_entry = MagicMock()
            mock_entry.query_text = "SELECT 1"
            mock_entry.session_id = "session123"
            mock_entry.timestamp = "2024-01-01 10:00:00"
            mock_entry.user = None
            mock_entry.default_database = "test_db"

            observed_query = source._convert_entry_to_observed_query(mock_entry)

            assert observed_query.user is None

    def test_check_historical_table_exists_success(self):
        """Test historical table check when table exists."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock successful query execution
            mock_connection = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value = mock_connection

            with patch.object(source, "get_metadata_engine", return_value=mock_engine):
                result = source._check_historical_table_exists()

                assert result is True
                mock_engine.dispose.assert_called_once()

    def test_check_historical_table_exists_failure(self):
        """Test historical table check when table doesn't exist."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock failed query execution
            mock_connection = MagicMock()
            mock_connection.execute.side_effect = Exception("Table not found")
            mock_engine = MagicMock()
            mock_engine.connect.return_value = mock_connection

            with patch.object(source, "get_metadata_engine", return_value=mock_engine):
                result = source._check_historical_table_exists()

                assert result is False
                mock_engine.dispose.assert_called_once()

    def test_close_cleanup(self):
        """Test that close() properly cleans up resources."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            # Pre-populate class-level caches to verify they are cleared on close
            source._tables_cache["db1"] = [
                TeradataTable(
                    database="db1",
                    name="t1",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime(2024, 1, 1),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
            source._table_creator_cache[("db1", "t1")] = "owner"

            with patch(
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.close"
            ) as mock_super_close:
                source.close()

                mock_aggregator.close.assert_called_once()
                mock_super_close.assert_called_once()

                # Class-level caches must be emptied so memory is released between
                # sequential recipe runs in the same process (OOM fix for #7602).
                assert len(source._tables_cache) == 0
                assert len(source._table_creator_cache) == 0

                # Module-level LRU caches must also be cleared between recipe runs.
                assert get_schema_columns.cache_info().currsize == 0
                assert get_schema_pk_constraints.cache_info().currsize == 0
                assert get_schema_foreign_keys.cache_info().currsize == 0

    def test_make_lineage_queries_with_time_defaults(self):
        """Test that _make_lineage_queries works with automatic time defaults."""
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:1025",
            "include_table_lineage": True,
            "include_usage_statistics": True,
        }

        config = TeradataConfig.model_validate(config_dict)

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=False
            ):
                queries = source._make_lineage_queries()

            assert len(queries) > 0
            query = queries[0]
            assert "TIMESTAMP" in query
            assert "None" not in query

            timestamp_pattern = r"TIMESTAMP '[^']+'"
            matches = re.findall(timestamp_pattern, query)
            assert len(matches) >= 2


class TestSQLInjectionSafety:
    """Test SQL injection vulnerability fixes."""

    def test_get_schema_columns_parameterized(self):
        """Test that get_schema_columns uses parameterized queries."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        # Call the function
        get_schema_columns(None, mock_connection, "columnsV", "test_schema")

        # Verify parameterized query was used
        call_args = mock_connection.execute.call_args
        query = call_args[0][0].text
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]

        assert ":schema" in query
        assert "schema" in params
        assert params["schema"] == "test_schema"

    def test_get_schema_pk_constraints_parameterized(self):
        """Test that get_schema_pk_constraints uses parameterized queries."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        # Call the function
        get_schema_pk_constraints(None, mock_connection, "test_schema")

        # Verify parameterized query was used
        call_args = mock_connection.execute.call_args
        query = call_args[0][0].text
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]

        assert ":schema" in query
        assert "schema" in params
        assert params["schema"] == "test_schema"

    def test_get_schema_foreign_keys_parameterized(self):
        """Test that get_schema_foreign_keys uses parameterized queries."""
        get_schema_foreign_keys.cache_clear()
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchall.return_value = []

        get_schema_foreign_keys(None, mock_connection, "test_schema")

        call_args = mock_connection.execute.call_args
        query = call_args[0][0].text
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]

        assert ":schema" in query
        assert "schema" in params
        assert params["schema"] == "test_schema"


class TestSchemaFunctionRetry:
    """get_schema_columns/pk_constraints/foreign_keys retry on transient DB errors."""

    def _make_conn(self, side_effects, fetchall_result=None):
        """Return a mock Connection whose execute() yields *side_effects* in order."""
        mock_conn = MagicMock()
        if fetchall_result is None:
            fetchall_result = []
        good_result = MagicMock()
        good_result.fetchall.return_value = fetchall_result
        # side_effects is a list: exceptions are raised, non-exceptions are returned
        mock_conn.execute.side_effect = [
            exc if isinstance(exc, Exception) else good_result for exc in side_effects
        ]
        return mock_conn

    def test_get_schema_columns_retries_transient_error(self):
        """A single transient failure is retried and the successful result is returned."""
        get_schema_columns.cache_clear()
        mock_conn = self._make_conn(
            [DatabaseError("transaction aborted", None, None), None]
        )

        with patch("time.sleep"):
            result = get_schema_columns(None, mock_conn, "columnsV", "db1")

        assert mock_conn.execute.call_count == 2
        assert result == {}

    def test_get_schema_columns_non_retryable_error_propagates(self):
        """A non-retryable error (syntax error) propagates immediately."""
        get_schema_columns.cache_clear()
        mock_conn = self._make_conn([DatabaseError("syntax error", None, None)])

        with patch("time.sleep"), pytest.raises(DatabaseError):
            get_schema_columns(None, mock_conn, "columnsV", "db1")

        assert mock_conn.execute.call_count == 1

    def test_get_schema_pk_constraints_retries_transient_error(self):
        """A single transient failure is retried and the successful result is returned."""
        get_schema_pk_constraints.cache_clear()
        mock_conn = self._make_conn(
            [DatabaseError("transaction aborted", None, None), None]
        )

        with patch("time.sleep"):
            result = get_schema_pk_constraints(None, mock_conn, "db1")

        assert mock_conn.execute.call_count == 2
        assert result == {}

    def test_get_schema_pk_constraints_non_retryable_error_propagates(self):
        """A non-retryable error propagates immediately."""
        get_schema_pk_constraints.cache_clear()
        mock_conn = self._make_conn([DatabaseError("syntax error", None, None)])

        with patch("time.sleep"), pytest.raises(DatabaseError):
            get_schema_pk_constraints(None, mock_conn, "db1")

        assert mock_conn.execute.call_count == 1

    def test_get_schema_foreign_keys_retries_transient_error(self):
        """A single transient failure is retried and the successful result is returned."""
        get_schema_foreign_keys.cache_clear()
        mock_conn = self._make_conn(
            [DatabaseError("transaction aborted", None, None), None]
        )

        with patch("time.sleep"):
            result = get_schema_foreign_keys(None, mock_conn, "db1")

        assert mock_conn.execute.call_count == 2
        assert result == {}

    def test_get_schema_foreign_keys_non_retryable_error_propagates(self):
        """A non-retryable error propagates immediately."""
        get_schema_foreign_keys.cache_clear()
        mock_conn = self._make_conn([DatabaseError("syntax error", None, None)])

        with patch("time.sleep"), pytest.raises(DatabaseError):
            get_schema_foreign_keys(None, mock_conn, "db1")

        assert mock_conn.execute.call_count == 1


class TestMemoryEfficiency:
    """Test memory efficiency improvements."""

    def test_fetch_lineage_entries_chunked_streaming(self):
        """Test that lineage entries are processed in streaming fashion."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            # Mock the chunked fetching method to return a generator
            def mock_generator():
                for i in range(5):
                    mock_entry = MagicMock()
                    mock_entry.query_text = f"SELECT {i}"
                    mock_entry.session_id = f"session_{i}"
                    mock_entry.timestamp = "2024-01-01 10:00:00"
                    mock_entry.user = "test_user"
                    mock_entry.default_database = "test_db"
                    yield mock_entry

            with patch.object(
                source, "_fetch_lineage_entries_chunked", return_value=mock_generator()
            ):
                mock_aggregator.gen_metadata.return_value = []

                # Process entries
                source._populate_aggregator_from_audit_logs()

                # Verify aggregator.add was called for each entry (streaming)
                assert mock_aggregator.add.call_count == 5


class TestConcurrencySupport:
    """Test thread safety and concurrent operations."""

    def test_tables_cache_thread_safety(self):
        """Test that tables cache operations are thread-safe."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Verify lock exists
            assert hasattr(source, "_tables_cache_lock")

            # Test safe cache access methods
            result = source._tables_cache.get("nonexistent_schema", [])
            assert result == []

    def test_cached_loop_tables_safe_access(self):
        """Test cached_loop_tables uses safe cache access."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Add test data to cache
            test_table = TeradataTable(
                database="test_db",
                name="test_table",
                description="Test",
                object_type="Table",
                create_timestamp=datetime.now(),
                last_alter_name=None,
                last_alter_timestamp=None,
                request_text=None,
            )
            source._tables_cache["test_schema"] = [test_table]

            # Mock inspector and config
            mock_inspector = MagicMock()
            mock_sql_config = MagicMock()

            with patch(
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.loop_tables"
            ) as mock_super:
                mock_super.return_value = []

                # This should not raise an exception even with missing schema
                list(
                    source.cached_loop_tables(
                        mock_inspector, "missing_schema", mock_sql_config
                    )
                )


class TestStageTracking:
    """Test stage tracking functionality."""

    def test_stage_tracking_in_cache_operation(self):
        """Test that table caching uses stage tracking."""
        config = TeradataConfig.model_validate(_base_config())

        # Create source without mocking to test the actual stage tracking during init
        with (
            patch("datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"),
            patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ) as mock_cache,
        ):
            TeradataSource(config, PipelineContext(run_id="test"))

            # Verify cache_tables_and_views was called during init (stage tracking happens there)
            mock_cache.assert_called_once()

    def test_stage_tracking_in_aggregator_processing(self):
        """Test that aggregator processing uses stage tracking."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            with patch.object(source.report, "new_stage") as mock_new_stage:
                mock_context_manager = MagicMock()
                mock_new_stage.return_value = mock_context_manager

                with patch.object(
                    source, "_fetch_lineage_entries_chunked", return_value=[]
                ):
                    mock_aggregator.gen_metadata.return_value = []

                    source._populate_aggregator_from_audit_logs()

                    # Should have called new_stage for query processing and metadata generation
                    # The actual implementation uses new_stage for "Fetching queries" and "Generating metadata"
                    assert mock_new_stage.call_count >= 1


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_empty_lineage_entries(self):
        """Test handling of empty lineage entries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_fetch_lineage_entries_chunked", return_value=[]
            ):
                mock_aggregator.gen_metadata.return_value = []
                source._populate_aggregator_from_audit_logs()
                # Method doesn't return a value, just populates the aggregator

    def test_malformed_query_entry(self):
        """Test handling of malformed query entries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            # Mock cache_tables_and_views to prevent database connection during init
            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock entry with missing attributes
            mock_entry = MagicMock()
            mock_entry.query_text = "SELECT 1"
            # Simulate missing attributes
            del mock_entry.session_id
            del mock_entry.timestamp
            del mock_entry.user
            del mock_entry.default_database

            # Should handle gracefully using getattr with defaults
            observed_query = source._convert_entry_to_observed_query(mock_entry)

            assert observed_query.query == "SELECT 1"
            assert observed_query.session_id is None
            assert observed_query.user is None


class TestLineageQuerySeparation:
    """Test the new separated lineage query functionality (no more UNION)."""

    def test_make_lineage_queries_current_only(self):
        """Test that only current query is returned when historical lineage is disabled."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": False,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            queries = source._make_lineage_queries()

            assert len(queries) == 1
            assert '"DBC".QryLogV' in queries[0]
            assert "PDCRDATA.DBQLSqlTbl_Hst" not in queries[0]
            assert "2024-01-01" in queries[0]
            assert "2024-01-02" in queries[0]

    def test_make_lineage_queries_with_historical_available(self):
        """Test that UNION query is returned when historical lineage is enabled and table exists."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=True
            ):
                queries = source._make_lineage_queries()

            assert len(queries) == 1

            # Single UNION query should contain both historical and current data
            union_query = queries[0]
            assert '"DBC".QryLogV' in union_query
            assert '"PDCRINFO".DBQLSqlTbl_Hst' in union_query
            assert "UNION" in union_query
            assert "combined_results" in union_query

            # Should have the time filters
            assert "2024-01-01" in union_query
            assert "2024-01-02" in union_query

    def test_make_lineage_queries_with_historical_unavailable(self):
        """Test that only current query is returned when historical lineage is enabled but table doesn't exist."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=False
            ):
                queries = source._make_lineage_queries()

            assert len(queries) == 1
            assert '"DBC".QryLogV' in queries[0]
            assert '"PDCRDATA".DBQLSqlTbl_Hst' not in queries[0]

    def test_make_lineage_queries_with_database_filter(self):
        """Test that database filters are correctly applied to UNION query."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "databases": ["test_db1", "test_db2"],
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=True
            ):
                queries = source._make_lineage_queries()

            assert len(queries) == 1

            # UNION query should have case-insensitive database filters for both parts
            union_query = queries[0]
            assert (
                "l.DefaultDatabase (NOT CASESPECIFIC) in ('test_db1' (NOT CASESPECIFIC),'test_db2' (NOT CASESPECIFIC))"
                in union_query
            )
            assert (
                "h.DefaultDatabase (NOT CASESPECIFIC) in ('test_db1' (NOT CASESPECIFIC),'test_db2' (NOT CASESPECIFIC))"
                in union_query
            )

    def test_fetch_lineage_entries_chunked_multiple_queries(self):
        """Test that _fetch_lineage_entries_chunked handles multiple queries correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock the query generation to return 2 queries
            with patch.object(
                source, "_make_lineage_queries", return_value=["query1", "query2"]
            ):
                # Mock database execution for both queries
                mock_result1 = MagicMock()
                mock_result1.fetchmany.side_effect = [
                    [MagicMock(query_text="SELECT 1")],  # First batch
                    [],  # End of results
                ]

                mock_result2 = MagicMock()
                mock_result2.fetchmany.side_effect = [
                    [MagicMock(query_text="SELECT 2")],  # First batch
                    [],  # End of results
                ]

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value = mock_connection

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source, "_execute_with_cursor_fallback"
                    ) as mock_execute,
                ):
                    mock_execute.side_effect = [mock_result1, mock_result2]

                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should have executed both queries
                    assert mock_execute.call_count == 2
                    mock_execute.assert_any_call(mock_connection, "query1")
                    mock_execute.assert_any_call(mock_connection, "query2")

                    # Should return entries from both queries
                    assert len(entries) == 2

    def test_fetch_lineage_entries_chunked_single_query(self):
        """Test that _fetch_lineage_entries_chunked handles single query correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": False,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Mock the query generation to return 1 query
            with patch.object(source, "_make_lineage_queries", return_value=["query1"]):
                mock_result = MagicMock()
                mock_result.fetchmany.side_effect = [
                    [MagicMock(query_text="SELECT 1")],  # First batch
                    [],  # End of results
                ]

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value = mock_connection

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source,
                        "_execute_with_cursor_fallback",
                        return_value=mock_result,
                    ) as mock_execute,
                ):
                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should have executed only one query
                    assert mock_execute.call_count == 1
                    mock_execute.assert_called_with(mock_connection, "query1")

                    # Should return entries from the query
                    assert len(entries) == 1

    def test_fetch_lineage_entries_chunked_batch_processing(self):
        """Test that batch processing works correctly with configurable batch size."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": False,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(source, "_make_lineage_queries", return_value=["query1"]):
                # Create mock entries
                mock_entries = [MagicMock(query_text=f"SELECT {i}") for i in range(7)]

                mock_result = MagicMock()
                # Simulate batching with batch_size=5 (hardcoded in the method)
                mock_result.fetchmany.side_effect = [
                    mock_entries[:5],  # First batch (5 items)
                    mock_entries[5:],  # Second batch (2 items)
                    [],  # End of results
                ]

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value = mock_connection

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source,
                        "_execute_with_cursor_fallback",
                        return_value=mock_result,
                    ),
                ):
                    entries = list(source._fetch_lineage_entries_chunked())

                    # Should return all 7 entries
                    assert len(entries) == 7

                    # Verify fetchmany was called with the right batch size (5000 is hardcoded)
                    calls = mock_result.fetchmany.call_args_list
                    for call in calls:
                        if call[0]:  # If positional args
                            assert call[0][0] == 5000
                        else:  # If keyword args
                            assert call[1].get("size", 5000) == 5000

    def test_end_to_end_separate_queries_integration(self):
        """Test end-to-end integration of separate queries in the aggregator flow."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Replace the aggregator with our mock after creation
            source.aggregator = mock_aggregator

            # Mock entries from both current and historical queries
            current_entry = MagicMock()
            current_entry.query_text = "SELECT * FROM current_table"
            current_entry.user = "current_user"
            current_entry.timestamp = "2024-01-01 10:00:00"
            current_entry.default_database = "current_db"

            historical_entry = MagicMock()
            historical_entry.query_text = "SELECT * FROM historical_table"
            historical_entry.user = "historical_user"
            historical_entry.timestamp = "2023-12-01 10:00:00"
            historical_entry.default_database = "historical_db"

            def mock_fetch_generator():
                yield current_entry
                yield historical_entry

            with patch.object(
                source,
                "_fetch_lineage_entries_chunked",
                return_value=mock_fetch_generator(),
            ):
                mock_aggregator.gen_metadata.return_value = []

                # Execute the aggregator flow
                source._populate_aggregator_from_audit_logs()

                # Verify both entries were added to aggregator
                assert mock_aggregator.add.call_count == 2

                # Verify the entries were converted correctly
                added_queries = [
                    call[0][0] for call in mock_aggregator.add.call_args_list
                ]

                assert any(
                    "SELECT * FROM current_table" in query.query
                    for query in added_queries
                )
                assert any(
                    "SELECT * FROM historical_table" in query.query
                    for query in added_queries
                )

    def test_query_logging_and_progress_tracking(self):
        """Test that proper logging occurs when processing multiple queries."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_make_lineage_queries", return_value=["query1", "query2"]
            ):
                mock_result = MagicMock()

                call_counter = {"count": 0}

                def mock_fetchmany_side_effect(batch_size):
                    # Return one batch then empty to simulate end of results
                    call_counter["count"] += 1
                    if call_counter["count"] == 1:
                        return [MagicMock(query_text="SELECT 1")]
                    return []

                mock_result.fetchmany.side_effect = mock_fetchmany_side_effect

                mock_connection = MagicMock()
                mock_engine = MagicMock()
                mock_engine.connect.return_value = mock_connection

                with (
                    patch.object(
                        source, "get_metadata_engine", return_value=mock_engine
                    ),
                    patch.object(
                        source,
                        "_execute_with_cursor_fallback",
                        return_value=mock_result,
                    ),
                    patch(
                        "datahub.ingestion.source.sql.teradata.logger"
                    ) as mock_logger,
                ):
                    list(source._fetch_lineage_entries_chunked())

                    # Verify progress logging for multiple queries
                    info_calls = [
                        call for call in mock_logger.info.call_args_list if call[0]
                    ]

                    # Should log execution of query 1/2 and 2/2
                    assert any("query 1/2" in str(call) for call in info_calls)
                    assert any("query 2/2" in str(call) for call in info_calls)

                    # Should log completion of both queries (new format: "Completed lineage query_N")
                    assert any(
                        "Completed lineage query_1" in str(call) for call in info_calls
                    )
                    assert any(
                        "Completed lineage query_2" in str(call) for call in info_calls
                    )


class TestQueryConstruction:
    """Test the construction of individual queries."""

    def test_current_query_construction(self):
        """Test that the current query is constructed correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            queries = source._make_lineage_queries()
            current_query = queries[0]

            # Verify current query structure
            assert 'FROM "DBC".QryLogV as l' in current_query
            assert 'JOIN "DBC".QryLogSqlV as s' in current_query
            assert "l.ErrorCode = 0" in current_query
            assert "2024-01-01" in current_query
            assert "2024-01-02" in current_query
            assert 'ORDER BY "timestamp", "query_id", "row_no"' in current_query

    def test_historical_query_construction(self):
        """Test that the UNION query contains historical data correctly."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "include_historical_lineage": True,
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            with patch.object(
                source, "_check_historical_table_exists", return_value=True
            ):
                queries = source._make_lineage_queries()
                union_query = queries[0]

                # Verify UNION query contains historical data structure
                assert 'FROM "PDCRINFO".DBQLSqlTbl_Hst as h' in union_query
                assert "h.ErrorCode = 0" in union_query
                assert "h.StartTime AT TIME ZONE 'GMT'" in union_query
                assert "h.DefaultDatabase" in union_query
                assert "2024-01-01" in union_query
                assert "2024-01-02" in union_query
                assert 'ORDER BY "timestamp", "query_id", "row_no"' in union_query
                assert "UNION" in union_query


class TestStreamingQueryReconstruction:
    """Test the streaming query reconstruction functionality."""

    def test_reconstruct_queries_streaming_single_row_queries(self):
        """Test streaming reconstruction with single-row queries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries for single-row queries
            entries = [
                self._create_mock_entry(
                    "Q1", "SELECT * FROM table1", 1, "2024-01-01 10:00:00"
                ),
                self._create_mock_entry(
                    "Q2", "SELECT * FROM table2", 1, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q3", "SELECT * FROM table3", 1, "2024-01-01 10:02:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 3
            assert reconstructed_queries[0].query == "SELECT * FROM table1"
            assert reconstructed_queries[1].query == "SELECT * FROM table2"
            assert reconstructed_queries[2].query == "SELECT * FROM table3"

            # Verify metadata preservation
            assert reconstructed_queries[0].timestamp == "2024-01-01 10:00:00"
            assert reconstructed_queries[1].timestamp == "2024-01-01 10:01:00"
            assert reconstructed_queries[2].timestamp == "2024-01-01 10:02:00"

    def test_reconstruct_queries_streaming_multi_row_queries(self):
        """Test streaming reconstruction with multi-row queries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries for multi-row queries
            entries = [
                # Query 1: 3 rows
                self._create_mock_entry(
                    "Q1", "SELECT a, b, c ", 1, "2024-01-01 10:00:00"
                ),
                self._create_mock_entry(
                    "Q1", "FROM large_table ", 2, "2024-01-01 10:00:00"
                ),
                self._create_mock_entry(
                    "Q1", "WHERE id > 1000", 3, "2024-01-01 10:00:00"
                ),
                # Query 2: 2 rows
                self._create_mock_entry(
                    "Q2", "UPDATE table3 SET ", 1, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q2", "status = 'active'", 2, "2024-01-01 10:01:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 2
            assert (
                reconstructed_queries[0].query
                == "SELECT a, b, c FROM large_table WHERE id > 1000"
            )
            assert (
                reconstructed_queries[1].query == "UPDATE table3 SET status = 'active'"
            )

            # Verify metadata preservation (should use metadata from first row of each query)
            assert reconstructed_queries[0].timestamp == "2024-01-01 10:00:00"
            assert reconstructed_queries[1].timestamp == "2024-01-01 10:01:00"

    def test_reconstruct_queries_streaming_mixed_queries(self):
        """Test streaming reconstruction with mixed single and multi-row queries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries mixing single and multi-row queries
            entries = [
                # Single-row query
                self._create_mock_entry(
                    "Q1", "SELECT * FROM table1", 1, "2024-01-01 10:00:00"
                ),
                # Multi-row query (3 rows)
                self._create_mock_entry(
                    "Q2", "SELECT a, b, c ", 1, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q2", "FROM large_table ", 2, "2024-01-01 10:01:00"
                ),
                self._create_mock_entry(
                    "Q2", "WHERE id > 1000", 3, "2024-01-01 10:01:00"
                ),
                # Single-row query
                self._create_mock_entry(
                    "Q3", "SELECT COUNT(*) FROM table2", 1, "2024-01-01 10:02:00"
                ),
                # Multi-row query (2 rows)
                self._create_mock_entry(
                    "Q4", "UPDATE table3 SET ", 1, "2024-01-01 10:03:00"
                ),
                self._create_mock_entry(
                    "Q4", "status = 'active'", 2, "2024-01-01 10:03:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 4
            assert reconstructed_queries[0].query == "SELECT * FROM table1"
            assert (
                reconstructed_queries[1].query
                == "SELECT a, b, c FROM large_table WHERE id > 1000"
            )
            assert reconstructed_queries[2].query == "SELECT COUNT(*) FROM table2"
            assert (
                reconstructed_queries[3].query == "UPDATE table3 SET status = 'active'"
            )

    def test_reconstruct_queries_streaming_empty_entries(self):
        """Test streaming reconstruction with empty entries."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test with empty entries
            entries: List[Any] = []
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))
            assert len(reconstructed_queries) == 0

    def test_reconstruct_queries_streaming_teradata_specific_transformations(self):
        """Test that Teradata-specific transformations are applied."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entry with Teradata-specific syntax
            entries = [
                self._create_mock_entry(
                    "Q1",
                    "SELECT * FROM table1 (NOT CASESPECIFIC)",
                    1,
                    "2024-01-01 10:00:00",
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 1
            # Should remove (NOT CASESPECIFIC)
            assert reconstructed_queries[0].query == "SELECT * FROM table1 "

    def test_reconstruct_queries_streaming_metadata_preservation(self):
        """Test that all metadata fields are preserved correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entry with all metadata fields
            entries: List[Any] = [
                self._create_mock_entry(
                    "Q1",
                    "SELECT * FROM table1",
                    1,
                    "2024-01-01 10:00:00",
                    user="test_user",
                    default_database="test_db",
                    session_id="session123",
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 1
            query = reconstructed_queries[0]

            # Verify all metadata fields
            assert query.query == "SELECT * FROM table1"
            assert query.timestamp == "2024-01-01 10:00:00"
            assert isinstance(query.user, CorpUserUrn)
            assert str(query.user) == "urn:li:corpuser:test_user"
            assert query.default_db is None  # Fixed for Teradata two-tier architecture
            assert query.default_schema == "test_db"  # Teradata uses database as schema
            assert query.session_id == "session123"

    def test_reconstruct_queries_streaming_with_none_user(self):
        """Test streaming reconstruction handles None user correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entry with None user
            entries = [
                self._create_mock_entry(
                    "Q1", "SELECT * FROM table1", 1, "2024-01-01 10:00:00", user=None
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            assert len(reconstructed_queries) == 1
            assert reconstructed_queries[0].user is None

    def test_reconstruct_queries_streaming_empty_query_text(self):
        """Test streaming reconstruction handles empty query text correctly."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Create entries with empty query text
            entries = [
                self._create_mock_entry("Q1", "", 1, "2024-01-01 10:00:00"),
                self._create_mock_entry(
                    "Q2", "SELECT * FROM table1", 1, "2024-01-01 10:01:00"
                ),
            ]

            # Test streaming reconstruction
            reconstructed_queries = list(source._reconstruct_queries_streaming(entries))

            # Should only get one query (the non-empty one)
            assert len(reconstructed_queries) == 1
            assert reconstructed_queries[0].query == "SELECT * FROM table1"

    def test_reconstruct_queries_streaming_space_joining_behavior(self):
        """Test that query parts are joined directly without adding spaces."""
        config = TeradataConfig.model_validate(_base_config())

        with patch(
            "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
        ) as mock_aggregator_class:
            mock_aggregator = MagicMock()
            mock_aggregator_class.return_value = mock_aggregator

            with patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ):
                source = TeradataSource(config, PipelineContext(run_id="test"))

            # Test case 1: Parts that include their own spacing
            entries1 = [
                self._create_mock_entry("Q1", "SELECT ", 1, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "col1, ", 2, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "col2 ", 3, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "FROM ", 4, "2024-01-01 10:00:00"),
                self._create_mock_entry("Q1", "table1", 5, "2024-01-01 10:00:00"),
            ]

            # Test case 2: Parts that already have trailing/leading spaces
            entries2 = [
                self._create_mock_entry("Q2", "SELECT * ", 1, "2024-01-01 10:01:00"),
                self._create_mock_entry("Q2", "FROM table2 ", 2, "2024-01-01 10:01:00"),
                self._create_mock_entry("Q2", "WHERE id > 1", 3, "2024-01-01 10:01:00"),
            ]

            # Test streaming reconstruction
            all_entries = entries1 + entries2
            reconstructed_queries = list(
                source._reconstruct_queries_streaming(all_entries)
            )

            assert len(reconstructed_queries) == 2

            # Query 1: Should be joined directly without adding spaces
            assert reconstructed_queries[0].query == "SELECT col1, col2 FROM table1"

            # Query 2: Should handle existing spaces correctly (may have extra spaces)
            assert reconstructed_queries[1].query == "SELECT * FROM table2 WHERE id > 1"

    def _create_mock_entry(
        self,
        query_id,
        query_text,
        row_no,
        timestamp,
        user="test_user",
        default_database="test_db",
        session_id=None,
    ):
        """Create a mock database entry for testing."""
        entry = MagicMock()
        entry.query_id = query_id
        entry.query_text = query_text
        entry.row_no = row_no
        entry.timestamp = timestamp
        entry.user = user
        entry.default_database = default_database
        entry.session_id = session_id or f"session_{query_id}"
        return entry


class TestOwnershipExtraction:
    """Test ownership extraction functionality."""

    def test_table_creator_cache_population(self):
        """Test that _table_creator_cache is populated during cache_tables_and_views."""
        source = _create_source()
        mock_entry = _create_mock_table_entry(
            "test_db",
            "test_table",
            creator_name="creator_user",
            description="test comment",
            alter_name="test_user",
            alter_time=datetime(2024, 1, 2),
        )

        with patch.object(source, "get_metadata_engine") as mock_get_engine:
            mock_conn = MagicMock()
            mock_conn.execute.return_value = _mock_execute_result([mock_entry])
            mock_engine = MagicMock()
            mock_engine.connect.return_value = mock_conn
            mock_get_engine.return_value = mock_engine

            source.cache_tables_and_views()

            assert ("test_db", "test_table") in source._table_creator_cache
            assert (
                source._table_creator_cache[("test_db", "test_table")] == "creator_user"
            )

    def test_table_creator_cache_handles_none_creator(self):
        """Test that _table_creator_cache handles None creator gracefully."""
        source = _create_source()
        mock_entry = _create_mock_table_entry(
            "test_db2", "test_table2", creator_name=None
        )

        with patch.object(source, "get_metadata_engine") as mock_get_engine:
            mock_conn = MagicMock()
            mock_conn.execute.return_value = _mock_execute_result([mock_entry])
            mock_engine = MagicMock()
            mock_engine.connect.return_value = mock_conn
            mock_get_engine.return_value = mock_engine

            source.cache_tables_and_views()

            assert ("test_db2", "test_table2") not in source._table_creator_cache

    def test_get_creator_for_entity_returns_creator(self):
        """Test that _get_creator_for_entity returns creator when available."""
        source = _create_source()
        source._table_creator_cache[("test_db", "test_table")] = "creator_user"

        result = source._get_creator_for_entity("test_db", "test_table")

        assert result == "creator_user"

    def test_get_creator_for_entity_returns_none_when_not_found(self):
        """Test that _get_creator_for_entity returns None when entity not in cache."""
        source = _create_source()

        result = source._get_creator_for_entity("test_db", "nonexistent_table")

        assert result is None

    @pytest.mark.parametrize(
        "extract_ownership,has_creator,expected_work_units",
        [
            (True, True, True),  # Ownership enabled + creator available = emit
            (True, False, False),  # Ownership enabled + no creator = no emit
            (False, True, False),  # Ownership disabled + creator available = no emit
        ],
    )
    def test_emit_ownership_scenarios(
        self, extract_ownership, has_creator, expected_work_units
    ):
        """Test various scenarios for _emit_ownership_if_available."""
        source = _create_source(extract_ownership=extract_ownership)

        if has_creator:
            source._table_creator_cache[("test_db", "test_table")] = "creator_user"

        work_units = list(
            source._emit_ownership_if_available(
                "test_db.test_table", "test_db", "test_table"
            )
        )

        if expected_work_units:
            assert len(work_units) > 0
        else:
            assert len(work_units) == 0

    @pytest.mark.parametrize(
        "entity_type,parent_method",
        [
            (
                "table",
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource._process_table",
            ),
            (
                "view",
                "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource._process_view",
            ),
        ],
    )
    def test_process_entity_emits_ownership_work_units(
        self, entity_type, parent_method
    ):
        """Test that _process_table and _process_view emit ownership work units when configured."""
        source = _create_source(extract_ownership=True)
        entity_name = f"test_{entity_type}"
        source._table_creator_cache[("test_db", entity_name)] = "creator_user"

        mock_inspector = MagicMock()
        mock_sql_config = MagicMock()

        with patch(parent_method, return_value=iter([])):
            process_method = getattr(source, f"_process_{entity_type}")
            work_units = list(
                process_method(
                    f"test_db.{entity_name}",
                    mock_inspector,
                    "test_db",
                    entity_name,
                    mock_sql_config,
                )
            )

            assert len(work_units) == 1

            wu = work_units[0]
            assert isinstance(wu, MetadataWorkUnit)
            assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
            assert wu.metadata.aspectName == "ownership"

    def test_emit_ownership_early_return(self):
        """Test that _emit_ownership_if_available returns early when extract_ownership is False."""
        source = _create_source(extract_ownership=False)
        # Populate cache to ensure early return happens before creator check
        source._table_creator_cache[("test_db", "test_table")] = "creator_user"

        work_units = list(
            source._emit_ownership_if_available(
                "test_db.test_table", "test_db", "test_table"
            )
        )
        assert len(work_units) == 0

    def test_table_creator_cache_population_with_creator(self):
        """Test that cache is populated when CreatorName is not None."""
        source = _create_source()

        # Create entries with and without creator
        entry_with_creator = _create_mock_table_entry(
            "db1", "table_with_creator", creator_name="user1"
        )
        entry_without_creator = _create_mock_table_entry(
            "db1", "table_without_creator", creator_name=None
        )

        with patch.object(source, "get_metadata_engine") as mock_get_engine:
            mock_conn = MagicMock()
            mock_conn.execute.return_value = _mock_execute_result(
                [entry_with_creator, entry_without_creator]
            )
            mock_engine = MagicMock()
            mock_engine.connect.return_value = mock_conn
            mock_get_engine.return_value = mock_engine

            source.cache_tables_and_views()

            # Only entry with creator should be in cache
            assert ("db1", "table_with_creator") in source._table_creator_cache
            assert source._table_creator_cache[("db1", "table_with_creator")] == "user1"
            assert ("db1", "table_without_creator") not in source._table_creator_cache

    def test_get_creator_returns_from_cache(self):
        """Test that _get_creator_for_entity returns value from cache when present."""
        source = _create_source()

        # Manually populate cache
        source._table_creator_cache[("schema1", "entity1")] = "owner1"
        source._table_creator_cache[("schema2", "entity2")] = "owner2"

        # Test retrieval
        result1 = source._get_creator_for_entity("schema1", "entity1")
        result2 = source._get_creator_for_entity("schema2", "entity2")
        result3 = source._get_creator_for_entity("schema3", "entity3")

        assert result1 == "owner1"
        assert result2 == "owner2"
        assert result3 is None

    def test_emit_ownership_creates_user_urn(self):
        """Test that _emit_ownership_if_available creates correct user URN."""
        source = _create_source(extract_ownership=True)
        source._table_creator_cache[("test_db", "test_table")] = "test_creator"

        work_units = list(
            source._emit_ownership_if_available(
                "test_db.test_table", "test_db", "test_table"
            )
        )

        assert len(work_units) == 1
        # The work unit should contain ownership with user URN
        # Verify by checking the work unit was created (indicates make_user_urn was called)
        assert isinstance(work_units[0], MetadataWorkUnit)


class TestTeradataGetIdentifier:
    """Test get_identifier lowercasing behaviour.

    Teradata returns object names in UPPERCASE. The schema resolver is populated
    during Phase 1 (schema extraction) using the identifier returned by
    get_identifier(). sqlglot normalises all identifiers to lowercase during
    Phase 2 (view SQL parsing). If the two phases use different cases the schema
    resolver lookup is always a miss and column-level lineage is never generated.

    get_identifier() must apply convert_urns_to_lowercase at the point the
    identifier is constructed so that both phases use the same case.
    """

    def _make_source(self, convert_urns_to_lowercase: bool) -> TeradataSource:
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "convert_urns_to_lowercase": convert_urns_to_lowercase,
            }
        )
        with patch(
            "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
        ):
            return TeradataSource(config, PipelineContext(run_id="test"))

    def test_get_identifier_lowercase_when_flag_enabled(self):
        """Identifiers must be lowercased when convert_urns_to_lowercase is True.

        Teradata returns names like C3_T / MRCH_PRFL. With the flag on,
        get_identifier() must return c3_t.mrch_prfl so the schema resolver is
        populated with the same key that sqlglot produces during view SQL parsing.
        """
        source = self._make_source(convert_urns_to_lowercase=True)
        inspector = MagicMock()

        result = source.get_identifier(
            schema="C3_T", entity="MRCH_PRFL", inspector=inspector
        )

        assert result == "c3_t.mrch_prfl"

    def test_get_identifier_preserves_case_when_flag_disabled(self):
        """Identifiers must not be modified when convert_urns_to_lowercase is False."""
        source = self._make_source(convert_urns_to_lowercase=False)
        inspector = MagicMock()

        result = source.get_identifier(
            schema="C3_T", entity="MRCH_PRFL", inspector=inspector
        )

        assert result == "C3_T.MRCH_PRFL"

    def test_get_identifier_schema_resolver_key_matches_sqlglot_output(self):
        """The identifier returned must match what sqlglot produces from the same SQL.

        sqlglot always normalises unquoted identifiers to lowercase. The schema
        resolver key must be the same lowercase string so that Phase 2 lookups
        find the schema registered during Phase 1.
        """
        source = self._make_source(convert_urns_to_lowercase=True)
        inspector = MagicMock()

        identifier = source.get_identifier(
            schema="C3_T", entity="MRCH_PRFL", inspector=inspector
        )

        # Simulate what sqlglot produces from "SELECT ... FROM C3_T.MRCH_PRFL"
        sqlglot_normalized = "c3_t.mrch_prfl"
        assert identifier == sqlglot_normalized


# ---------------------------------------------------------------------------
# Tests for the 6 performance / scalability improvements
# ---------------------------------------------------------------------------


def _create_source_patched(
    extra_config: Optional[Dict[str, Any]] = None,
) -> TeradataSource:
    """Create a TeradataSource with cache_tables_and_views patched out."""
    config = TeradataConfig.model_validate({**_base_config(), **(extra_config or {})})
    with patch(
        "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
    ) as mock_class:
        mock_class.return_value = MagicMock()
        with patch(
            "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
        ):
            return TeradataSource(config, PipelineContext(run_id="test"))


class TestNewConfigDefaults:
    """New config fields have correct defaults and accept custom values."""

    def test_column_extraction_watermark_default(self) -> None:
        config = TeradataConfig.model_validate(_base_config())
        assert config.column_extraction_watermark is None

    def test_use_dbc_columns_for_views_default(self) -> None:
        config = TeradataConfig.model_validate(_base_config())
        assert config.use_dbc_columns_for_views is False

    def test_request_timeout_ms_default(self) -> None:
        config = TeradataConfig.model_validate(_base_config())
        assert config.request_timeout_ms == 120000

    def test_custom_timeout_values_accepted(self) -> None:
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "request_timeout_ms": 300000,
                "connect_timeout_ms": 60000,
            }
        )
        assert config.request_timeout_ms == 300000
        assert config.connect_timeout_ms == 60000

    def test_watermark_timezone_aware_is_normalised_to_naive_utc(self) -> None:
        """A tz-aware watermark must be normalised to naive UTC.

        Teradata returns LastAlterTimeStamp as a timezone-naive datetime.
        Comparing aware vs naive raises TypeError at runtime; the validator
        prevents this by stripping tzinfo after converting to UTC.
        """

        aware_watermark = "2024-06-01T12:00:00+05:30"  # IST = 06:30 UTC
        config = TeradataConfig.model_validate(
            {**_base_config(), "column_extraction_watermark": aware_watermark}
        )
        result = config.column_extraction_watermark
        assert result is not None
        assert result.tzinfo is None  # must be naive
        assert result == datetime(2024, 6, 1, 6, 30, 0)  # converted to UTC

    def test_watermark_naive_datetime_is_unchanged(self) -> None:
        """A timezone-naive watermark passes through unchanged."""
        config = TeradataConfig.model_validate(
            {
                **_base_config(),
                "column_extraction_watermark": "2024-06-01T00:00:00",
            }
        )
        assert config.column_extraction_watermark == datetime(2024, 6, 1, 0, 0, 0)
        assert config.column_extraction_watermark.tzinfo is None  # type: ignore[union-attr]

    def test_column_extraction_days_back_accepted(self) -> None:
        config = TeradataConfig.model_validate(
            {**_base_config(), "column_extraction_days_back": 3}
        )
        assert config.column_extraction_days_back == 3

    def test_both_watermark_options_raises_validation_error(self) -> None:
        """Setting both watermark options at the same time must raise a validation error."""
        with pytest.raises(Exception, match="mutually exclusive"):
            TeradataConfig.model_validate(
                {
                    **_base_config(),
                    "column_extraction_watermark": "2024-06-01T00:00:00",
                    "column_extraction_days_back": 3,
                }
            )


class TestIncrementalColumnExtraction:
    """#1 — skip column extraction for tables unchanged since the watermark."""

    def test_no_watermark_leaves_extraction_set_none(self) -> None:
        source = _create_source_patched()
        assert source._tables_needing_column_extraction is None

    def test_watermark_classifies_changed_and_unchanged_tables(self) -> None:
        watermark = datetime(2024, 6, 1)
        source = _create_source_patched(
            {"column_extraction_watermark": watermark.isoformat()}
        )

        entries = [
            _create_mock_table_entry(
                "db1", "new_table", alter_time=datetime(2024, 6, 2)
            ),  # after watermark → include
            _create_mock_table_entry(
                "db1", "old_table", alter_time=datetime(2024, 5, 1)
            ),  # before watermark → exclude
            _create_mock_table_entry(
                "db1", "no_ts_table"
            ),  # None timestamp → include (conservative)
        ]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = _mock_execute_result(entries)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        with patch.object(source, "get_metadata_engine", return_value=mock_engine):
            source.cache_tables_and_views()

        assert source._tables_needing_column_extraction is not None
        assert ("db1", "new_table") in source._tables_needing_column_extraction
        assert ("db1", "old_table") not in source._tables_needing_column_extraction
        assert ("db1", "no_ts_table") in source._tables_needing_column_extraction

    def test_days_back_resolves_to_watermark_at_runtime(self) -> None:
        """column_extraction_days_back uses the Teradata server clock, not the client clock."""

        days_back = 3
        # Simulate the Teradata server returning a timezone-aware timestamp
        # (Teradata drivers often return timezone-aware datetimes).
        # effective watermark = 2024-06-12 12:00:00 (server_now - 3 days, tzinfo stripped)
        td_server_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        source = _create_source_patched({"column_extraction_days_back": days_back})

        # Table altered 1 day before server_now → within the 3-day window → included
        recent_entry = _create_mock_table_entry(
            "db1", "recent_table", alter_time=datetime(2024, 6, 14, 12, 0, 0)
        )
        # Table altered 10 days before server_now → outside the window → excluded
        stale_entry = _create_mock_table_entry(
            "db1", "stale_table", alter_time=datetime(2024, 6, 5, 12, 0, 0)
        )

        # Two engine.connect() calls are made: one for SELECT CURRENT_TIMESTAMP(0)
        # and one for the tables/views query. Use side_effect to return a different
        # mock connection for each.
        mock_conn_ts = MagicMock()
        mock_conn_ts.execute.return_value.fetchone.return_value = (td_server_now,)

        mock_conn_rows = MagicMock()
        mock_conn_rows.execute.return_value = _mock_execute_result(
            [recent_entry, stale_entry]
        )

        mock_engine = MagicMock()
        mock_engine.connect.side_effect = [mock_conn_ts, mock_conn_rows]

        with patch.object(source, "get_metadata_engine", return_value=mock_engine):
            source.cache_tables_and_views()

        assert source._tables_needing_column_extraction is not None
        assert ("db1", "recent_table") in source._tables_needing_column_extraction
        assert ("db1", "stale_table") not in source._tables_needing_column_extraction

    def test_optimized_get_columns_skips_table_not_in_extraction_set(self) -> None:
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        tables_cache: Dict[str, List[TeradataTable]] = {
            "mydb": [
                TeradataTable(
                    database="mydb",
                    name="unchanged",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
        }
        tables_needing_extraction = {("mydb", "other_table")}  # "unchanged" absent

        result = optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "unchanged",
            "mydb",
            tables_cache=tables_cache,
            tables_needing_extraction=tables_needing_extraction,
        )

        assert result == []
        mock_dialect._get_column_help.assert_not_called()
        mock_dialect.get_schema_columns.assert_not_called()

    def test_optimized_get_columns_proceeds_for_changed_table(self) -> None:
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"
        mock_dialect.get_schema_columns.return_value = {"my_table": []}
        mock_dialect._get_column_info.return_value = {"name": "col1"}

        tables_cache: Dict[str, List[TeradataTable]] = {
            "mydb": [
                TeradataTable(
                    database="mydb",
                    name="my_table",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
        }
        tables_needing_extraction = {("mydb", "my_table")}

        # Should not short-circuit; get_schema_columns will be reached
        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_table",
            "mydb",
            tables_cache=tables_cache,
            tables_needing_extraction=tables_needing_extraction,
        )

        mock_dialect.get_schema_columns.assert_called_once()

    def test_none_extraction_set_processes_all_tables(self) -> None:
        """tables_needing_extraction=None (no watermark) means extract everything."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"
        mock_dialect.get_schema_columns.return_value = {"my_table": []}

        tables_cache: Dict[str, List[TeradataTable]] = {
            "mydb": [
                TeradataTable(
                    database="mydb",
                    name="my_table",
                    description=None,
                    object_type="Table",
                    create_timestamp=datetime.now(),
                    last_alter_name=None,
                    last_alter_timestamp=None,
                    request_text=None,
                )
            ]
        }

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_table",
            "mydb",
            tables_cache=tables_cache,
            tables_needing_extraction=None,
        )

        mock_dialect.get_schema_columns.assert_called_once()


class TestDbcColumnsForViews:
    """#2 — bulk dbc.ColumnsV for views with HELP fallback for derived columns."""

    def _view_table(self, name: str = "my_view", schema: str = "mydb") -> TeradataTable:
        return TeradataTable(
            database=schema,
            name=name,
            description=None,
            object_type="View",
            create_timestamp=datetime.now(),
            last_alter_name=None,
            last_alter_timestamp=None,
            request_text=None,
        )

    def test_uses_dbc_columns_when_all_types_present(self) -> None:
        """No HELP call when every column has a non-null ColumnType."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        mock_row = MagicMock()
        mock_row.ColumnType = "CV"  # explicit type present
        mock_dialect.get_schema_columns.return_value = {"my_view": [mock_row]}
        mock_dialect._get_column_info.return_value = {"name": "col1"}

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=True,
        )

        mock_dialect._get_column_help.assert_not_called()
        mock_dialect.get_schema_columns.assert_called_once()

    def test_falls_back_to_help_when_column_type_is_null(self) -> None:
        """Falls back to HELP when a column has null ColumnType (derived expression)."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        mock_row = MagicMock()
        mock_row.ColumnType = None
        mock_dialect.get_schema_columns.return_value = {"my_view": [mock_row]}
        mock_dialect._get_column_help.return_value = []

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=True,
        )

        mock_dialect._get_column_help.assert_called_once()

    def test_falls_back_to_help_when_column_type_is_empty_string(self) -> None:
        """Empty ColumnType string is also treated as unknown."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"

        mock_row = MagicMock()
        mock_row.ColumnType = "   "  # whitespace-only
        mock_dialect.get_schema_columns.return_value = {"my_view": [mock_row]}
        mock_dialect._get_column_help.return_value = []

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=True,
        )

        mock_dialect._get_column_help.assert_called_once()

    def test_conservative_default_always_uses_help(self) -> None:
        """When use_dbc_columns_for_views=False, HELP is always used for views."""
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "mydb"
        mock_dialect._get_column_help.return_value = []

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "my_view",
            "mydb",
            tables_cache={"mydb": [self._view_table()]},
            use_dbc_columns_for_views=False,
        )

        mock_dialect._get_column_help.assert_called_once()
        mock_dialect.get_schema_columns.assert_not_called()


class TestLruCacheSize:
    """#3 — caches are unbounded (maxsize=None) so multi-database runs don't lose entries."""

    def test_all_schema_caches_are_bounded_at_32(self) -> None:
        # 32 covers any realistic number of concurrently active schemas without
        # allowing unbounded accumulation of entries for dead connections.
        assert get_schema_columns.cache_info().maxsize == 32
        assert get_schema_pk_constraints.cache_info().maxsize == 32
        assert get_schema_foreign_keys.cache_info().maxsize == 32

    def test_multiple_schemas_are_all_retained(self) -> None:
        """All schemas stay cached; switching databases does not evict earlier results."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        get_schema_columns.cache_clear()
        try:
            get_schema_columns(None, mock_conn, "columnsV", "schema_a")
            get_schema_columns(None, mock_conn, "columnsV", "schema_b")
            get_schema_columns(None, mock_conn, "columnsV", "schema_c")

            info = get_schema_columns.cache_info()
            assert info.currsize == 3
            assert info.misses == 3
            # Repeated call for schema_a should be a cache hit, not a miss
            get_schema_columns(None, mock_conn, "columnsV", "schema_a")
            assert get_schema_columns.cache_info().hits == 1
        finally:
            get_schema_columns.cache_clear()


class TestLineageQueryScoping:
    """#4 — lineage queries are scoped to discovered databases via database_pattern."""

    def test_scopes_to_discovered_databases_filtered_by_pattern(self) -> None:
        source = _create_source_patched(
            {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )
        # Simulate what cache_tables_and_views populates
        source._tables_cache["sales_db"] = []
        source._tables_cache["hr_db"] = []
        # "All" is in EXCLUDED_DATABASES, so database_pattern.allowed("All") is False
        source._tables_cache["All"] = []

        with patch.object(source, "_check_historical_table_exists", return_value=False):
            queries = source._make_lineage_queries()

        query = queries[0]
        assert "sales_db" in query
        assert "hr_db" in query
        assert "'All'" not in query

    def test_config_databases_takes_precedence_over_cache(self) -> None:
        source = _create_source_patched(
            {
                "databases": ["explicit_db"],
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )
        source._tables_cache["other_db"] = []

        with patch.object(source, "_check_historical_table_exists", return_value=False):
            queries = source._make_lineage_queries()

        query = queries[0]
        assert "explicit_db" in query
        assert "other_db" not in query

    def test_no_database_filter_when_cache_is_empty(self) -> None:
        """Lineage-only runs (empty cache) should not produce an empty IN clause."""
        source = _create_source_patched(
            {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
            }
        )
        assert len(source._tables_cache) == 0

        with patch.object(source, "_check_historical_table_exists", return_value=False):
            queries = source._make_lineage_queries()

        assert len(queries) == 1
        assert "DefaultDatabase in" not in queries[0]


class TestConfigurableTimeouts:
    """request_timeout_ms / connect_timeout_ms flow through to all engines;
    connection_pool_timeout_ms flows only to the QueuePool-based pooled engine."""

    def _get_engine_kwargs(
        self, extra_config: Dict[str, Any], *, engine_site: str = "pooled"
    ) -> Dict[str, Any]:
        """Capture kwargs passed to create_engine for the requested site.

        engine_site: "pooled" calls _get_or_create_pooled_engine;
                     "metadata" calls get_metadata_engine.
        """
        source = _create_source_patched(extra_config)
        captured: Dict[str, Any] = {}

        def capture(url: Any, **kwargs: Any) -> MagicMock:
            captured.update(kwargs)
            return MagicMock()

        with patch(
            "datahub.ingestion.source.sql.teradata.create_engine", side_effect=capture
        ):
            if engine_site == "pooled":
                source._get_or_create_pooled_engine()
            else:
                source.get_metadata_engine()

        return captured

    def test_default_timeouts_used(self) -> None:
        kwargs = self._get_engine_kwargs({})
        connect_args = kwargs["connect_args"]
        assert connect_args["request_timeout"] == "120000"
        assert connect_args["connect_timeout"] == "30000"

    def test_custom_timeouts_propagated(self) -> None:
        kwargs = self._get_engine_kwargs(
            {"request_timeout_ms": 300000, "connect_timeout_ms": 60000}
        )
        connect_args = kwargs["connect_args"]
        assert connect_args["request_timeout"] == "300000"
        assert connect_args["connect_timeout"] == "60000"

    def test_pool_timeout_applied_to_pooled_engine_only(self) -> None:
        """connection_pool_timeout_ms reaches the pooled (QueuePool) engine but must NOT
        be passed to the metadata/schema-discovery engine, which uses SingletonThreadPool
        and rejects pool_timeout as an invalid argument."""
        pooled_kwargs = self._get_engine_kwargs(
            {"connection_pool_timeout_ms": 45000}, engine_site="pooled"
        )
        metadata_kwargs = self._get_engine_kwargs(
            {"connection_pool_timeout_ms": 45000}, engine_site="metadata"
        )
        assert pooled_kwargs["pool_timeout"] == 45.0
        assert "pool_timeout" not in metadata_kwargs

    def test_connect_args_consistent_across_engine_sites(self) -> None:
        """Both engine creation paths must see identical connect_args timeout values."""
        extra = {
            "request_timeout_ms": 90000,
            "connect_timeout_ms": 15000,
        }
        pooled_kwargs = self._get_engine_kwargs(extra, engine_site="pooled")
        metadata_kwargs = self._get_engine_kwargs(extra, engine_site="metadata")

        assert pooled_kwargs["connect_args"]["request_timeout"] == "90000"
        assert pooled_kwargs["connect_args"]["connect_timeout"] == "15000"
        assert metadata_kwargs["connect_args"]["request_timeout"] == "90000"
        assert metadata_kwargs["connect_args"]["connect_timeout"] == "15000"


class TestCacheCaseInsensitivity:
    """Cache must hit regardless of whether config and Teradata report the database
    name in the same case. dbc.TablesV returns Teradata's stored case (typically
    uppercase) while users commonly write `databases: [my_db]` in lowercase — the
    pre-fix lookup missed and the run silently produced zero datasets.
    """

    def _make_table(
        self, database: str, name: str, object_type: str = "Table"
    ) -> TeradataTable:
        return TeradataTable(
            database=database,
            name=name,
            description=None,
            object_type=object_type,
            create_timestamp=datetime(2024, 1, 1),
            last_alter_name=None,
            last_alter_timestamp=None,
            request_text="SELECT 1" if object_type == "View" else None,
        )

    def test_cache_write_lowercases_database_key(self) -> None:
        """Teradata returns uppercase DataBaseName; the cache stores it lowercased."""
        source = _create_source_patched()
        mock_conn = MagicMock()
        mock_conn.execute.return_value = _mock_execute_result(
            [_create_mock_table_entry("MY_DB", "MY_TABLE")]
        )
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        with patch.object(source, "get_metadata_engine", return_value=mock_engine):
            source.cache_tables_and_views()

        assert "my_db" in source._tables_cache
        assert "MY_DB" not in source._tables_cache

    def test_cached_loop_tables_finds_uppercase_entries_with_lowercase_schema(
        self,
    ) -> None:
        """Lookup with a lowercase schema must hit cache entries written in
        Teradata's stored (typically uppercase) case."""
        source = _create_source_patched()
        source._tables_cache["my_db"] = [self._make_table("MY_DB", "MY_TABLE")]

        with patch(
            "datahub.ingestion.source.sql.two_tier_sql_source.TwoTierSQLAlchemySource.loop_tables"
        ) as mock_super:
            mock_super.return_value = []
            list(source.cached_loop_tables(MagicMock(), "my_db", MagicMock()))

        # super().loop_tables sees the patched get_table_names — invoke it and check
        # that the schema lookup hits the cache despite case mismatch.
        inspector = mock_super.call_args.args[0]
        assert inspector.get_table_names("my_db") == ["MY_TABLE"]

    def test_cached_loop_views_finds_uppercase_entries_with_lowercase_schema(
        self,
    ) -> None:
        source = _create_source_patched()
        source._tables_cache["my_db"] = [
            self._make_table("MY_DB", "MY_VIEW", object_type="View")
        ]

        with patch.object(
            source, "_loop_views_with_connection_pool", return_value=iter([])
        ) as mock_pool:
            list(source.cached_loop_views(MagicMock(), "my_db", MagicMock()))

        # Pre-fix the view list would be empty and the pool never invoked.
        mock_pool.assert_called_once()
        view_names = mock_pool.call_args.args[0]
        assert view_names == ["MY_VIEW"]

    def test_cached_get_table_properties_finds_entry_with_lowercase_schema(
        self,
    ) -> None:
        source = _create_source_patched()
        entry = self._make_table("MY_DB", "MY_TABLE", object_type="View")
        entry.description = "promo mart"
        source._tables_cache["my_db"] = [entry]

        description, properties, _ = source.cached_get_table_properties(
            MagicMock(), "my_db", "MY_TABLE"
        )

        assert description == "promo mart"
        assert properties["view_definition"] == "SELECT 1"

    def test_optimized_get_columns_lowercases_schema_lookup(self) -> None:
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "MY_DB"
        mock_dialect.get_schema_columns.return_value = {"MY_TABLE": []}

        tables_cache: Dict[str, List[TeradataTable]] = {
            "my_db": [self._make_table("MY_DB", "MY_TABLE")]
        }

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "MY_TABLE",
            "MY_DB",
            tables_cache=tables_cache,
        )

        # Reaches column extraction only if the cache lookup hits.
        mock_dialect.get_schema_columns.assert_called_once()

    def test_optimized_get_view_definition_lowercases_schema_lookup(self) -> None:
        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "MY_DB"
        mock_dialect.normalize_name = lambda s: s

        tables_cache: Dict[str, List[TeradataTable]] = {
            "my_db": [self._make_table("MY_DB", "MY_VIEW", object_type="View")]
        }

        view_def = optimized_get_view_definition(
            mock_dialect,
            MagicMock(),
            "MY_VIEW",
            "MY_DB",
            tables_cache=tables_cache,
        )

        assert view_def == "SELECT 1"

    def test_creator_cache_lookup_is_case_insensitive_on_database(self) -> None:
        """extract_ownership: True + lowercase databases must still find creators."""
        source = _create_source_patched()
        mock_conn = MagicMock()
        mock_conn.execute.return_value = _mock_execute_result(
            [_create_mock_table_entry("MY_DB", "MY_TABLE", creator_name="creator_user")]
        )
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        with patch.object(source, "get_metadata_engine", return_value=mock_engine):
            source.cache_tables_and_views()

        assert source._get_creator_for_entity("my_db", "MY_TABLE") == "creator_user"

    def test_column_extraction_set_lookup_is_case_insensitive_on_database(
        self,
    ) -> None:
        """Incremental column extraction must hit the set when the watermark-populated
        rows are uppercase but the SQLAlchemy reflection passes the user's lowercase
        schema. Pre-fix this skipped every table with a "unchanged since watermark"
        debug log.
        """
        watermark = datetime(2024, 6, 1)
        source = _create_source_patched(
            {"column_extraction_watermark": watermark.isoformat()}
        )
        mock_conn = MagicMock()
        mock_conn.execute.return_value = _mock_execute_result(
            [
                _create_mock_table_entry(
                    "MY_DB",
                    "MY_TABLE",
                    alter_time=datetime(2024, 6, 2),
                )
            ]
        )
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        with patch.object(source, "get_metadata_engine", return_value=mock_engine):
            source.cache_tables_and_views()

        assert source._tables_needing_column_extraction == {("my_db", "MY_TABLE")}

        mock_dialect = MagicMock()
        mock_dialect.default_schema_name = "MY_DB"
        mock_dialect.get_schema_columns.return_value = {"MY_TABLE": []}

        optimized_get_columns(
            mock_dialect,
            MagicMock(),
            "MY_TABLE",
            "MY_DB",
            tables_cache=dict(source._tables_cache),
            tables_needing_extraction=source._tables_needing_column_extraction,
        )

        # Reaches column extraction only if the (schema, table) tuple matched the set.
        mock_dialect.get_schema_columns.assert_called_once()

    def test_tables_query_uses_not_casespecific_on_database_filter(self) -> None:
        """Guards against installations whose default session collation is
        CASESPECIFIC — without (NOT CASESPECIFIC) the IN-list would not match the
        uppercase DataBaseName rows in dbc.TablesV.
        """
        source = _create_source_patched({"databases": ["my_db"]})
        query = source._build_tables_and_views_query()
        assert "DataBaseName (NOT CASESPECIFIC) IN" in query
        assert "'my_db' (NOT CASESPECIFIC)" in query


class TestConfiguredDatabasesValidation:
    """When the user supplies an explicit `databases` list, entries that don't
    exist on the source must not produce a container URN. Pre-fix, every name
    in the list was yielded by get_inspectors() and the base SQL source emitted
    a container for it — so a typo polluted the platform with phantom entities.
    """

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    def test_nonexistent_database_skipped_with_warning(
        self, mock_create_engine: MagicMock
    ) -> None:
        source = _create_source_patched({"databases": ["real_db", "typo_db"]})
        # Discovery populates the cache for "real_db" only (uppercase from
        # Teradata → lowercased by the cache fix).
        source._tables_cache["real_db"] = [
            TeradataTable(
                database="REAL_DB",
                name="T",
                description=None,
                object_type="Table",
                create_timestamp=datetime(2024, 1, 1),
                last_alter_name=None,
                last_alter_timestamp=None,
                request_text=None,
            )
        ]

        mock_engine = MagicMock()
        mock_engine.connect.return_value = MagicMock()
        mock_create_engine.return_value = mock_engine

        with patch("datahub.ingestion.source.sql.teradata.inspect") as mock_inspect:
            mock_inspect.return_value = MagicMock()
            inspectors = list(source.get_inspectors())

        # Only the database that actually has entries in the cache gets an
        # inspector yielded; the typo is dropped before a container is emitted.
        yielded = [i._datahub_database for i in inspectors]
        assert yielded == ["real_db"]

        warning_titles = [w.title for w in source.report.warnings]
        assert "Configured database not found on source" in warning_titles

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    def test_no_validation_when_discovery_disabled(
        self, mock_create_engine: MagicMock
    ) -> None:
        """include_tables=False and include_views=False means the cache was
        never populated, so we have no oracle to validate against. Fall back
        to trusting the user's list.
        """
        source = _create_source_patched(
            {
                "databases": ["any_db"],
                "include_tables": False,
                "include_views": False,
            }
        )
        # _tables_cache is empty in this scenario.

        mock_engine = MagicMock()
        mock_engine.connect.return_value = MagicMock()
        mock_create_engine.return_value = mock_engine

        with patch("datahub.ingestion.source.sql.teradata.inspect") as mock_inspect:
            mock_inspect.return_value = MagicMock()
            inspectors = list(source.get_inspectors())

        yielded = [i._datahub_database for i in inspectors]
        assert yielded == ["any_db"]
        assert source.report.warnings == []

    @patch("datahub.ingestion.source.sql.teradata.create_engine")
    def test_no_validation_when_databases_not_user_supplied(
        self, mock_create_engine: MagicMock
    ) -> None:
        """When the user did not supply config.database(s), the database list
        comes from inspector.get_schema_names() which is already authoritative.
        Validation against the cache would be redundant (and would wrongly
        suppress empty-but-real databases).
        """
        source = _create_source_patched()  # no `databases` set

        mock_inspector = MagicMock()
        mock_inspector.get_schema_names.return_value = ["from_inspector"]
        # _tables_cache is empty (no discovery in this minimal test setup),
        # but we still expect "from_inspector" to be yielded because it came
        # from the authoritative source.

        mock_engine = MagicMock()
        mock_engine.connect.return_value = MagicMock()
        mock_create_engine.return_value = mock_engine

        with patch(
            "datahub.ingestion.source.sql.teradata.inspect",
            return_value=mock_inspector,
        ):
            inspectors = list(source.get_inspectors())

        yielded = [i._datahub_database for i in inspectors]
        assert yielded == ["from_inspector"]
        assert source.report.warnings == []


class TestJitteredBackoff:
    """_jittered_backoff produces bounded, non-deterministic delays."""

    def test_result_within_bounds(self):
        """Backoff must be in [0, initial * 2^attempt]."""
        for attempt in range(4):
            cap = 1.0 * (2**attempt)
            for _ in range(20):
                b = _jittered_backoff(attempt, 1.0)
                assert 0 <= b <= cap, f"attempt={attempt}: {b} not in [0, {cap}]"

    def test_not_always_zero(self):
        """With many samples at least one must be > 0 (probability of all-zero is negligible)."""
        samples = [_jittered_backoff(0, 1.0) for _ in range(50)]
        assert any(s > 0 for s in samples)

    def test_values_are_not_all_identical(self):
        """Different calls should produce different values (jitter is actually random)."""
        samples = [_jittered_backoff(2, 1.0) for _ in range(20)]
        assert len(set(samples)) > 1, (
            "All backoff values were identical — jitter missing"
        )

    def test_scales_with_initial_backoff(self):
        """Larger initial_backoff_seconds raises the cap proportionally."""
        cap_small = 0.5 * (2**2)
        cap_large = 2.0 * (2**2)
        for _ in range(20):
            assert _jittered_backoff(2, 0.5) <= cap_small
            assert _jittered_backoff(2, 2.0) <= cap_large


class TestShouldRetry:
    """_should_retry classifies errors as retryable or not."""

    def test_pool_timeout_is_always_retryable(self):
        assert _should_retry(PoolTimeoutError("pool exhausted")) is True

    def test_operational_error_with_retryable_message(self):
        assert _should_retry(OperationalError("connect timed out", None, None)) is True

    def test_operational_error_with_retryable_error_code(self):
        # Error codes 2631, 3111, 3120, 3598, 3897, 3603 are explicitly retryable.
        assert _should_retry(OperationalError("[Error 3598]", None, None)) is True
        assert _should_retry(OperationalError("[Error 3897]", None, None)) is True
        assert _should_retry(OperationalError("[Error 3603]", None, None)) is True
        assert _should_retry(OperationalError("[Error 2631]", None, None)) is True

    def test_operational_error_non_retryable_auth_failure(self):
        """Auth failures and config errors embedded in OperationalError must NOT be retried."""
        assert (
            _should_retry(OperationalError("authentication failed", None, None))
            is False
        )
        assert _should_retry(OperationalError("permission denied", None, None)) is False
        # Teradata error 3807 = "Object does not exist" — non-transient config error.
        assert _should_retry(OperationalError("[Error 3807]", None, None)) is False

    def test_database_error_with_retryable_message(self):
        assert _should_retry(DatabaseError("connect timed out", None, None)) is True

    def test_database_error_non_retryable(self):
        assert _should_retry(DatabaseError("syntax error", None, None)) is False

    def test_generic_exception_not_retryable(self):
        assert _should_retry(ValueError("something went wrong")) is False

    def test_all_retryable_substrings_match(self):
        """Every substring in _RETRYABLE_ERROR_SUBSTRINGS is recognised as retryable."""
        retryable_messages = [
            "transaction aborted",
            "database restart",
            "connect timed out",
            "i/o timeout",
        ]
        for msg in retryable_messages:
            assert _should_retry(DatabaseError(msg, None, None)) is True, (
                f"Expected {msg!r} to be retryable"
            )
            # Also retryable when mixed-case (check is lowercased)
            assert _should_retry(DatabaseError(msg.upper(), None, None)) is True, (
                f"Expected upper-case {msg!r} to be retryable"
            )

    def test_all_retryable_error_codes_match(self):
        """Every numeric error code in _RETRYABLE_ERROR_CODE_RE is recognised as retryable."""
        retryable_codes = [2631, 2639, 3111, 3120, 3598, 3897, 3603]
        for code in retryable_codes:
            assert (
                _should_retry(OperationalError(f"[Error {code}]", None, None)) is True
            ), f"Expected error code {code} to be retryable"

    def test_dead_socket_substrings_not_retryable_on_execute(self):
        """Dead-socket errors must not be retried on an existing connection."""
        for msg in ("connection reset", "broken pipe", "eof", "socket closed"):
            assert _should_retry(OperationalError(msg, None, None)) is False, msg


class TestShouldRetryConnect:
    """_should_retry_connect is a superset of _should_retry for connect-time errors."""

    def test_inherits_all_execute_retryable_cases(self):
        """Everything retryable at execute time is also retryable at connect time."""
        assert _should_retry_connect(PoolTimeoutError("pool exhausted")) is True
        assert (
            _should_retry_connect(OperationalError("connect timed out", None, None))
            is True
        )
        assert (
            _should_retry_connect(OperationalError("[Error 3598]", None, None)) is True
        )
        assert (
            _should_retry_connect(DatabaseError("transaction aborted", None, None))
            is True
        )

    def test_dead_socket_errors_retryable_at_connect_time(self):
        """Dead-socket errors are retryable at connect time since a fresh socket is opened."""
        for msg in ("connection reset", "broken pipe", "eof", "socket closed"):
            assert _should_retry_connect(OperationalError(msg, None, None)) is True, (
                f"Expected {msg!r} to be retryable at connect time"
            )
            assert _should_retry_connect(DatabaseError(msg, None, None)) is True, (
                f"Expected DatabaseError({msg!r}) to be retryable at connect time"
            )

    def test_non_retryable_errors_still_rejected(self):
        """Permanent errors (auth failure, syntax error) are not retried even at connect time."""
        assert (
            _should_retry_connect(OperationalError("authentication failed", None, None))
            is False
        )
        assert _should_retry_connect(DatabaseError("syntax error", None, None)) is False
        assert _should_retry_connect(ValueError("something went wrong")) is False

    def test_engine_connect_retries_dead_socket(self):
        """_engine_connect_with_retry retries dead-socket errors because a new socket is opened."""
        good_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = [
            OperationalError("connection reset", None, None),
            good_conn,
        ]
        report = TeradataReport()

        with (
            patch("time.sleep"),
            _engine_connect_with_retry(
                mock_engine, max_attempts=2, report=report
            ) as conn,
        ):
            assert conn is good_conn

        assert mock_engine.connect.call_count == 2
        assert report.num_db_retries == 1
        good_conn.close.assert_called_once()


class TestRetryConfig:
    """retry_max_attempts and retry_initial_backoff_seconds config fields."""

    def test_retry_max_attempts_custom(self):
        config = TeradataConfig.model_validate(
            {**_base_config(), "retry_max_attempts": 5}
        )
        assert config.retry_max_attempts == 5

    def test_retry_initial_backoff_seconds_custom(self):
        config = TeradataConfig.model_validate(
            {**_base_config(), "retry_initial_backoff_seconds": 2.5}
        )
        assert config.retry_initial_backoff_seconds == 2.5

    def test_retry_max_attempts_zero_is_invalid(self):
        with pytest.raises(ValidationError):
            TeradataConfig.model_validate({**_base_config(), "retry_max_attempts": 0})

    def test_retry_initial_backoff_seconds_zero_is_invalid(self):
        with pytest.raises(ValidationError):
            TeradataConfig.model_validate(
                {**_base_config(), "retry_initial_backoff_seconds": 0.0}
            )

    def test_retry_initial_backoff_seconds_negative_is_invalid(self):
        with pytest.raises(ValidationError):
            TeradataConfig.model_validate(
                {**_base_config(), "retry_initial_backoff_seconds": -1.0}
            )

    def test_retry_wrappers_honour_config(self):
        """_retry_connect / _retry_execute / _retry_fetchmany use config values."""
        source = _create_source_patched(
            {"retry_max_attempts": 2, "retry_initial_backoff_seconds": 0.01}
        )

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = [
            PoolTimeoutError("exhausted"),
            mock_conn,
        ]

        with patch("time.sleep"), source._retry_connect(mock_engine) as conn:
            assert conn is mock_conn

        assert mock_engine.connect.call_count == 2


class TestPoolSizeConfig:
    """max_pool_size field: default, valid bounds, and out-of-range rejection."""

    def test_max_pool_size_zero_is_invalid(self):
        with pytest.raises(ValidationError):
            TeradataConfig.model_validate({**_base_config(), "max_pool_size": 0})


class TestEffectiveMaxWorkers:
    """_effective_max_workers is capped to max_pool_size; config.max_workers is never mutated."""

    def test_effective_max_workers_initialized_from_config(self):
        """On source creation, _effective_max_workers mirrors config.max_workers."""
        source = _create_source_patched({"max_workers": 7})
        assert source._effective_max_workers == 7

    def test_effective_max_workers_capped_to_pool_size(self):
        """When max_workers > max_pool_size the effective count is capped to the pool limit."""
        source = _create_source_patched({"max_workers": 20, "max_pool_size": 5})

        with patch(
            "datahub.ingestion.source.sql.teradata.create_engine",
            return_value=MagicMock(),
        ):
            source._get_or_create_pooled_engine()

        assert source._effective_max_workers == 5

    def test_config_max_workers_unchanged_after_pool_creation(self):
        """config.max_workers must not be mutated when the pool caps the worker count."""
        source = _create_source_patched({"max_workers": 20, "max_pool_size": 5})

        with patch(
            "datahub.ingestion.source.sql.teradata.create_engine",
            return_value=MagicMock(),
        ):
            source._get_or_create_pooled_engine()

        assert source.config.max_workers == 20

    def test_effective_max_workers_unchanged_when_within_pool_size(self):
        """When max_workers <= max_pool_size, no capping occurs."""
        source = _create_source_patched({"max_workers": 5, "max_pool_size": 13})

        with patch(
            "datahub.ingestion.source.sql.teradata.create_engine",
            return_value=MagicMock(),
        ):
            source._get_or_create_pooled_engine()

        assert source._effective_max_workers == 5
        assert source.config.max_workers == 5


class TestTeradataReportFields:
    """Newly declared TeradataReport fields have the correct default values."""

    def test_concurrent_increments_are_atomic(self):
        """All lock-protected helpers produce exact counts under thread contention.

        N threads each call every increment_* / add_* helper M times.  Without
        the _lock, float += is not atomic under the GIL and int += can lose
        updates under high contention.  Any removal of 'with self._lock:' from
        a helper will cause this test to fail non-deterministically (and very
        reliably at n_threads=16, m_per_thread=1000).
        """
        report = TeradataReport()
        n_threads, m_per_thread = 16, 1000
        duration_per_call = 0.001  # 1 ms per add_column_extraction_duration call

        def worker(_: int) -> None:
            for _ in range(m_per_thread):
                report.increment_db_retries()
                report.increment_pool_timeout_retries()
                report.increment_columns_processed()
                report.increment_column_extraction_failures()
                report.increment_primary_keys_processed()
                report.add_column_extraction_duration(duration_per_call)

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            list(ex.map(worker, range(n_threads)))

        expected_count = n_threads * m_per_thread
        assert report.num_db_retries == expected_count
        assert report.num_pool_timeout_retries == expected_count
        assert report.num_columns_processed == expected_count
        assert report.num_column_extraction_failures == expected_count
        assert report.num_primary_keys_processed == expected_count
        assert report.column_extraction_duration_seconds == pytest.approx(
            expected_count * duration_per_call, rel=1e-6
        )


class TestConnectionPoolRetry:
    """Tests for connection-pool timeout config and pool-exhaustion retry/logging."""

    def test_connection_pool_timeout_ms_custom(self):
        """connection_pool_timeout_ms accepts a custom value."""
        config = TeradataConfig.model_validate(
            {**_base_config(), "connection_pool_timeout_ms": 120000}
        )
        assert config.connection_pool_timeout_ms == 120000

    def test_pool_exhaustion_logs_thread_context_and_retries(self, caplog):
        """_engine_connect_with_retry logs thread name + tid on PoolTimeoutError and retries."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        # Fail twice with PoolTimeoutError, then succeed on the third attempt.
        mock_engine.connect.side_effect = [
            PoolTimeoutError("pool exhausted"),
            PoolTimeoutError("pool exhausted"),
            mock_conn,
        ]

        report = TeradataReport()

        with (
            patch("time.sleep"),  # skip real backoff sleeps
            caplog.at_level(
                logging.WARNING, logger="datahub.ingestion.source.sql.teradata"
            ),
            _engine_connect_with_retry(mock_engine, report=report) as conn,
        ):
            assert conn is mock_conn

        # Two failures → two pool-timeout retries (attempts 0 and 1 sleep+increment).
        assert report.num_pool_timeout_retries == 2
        assert report.num_db_retries == 2

        # Both WARNING lines must carry thread name and tid.
        thread = current_thread()
        exhaustion_warnings = [
            r for r in caplog.records if "pool exhausted" in r.message.lower()
        ]
        assert len(exhaustion_warnings) == 2
        for record in exhaustion_warnings:
            assert thread.name in record.message
            assert str(thread.ident) in record.message

        mock_conn.close.assert_called_once()

    def test_pool_exhaustion_raises_after_max_attempts(self):
        """_engine_connect_with_retry re-raises PoolTimeoutError after all attempts fail."""
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = PoolTimeoutError("always exhausted")

        report = TeradataReport()

        with (
            patch("time.sleep"),
            pytest.raises(PoolTimeoutError),
            _engine_connect_with_retry(mock_engine, max_attempts=3, report=report),
        ):
            pass  # should never reach here

        # 2 retries (attempts 0 and 1 sleep+increment); attempt 2 raises without counting.
        assert report.num_pool_timeout_retries == 2
        assert report.num_db_retries == 2

    def test_connection_closed_on_normal_exit(self):
        """conn.close() is called after a successful with-block."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with _engine_connect_with_retry(mock_engine, max_attempts=1):
            pass

        mock_conn.close.assert_called_once()

    def test_connection_closed_when_body_raises(self):
        """conn.close() is called even when the with-block body raises an exception."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            pytest.raises(RuntimeError, match="body error"),
            _engine_connect_with_retry(mock_engine, max_attempts=1),
        ):
            raise RuntimeError("body error")

        mock_conn.close.assert_called_once()

    def test_pool_timeout_propagated_to_sqlalchemy_pool_timeout(self):
        """connection_pool_timeout_ms / 1000 is passed as pool_timeout to QueuePool."""
        config = TeradataConfig.model_validate(
            {**_base_config(), "connection_pool_timeout_ms": 90000}
        )

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.create_engine"
            ) as mock_create_engine,
            patch(
                "datahub.ingestion.source.sql.teradata.TeradataSource.cache_tables_and_views"
            ),
            patch(
                "datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator"
            ) as mock_agg_class,
        ):
            mock_agg_class.return_value = MagicMock()
            mock_create_engine.return_value = MagicMock()
            source = TeradataSource(config, PipelineContext(run_id="test"))
            source._get_or_create_pooled_engine()

        # Find the create_engine call that carried pool_timeout (the pooled engine call).
        pool_calls = [
            c
            for c in mock_create_engine.call_args_list
            if c[1].get("pool_timeout") is not None
        ]
        assert pool_calls, "create_engine was never called with pool_timeout"
        assert pool_calls[-1][1]["pool_timeout"] == 90.0  # 90000 ms → 90 s


class TestExecuteWithRetry:
    """_execute_with_retry retries server-side transient errors on the same connection."""

    def test_first_fail_then_succeed_returns_result(self):
        """A single retryable failure followed by success returns the result."""
        sentinel = object()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [
            DatabaseError("transaction aborted", None, None),
            sentinel,
        ]
        report = TeradataReport()

        with patch("time.sleep"):
            result = _execute_with_retry(
                mock_conn, "SELECT 1", max_attempts=2, report=report
            )

        assert result is sentinel
        assert mock_conn.execute.call_count == 2
        assert report.num_db_retries == 1

    def test_exhausts_all_attempts_and_reraises(self):
        """When every attempt raises a retryable error the last exception propagates."""
        exc = DatabaseError("transaction aborted", None, None)
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = exc

        with patch("time.sleep"), pytest.raises(DatabaseError):
            _execute_with_retry(mock_conn, "SELECT 1", max_attempts=3)

        assert mock_conn.execute.call_count == 3

    def test_dead_socket_error_not_retried(self):
        """Dead-socket errors (connection reset) propagate immediately without retry."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = OperationalError("connection reset", None, None)

        with patch("time.sleep"), pytest.raises(OperationalError):
            _execute_with_retry(mock_conn, "SELECT 1", max_attempts=3)

        # Must not retry — the socket is gone.
        assert mock_conn.execute.call_count == 1

    def test_deadlock_error_code_is_retried(self):
        """Deadlock error codes (e.g. [Error 2631]) trigger a retry on the same connection."""
        sentinel = object()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [
            DatabaseError("[Error 2631] deadlock", None, None),
            sentinel,
        ]
        report = TeradataReport()

        with patch("time.sleep"):
            result = _execute_with_retry(
                mock_conn, "SELECT 1", max_attempts=2, report=report
            )

        assert result is sentinel
        assert report.num_db_retries == 1

    def test_permanent_error_not_retried(self):
        """A non-retryable error (syntax error) propagates on the first attempt."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseError("syntax error", None, None)

        with patch("time.sleep"), pytest.raises(DatabaseError):
            _execute_with_retry(mock_conn, "SELECT 1", max_attempts=3)

        assert mock_conn.execute.call_count == 1

    def test_params_forwarded_to_execute(self):
        """Params dict is passed through to conn.execute on a successful call."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value = "ok"
        params = {"key": "val"}

        _execute_with_retry(mock_conn, "SELECT :key", params=params)

        mock_conn.execute.assert_called_once_with("SELECT :key", params)

    # ------------------------------------------------------------------
    # warn_on_permanent_failure flag
    # ------------------------------------------------------------------

    def test_permanent_error_emits_warning_by_default(self):
        """Default behaviour (warn_on_permanent_failure=True): a permanent first-attempt
        failure writes a report.warning so callers without try/except still surface
        a breadcrumb in the ingestion report."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = OperationalError(
            "[Error 3802] Database 'PDCRINFO' does not exist.", None, None
        )
        report = TeradataReport()

        with pytest.raises(OperationalError):
            _execute_with_retry(mock_conn, "SELECT 1", max_attempts=1, report=report)

        assert len(report.warnings) == 1
        warning_title = report.warnings[0].title
        assert warning_title is not None
        assert "Database execute failed" in warning_title

    def test_permanent_error_no_warning_when_suppressed(self):
        """warn_on_permanent_failure=False: probe callers that handle the exception
        themselves (e.g. _check_historical_table_exists) receive no report entry."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = OperationalError(
            "[Error 3802] Database 'PDCRINFO' does not exist.", None, None
        )
        report = TeradataReport()

        with pytest.raises(OperationalError):
            _execute_with_retry(
                mock_conn,
                "SELECT 1",
                max_attempts=1,
                report=report,
                warn_on_permanent_failure=False,
            )

        assert len(report.warnings) == 0

    def test_retry_exhaustion_always_emits_warning_even_when_flag_false(self):
        """warn_on_permanent_failure=False does not suppress the retry-exhaustion
        warning: if we actually slept and retried, a report entry is always warranted."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseError(
            "[Error 2631] transaction aborted", None, None
        )
        report = TeradataReport()

        with patch("time.sleep"), pytest.raises(DatabaseError):
            _execute_with_retry(
                mock_conn,
                "SELECT 1",
                max_attempts=2,
                report=report,
                warn_on_permanent_failure=False,
            )

        assert mock_conn.execute.call_count == 2
        assert len(report.warnings) == 1
        warning_title = report.warnings[0].title
        assert warning_title is not None
        assert "after retries" in warning_title


class TestFetchmanyWithRetry:
    """_fetchmany_with_retry mirrors _execute_with_retry semantics on cursor.fetchmany()."""

    def test_first_fail_then_succeed_returns_batch(self):
        """A single retryable failure is retried and the successful batch is returned."""
        batch = [object(), object()]
        mock_result = MagicMock()
        mock_result.fetchmany.side_effect = [
            DatabaseError("transaction aborted", None, None),
            batch,
        ]
        report = TeradataReport()

        with patch("time.sleep"):
            result = _fetchmany_with_retry(
                mock_result, batch_size=100, max_attempts=2, report=report
            )

        assert result is batch
        assert mock_result.fetchmany.call_count == 2
        assert report.num_db_retries == 1

    def test_exhausts_all_attempts_and_reraises(self):
        """When every attempt raises a retryable error the last exception propagates."""
        mock_result = MagicMock()
        mock_result.fetchmany.side_effect = DatabaseError(
            "transaction aborted", None, None
        )

        with patch("time.sleep"), pytest.raises(DatabaseError):
            _fetchmany_with_retry(mock_result, batch_size=100, max_attempts=3)

        assert mock_result.fetchmany.call_count == 3

    def test_non_retryable_error_propagates_immediately(self):
        """A non-retryable error propagates on the first attempt without retry."""
        mock_result = MagicMock()
        mock_result.fetchmany.side_effect = DatabaseError("syntax error", None, None)

        with patch("time.sleep"), pytest.raises(DatabaseError):
            _fetchmany_with_retry(mock_result, batch_size=100, max_attempts=3)

        assert mock_result.fetchmany.call_count == 1

    def test_batch_size_forwarded(self):
        """batch_size is passed through to result.fetchmany on every call."""
        mock_result = MagicMock()
        mock_result.fetchmany.return_value = []

        _fetchmany_with_retry(mock_result, batch_size=512)

        mock_result.fetchmany.assert_called_once_with(512)

    def test_report_counter_incremented_once_per_retry(self):
        """num_db_retries is incremented exactly once per retry attempt."""
        mock_result = MagicMock()
        mock_result.fetchmany.side_effect = [
            DatabaseError("transaction aborted", None, None),
            DatabaseError("transaction aborted", None, None),
            [],
        ]
        report = TeradataReport()

        with patch("time.sleep"):
            _fetchmany_with_retry(
                mock_result, batch_size=10, max_attempts=3, report=report
            )

        assert report.num_db_retries == 2

    def test_max_attempts_less_than_one_raises(self):
        """max_attempts=0 raises ValueError before any fetchmany call."""
        mock_result = MagicMock()
        with pytest.raises(ValueError, match="max_attempts"):
            _fetchmany_with_retry(mock_result, batch_size=10, max_attempts=0)
        mock_result.fetchmany.assert_not_called()


class TestBackoffTiming:
    """Retry helpers pass jittered backoff durations to time.sleep."""

    def test_execute_retry_sleeps_with_jittered_backoff(self):
        """_execute_with_retry passes the value from _jittered_backoff to time.sleep."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [
            DatabaseError("transaction aborted", None, None),
            "ok",
        ]
        fixed_backoff = 0.42

        with (
            patch(
                "datahub.ingestion.source.sql.teradata._jittered_backoff",
                return_value=fixed_backoff,
            ),
            patch("time.sleep") as mock_sleep,
        ):
            _execute_with_retry(mock_conn, "SELECT 1", max_attempts=2)

        mock_sleep.assert_called_once_with(fixed_backoff)

    def test_fetchmany_retry_sleeps_with_jittered_backoff(self):
        """_fetchmany_with_retry passes the value from _jittered_backoff to time.sleep."""
        batch: List[Any] = []
        mock_result = MagicMock()
        mock_result.fetchmany.side_effect = [
            DatabaseError("transaction aborted", None, None),
            batch,
        ]
        fixed_backoff = 0.77

        with (
            patch(
                "datahub.ingestion.source.sql.teradata._jittered_backoff",
                return_value=fixed_backoff,
            ),
            patch("time.sleep") as mock_sleep,
        ):
            _fetchmany_with_retry(mock_result, batch_size=10, max_attempts=2)

        mock_sleep.assert_called_once_with(fixed_backoff)

    def test_connect_retry_sleeps_with_jittered_backoff(self):
        """_engine_connect_with_retry passes the value from _jittered_backoff to time.sleep."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = [
            OperationalError("connect timed out", None, None),
            mock_conn,
        ]
        fixed_backoff = 1.23

        with (
            patch(
                "datahub.ingestion.source.sql.teradata._jittered_backoff",
                return_value=fixed_backoff,
            ),
            patch("time.sleep") as mock_sleep,
            _engine_connect_with_retry(mock_engine, max_attempts=2),
        ):
            pass

        mock_sleep.assert_called_once_with(fixed_backoff)

    def test_backoff_grows_with_attempt_number(self):
        """_jittered_backoff cap doubles with each attempt (exponential growth)."""
        caps = [1.0 * (2**attempt) for attempt in range(4)]
        for attempt, cap in enumerate(caps):
            # Patch uniform to return its upper bound, making the cap observable.
            with patch("random.uniform", side_effect=lambda lo, hi: hi):
                assert _jittered_backoff(attempt, 1.0) == cap

    def test_backoff_capped_at_30_seconds(self):
        """Cap kicks in once initial*2^attempt exceeds 30 s."""
        with patch("random.uniform", side_effect=lambda lo, hi: hi):
            # initial=20.0, attempt=2 -> raw = 80.0 -> capped to 30.0
            assert _jittered_backoff(2, 20.0) == 30.0
            # initial=5.0, attempt=10 -> raw = 5120.0 -> capped to 30.0
            assert _jittered_backoff(10, 5.0) == 30.0


class TestGetInspectorsDispose:
    """engine.dispose() is called regardless of how get_inspectors exits."""

    def _make_source_with_engine(self, mock_engine, databases=("db1",)):
        source = _create_source_patched(
            {"database": databases[0] if len(databases) == 1 else None}
        )
        if len(databases) > 1:
            source.config.databases = list(databases)
        mock_conn = MagicMock()
        mock_engine.connect.return_value = mock_conn
        return source

    def test_dispose_called_after_normal_exhaustion(self):
        """engine.dispose() is called after all databases have been yielded."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value = mock_conn

        source = _create_source_patched({"database": "db1"})

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.create_engine",
                return_value=mock_engine,
            ),
            patch("datahub.ingestion.source.sql.teradata.inspect"),
        ):
            list(source.get_inspectors())  # fully consume

        mock_engine.dispose.assert_called_once()

    def test_dispose_called_when_consumer_raises(self):
        """engine.dispose() is called even when the consuming loop raises mid-iteration."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value = mock_conn

        source = _create_source_patched({"databases": ["db1", "db2"]})

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.create_engine",
                return_value=mock_engine,
            ),
            patch("datahub.ingestion.source.sql.teradata.inspect"),
            pytest.raises(RuntimeError, match="consumer error"),
        ):
            for _ in source.get_inspectors():
                raise RuntimeError("consumer error")

        mock_engine.dispose.assert_called_once()

    def test_dispose_called_when_generator_abandoned(self):
        """engine.dispose() is called when the generator is GC'd without being consumed."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value = mock_conn

        source = _create_source_patched({"database": "db1"})

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.create_engine",
                return_value=mock_engine,
            ),
            patch("datahub.ingestion.source.sql.teradata.inspect"),
        ):
            gen = source.get_inspectors()
            next(gen)  # advance past the first yield
            gen.close()  # explicit close simulates GC / abandoned generator

        mock_engine.dispose.assert_called_once()


class TestGetInspectorsPerDbConnectionFailure:
    """get_inspectors() skips individual databases whose connection fails and
    continues to yield inspectors for the remaining databases.

    Regression guard for the split connect/yield fix: the try/except must only
    wrap the connect step, not the yield, so that:
      - A connection failure for db2 emits exactly one report.warning and
        skips that database.
      - Errors raised by the *consumer* while iterating db1 or db3 propagate
        normally and are NOT misclassified as connection failures.
      - db1 and db3 are still yielded despite db2's failure.
    """

    def test_connection_failure_for_one_db_skips_and_warns(self):
        """[db1 ok, db2 auth error, db3 ok] → yielded==[db1,db3], warnings==1."""
        source = _create_source_patched({"databases": ["db1", "db2", "db3"]})

        ok_conn = MagicMock()
        auth_error = OperationalError("authentication failed", None, None)

        mock_engine = MagicMock()
        # db1 succeeds, db2 fails with a permanent auth error, db3 succeeds.
        # _engine_connect_with_retry raises immediately on auth failures
        # (_should_retry_connect returns False), so each db makes exactly one
        # engine.connect() call.
        mock_engine.connect.side_effect = [ok_conn, auth_error, ok_conn]

        # Return a distinct inspector per call so _datahub_database is trackable.
        db1_inspector = MagicMock()
        db3_inspector = MagicMock()

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.create_engine",
                return_value=mock_engine,
            ),
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                side_effect=[db1_inspector, db3_inspector],
            ),
        ):
            inspectors = list(source.get_inspectors())

        # db1 and db3 were yielded; db2 was skipped
        assert len(inspectors) == 2
        yielded_dbs = [i._datahub_database for i in inspectors]
        assert yielded_dbs == ["db1", "db3"]

        # Exactly one warning emitted (for db2), none for db1 or db3
        assert len(source.report.warnings) == 1
        warning = source.report.warnings[0]
        assert warning.title == "Failed to inspect database"
        assert "db2" in warning.message

    def test_consumer_error_propagates_and_is_not_swallowed(self):
        """An exception raised inside the consumer loop is NOT caught by get_inspectors.

        Before the split-connect/yield fix, the try/except around ``yield``
        caught downstream errors, reclassified them as connection failures, and
        silently continued — masking the real problem. After the fix only the
        connect step is guarded; consumer errors propagate normally.
        """
        source = _create_source_patched({"databases": ["db1", "db2"]})

        mock_engine = MagicMock()
        mock_engine.connect.return_value = MagicMock()

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.create_engine",
                return_value=mock_engine,
            ),
            patch("datahub.ingestion.source.sql.teradata.inspect"),
            pytest.raises(AttributeError, match="downstream consumer bug"),
        ):
            for _ in source.get_inspectors():
                raise AttributeError("downstream consumer bug")

        # The error must NOT be misclassified as a connection failure warning
        assert len(source.report.warnings) == 0


class TestSchemaNameRetry:
    """_get_schema_names_with_retry() retries the connect+query sequence on transient errors."""

    def _make_engine(self, connect_results):
        """Return a mock engine whose connect() yields the given side effects."""
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = connect_results
        return mock_engine

    def test_retries_on_transient_error_then_succeeds(self, caplog):
        """A transient error on the first attempt is retried; the second succeeds."""
        source = _create_source_patched()

        good_conn = MagicMock()
        good_inspector = MagicMock()
        good_inspector.get_schema_names.return_value = ["db1", "db2"]

        # First connect raises a transient error; second succeeds.
        mock_engine = self._make_engine(
            [OperationalError("connect timed out", None, None), good_conn]
        )

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=good_inspector,
            ),
            patch("time.sleep"),
            caplog.at_level(
                logging.WARNING, logger="datahub.ingestion.source.sql.teradata"
            ),
        ):
            result = source._get_schema_names_with_retry(mock_engine)

        assert result == ["db1", "db2"]
        assert source.report.num_db_retries >= 1
        assert len(source.report.failures) == 0
        assert any("schema names" in r.message for r in caplog.records)

    def test_aborts_on_non_retryable_error(self):
        """A non-retryable error propagates immediately without retry."""
        source = _create_source_patched()

        mock_engine = self._make_engine([Exception("permission denied")])

        with (
            patch("time.sleep"),
            pytest.raises(Exception, match="permission denied"),
        ):
            source._get_schema_names_with_retry(mock_engine)

        # One connect attempt, immediately re-raised.
        assert mock_engine.connect.call_count == 1

    def test_exhausts_all_attempts_and_reraises(self):
        """When every attempt fails transiently the last exception is re-raised."""
        source = _create_source_patched({"retry_max_attempts": 2})

        transient = OperationalError("connect timed out", None, None)
        mock_engine = self._make_engine([transient, transient])

        with (
            patch("time.sleep"),
            pytest.raises(type(transient)),
        ):
            source._get_schema_names_with_retry(mock_engine)

        assert mock_engine.connect.call_count == 2
        assert source.report.num_db_retries == 1  # retried once, then gave up

    def test_retries_on_dead_socket_error(self):
        """Dead-socket errors (connection reset, broken pipe) are only handled by
        _should_retry_connect, not _should_retry — so the outer loop in
        _get_schema_names_with_retry must use _should_retry_connect."""
        source = _create_source_patched()

        good_conn = MagicMock()
        good_inspector = MagicMock()
        good_inspector.get_schema_names.return_value = ["db1"]

        # "connection reset" is in _RETRYABLE_CONNECT_EXTRA_SUBSTRINGS but NOT in
        # _RETRYABLE_ERROR_SUBSTRINGS, so _should_retry() would return False while
        # _should_retry_connect() returns True.
        dead_socket = OperationalError("connection reset by peer", None, None)
        mock_engine = self._make_engine([dead_socket, good_conn])

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=good_inspector,
            ),
            patch("time.sleep"),
        ):
            result = source._get_schema_names_with_retry(mock_engine)

        assert result == ["db1"]
        assert mock_engine.connect.call_count == 2
        assert source.report.num_db_retries >= 1


class TestHistoricalTableCheckLogging:
    """_check_historical_table_exists() logs at the right level depending on error type."""

    def test_transient_error_logs_warning(self, caplog):
        """When a transient error exhausts all retries the method logs a WARNING
        (not just INFO) so the operator knows the skip was caused by a connectivity
        problem, not a missing table."""
        source = _create_source_patched()

        # Simulate a transient error that survives all retry attempts.
        transient_exc = OperationalError("connect timed out", None, None)
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = transient_exc

        with (
            patch.object(source, "get_metadata_engine", return_value=mock_engine),
            patch("time.sleep"),
            caplog.at_level(
                logging.WARNING, logger="datahub.ingestion.source.sql.teradata"
            ),
        ):
            result = source._check_historical_table_exists()

        assert result is False
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("transient" in r.message.lower() for r in warning_records), (
            "Expected a WARNING mentioning 'transient' but got: "
            + str([r.message for r in warning_records])
        )
        # Transient failures must also surface in the ingestion report so the
        # operator sees them in the DataHub UI, not only in raw log output.
        assert any("transient" in w.message.lower() for w in source.report.warnings), (
            f"Expected report.warnings to mention 'transient', got: {source.report.warnings}"
        )

    def test_non_transient_error_logs_info_not_warning(self, caplog):
        """A genuine 'table not found' error should log at INFO, not WARNING."""
        source = _create_source_patched()

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Object does not exist")
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(source, "get_metadata_engine", return_value=mock_engine),
            patch("time.sleep"),
            caplog.at_level(
                logging.DEBUG, logger="datahub.ingestion.source.sql.teradata"
            ),
        ):
            result = source._check_historical_table_exists()

        assert result is False
        # Should have an INFO log, but NOT a WARNING about transient errors.
        assert not any(
            "transient" in r.message.lower() and r.levelno == logging.WARNING
            for r in caplog.records
        )
        # warn_on_permanent_failure=False is set in _check_historical_table_exists,
        # so no report.warning should leak through for an expected "table not found".
        assert len(source.report.warnings) == 0


class TestExecuteWithCursorFallback:
    """_execute_with_cursor_fallback() falls back to client-side buffering only
    when the error unambiguously signals that server-side cursors are
    unsupported.  All other errors must propagate.

    Regression guard for N16: the substring filter must require BOTH a
    "not supported / unsupported" token AND a "cursor / stream" token to
    co-occur, so SQL text that incidentally contains "cursor" or "stream"
    (e.g. a CTE named stream_events) is not mis-classified as a cursor-mode
    failure.
    """

    def _make_source(self, use_server_side_cursors: bool = True) -> TeradataSource:
        return _create_source_patched(
            {"use_server_side_cursors": use_server_side_cursors}
        )

    def _mock_conn(self) -> MagicMock:
        """Return a connection mock whose execution_options() returns a
        distinct streaming_conn object (used to verify which path is taken)."""
        conn = MagicMock()
        conn.execution_options.return_value = MagicMock()
        return conn

    def _retry_execute_raising(self, exc: Exception, fallback_result: MagicMock) -> Any:
        """side_effect for _retry_execute: raises on the streaming call
        (streaming_conn arg), returns fallback_result on the plain call."""
        calls: list = []

        def _side_effect(conn, stmt, **kwargs):
            calls.append(conn)
            if len(calls) == 1:
                raise exc
            return fallback_result

        return _side_effect

    # ------------------------------------------------------------------
    # Fallback cases — should silently switch to client-side buffering
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "msg",
        [
            "stream_results not supported by driver",
            "server-side cursor not supported",
            "unsupported: streaming cursor",
            "cursor mode is not supported",
            "streaming is unsupported",
        ],
    )
    def test_falls_back_on_cursor_not_supported_messages(self, msg: str) -> None:
        """OperationalError messages that mention both unsupported and cursor/stream
        trigger fallback and emit a warning — not a raise."""
        source = self._make_source()
        conn = self._mock_conn()
        fallback_result = MagicMock()
        exc = OperationalError(msg, None, None)

        with patch.object(
            source,
            "_retry_execute",
            side_effect=self._retry_execute_raising(exc, fallback_result),
        ) as mock_retry:
            result = source._execute_with_cursor_fallback(conn, "SELECT 1")

        assert result is fallback_result
        assert len(source.report.warnings) == 1
        # Second _retry_execute call must use the plain (non-streaming) connection
        assert mock_retry.call_count == 2
        second_call_conn = mock_retry.call_args_list[1][0][0]
        assert second_call_conn is conn

    # ------------------------------------------------------------------
    # Propagation cases — the B3 safety guard must NOT fall back
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "msg",
        [
            # SQL text incidentally containing "stream" without "not supported"
            # — the old single-token check would mis-classify this as cursor failure
            "SELECT * FROM stream_events WHERE id = 1",
            # SQL text incidentally containing "cursor" without "not supported"
            "CursorPos exceeded maximum allowed value",
            # Auth / permission errors — must always propagate
            "authentication failed",
            "permission denied",
            "access denied to database mydb",
            # Generic transient errors unrelated to cursor mode
            "connection reset by peer",
            "eof occurred in violation of protocol",
        ],
    )
    def test_propagates_non_cursor_errors(self, msg: str) -> None:
        """OperationalError messages that do NOT clearly indicate cursor-mode
        unsupported must propagate — never fall back to client-side buffering.

        This is the critical B3 safety guard: a permission-denied or auth error
        on the streaming execute must bubble up, not get swallowed as a
        "cursor not supported" warning.
        """
        source = self._make_source()
        conn = self._mock_conn()
        exc = OperationalError(msg, None, None)

        with (
            patch.object(
                source,
                "_retry_execute",
                side_effect=exc,  # always raises — fallback path must never be reached
            ),
            pytest.raises(OperationalError),
        ):
            source._execute_with_cursor_fallback(conn, "SELECT 1")

        # No fallback warning must have been emitted
        assert len(source.report.warnings) == 0

    def test_disabled_cursor_skips_streaming_entirely(self) -> None:
        """When use_server_side_cursors=False, the streaming path is never
        attempted — execution_options is never called."""
        source = self._make_source(use_server_side_cursors=False)
        conn = self._mock_conn()
        expected = MagicMock()

        with patch.object(source, "_retry_execute", return_value=expected):
            result = source._execute_with_cursor_fallback(conn, "SELECT 1")

        conn.execution_options.assert_not_called()
        assert result is expected


class TestCategorizeViewError:
    """Unit tests for _categorize_view_error."""

    def test_pool_timeout_error_is_timeout(self) -> None:
        assert _categorize_view_error(PoolTimeoutError("pool exhausted")) == "timeout"

    def test_python_timeout_error_is_timeout(self) -> None:
        assert _categorize_view_error(TimeoutError("timed out")) == "timeout"

    def test_operational_error_with_timeout_message_is_timeout(self) -> None:
        exc = OperationalError("request timed out", None, None)
        assert _categorize_view_error(exc) == "timeout"

    def test_database_error_with_io_timeout_is_timeout(self) -> None:
        exc = DatabaseError("i/o timeout during query", None, None)
        assert _categorize_view_error(exc) == "timeout"

    def test_permission_denied_substring_is_permission(self) -> None:
        exc = OperationalError("permission denied for table foo", None, None)
        assert _categorize_view_error(exc) == "permission"

    def test_access_denied_substring_is_permission(self) -> None:
        exc = DatabaseError("access denied to database bar", None, None)
        assert _categorize_view_error(exc) == "permission"

    def test_no_access_substring_is_permission(self) -> None:
        exc = DatabaseError("no access to object", None, None)
        assert _categorize_view_error(exc) == "permission"

    def test_teradata_permission_error_code_3523_is_permission(self) -> None:
        exc = DatabaseError("[Error 3523] user does not have SELECT access", None, None)
        assert _categorize_view_error(exc) == "permission"

    def test_teradata_auth_error_code_8017_is_permission(self) -> None:
        exc = OperationalError("[Error 8017] The UserId is invalid.", None, None)
        assert _categorize_view_error(exc) == "permission"

    def test_syntax_error_substring_is_parse(self) -> None:
        exc = DatabaseError("syntax error in SQL statement", None, None)
        assert _categorize_view_error(exc) == "parse"

    def test_parse_error_substring_is_parse(self) -> None:
        exc = DatabaseError("parse error near token SELECT", None, None)
        assert _categorize_view_error(exc) == "parse"

    def test_teradata_parse_error_code_3706_is_parse(self) -> None:
        exc = DatabaseError("[Error 3706] Syntax error: expected name.", None, None)
        assert _categorize_view_error(exc) == "parse"

    def test_teradata_parse_error_code_3707_is_parse(self) -> None:
        exc = DatabaseError(
            "[Error 3707] Syntax error, expected something between 'x' and 'y'.",
            None,
            None,
        )
        assert _categorize_view_error(exc) == "parse"

    def test_generic_exception_is_unknown(self) -> None:
        assert _categorize_view_error(RuntimeError("something exploded")) == "unknown"

    def test_value_error_is_unknown(self) -> None:
        assert _categorize_view_error(ValueError("bad value")) == "unknown"

    def test_timeout_takes_precedence_over_permission_keywords(self) -> None:
        """An error that mentions both 'timed out' and 'permission' is a timeout."""
        exc = OperationalError(
            "request timed out — permission check failed", None, None
        )
        assert _categorize_view_error(exc) == "timeout"


class TestErrorCategorizationReport:
    """Report counters are wired correctly to _categorize_view_error categories."""

    def test_new_fields_default_to_zero(self) -> None:
        report = TeradataReport()
        assert report.schema_discovery_failures == 0
        assert report.view_timeout_errors == 0
        assert report.view_parse_errors == 0
        assert report.view_permission_errors == 0
        assert report.view_unknown_errors == 0

    def test_increment_schema_discovery_failures(self) -> None:
        report = TeradataReport()
        report.increment_schema_discovery_failures()
        report.increment_schema_discovery_failures()
        assert report.schema_discovery_failures == 2

    def test_schema_discovery_failures_is_thread_safe(self) -> None:
        report = TeradataReport()
        n_threads, m_per_thread = 8, 500

        def worker(_: int) -> None:
            for _ in range(m_per_thread):
                report.increment_schema_discovery_failures()

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            list(ex.map(worker, range(n_threads)))

        assert report.schema_discovery_failures == n_threads * m_per_thread

    def test_view_error_counters_are_exclusive(self) -> None:
        """Each category increments exactly its own counter, leaving others at zero."""
        categories = {
            "timeout": "view_timeout_errors",
            "parse": "view_parse_errors",
            "permission": "view_permission_errors",
            "unknown": "view_unknown_errors",
        }
        for category, field_name in categories.items():
            report = TeradataReport()
            with report.atomic():
                if category == "timeout":
                    report.view_timeout_errors += 1
                elif category == "parse":
                    report.view_parse_errors += 1
                elif category == "permission":
                    report.view_permission_errors += 1
                else:
                    report.view_unknown_errors += 1

            assert getattr(report, field_name) == 1
            for other_field in categories.values():
                if other_field != field_name:
                    assert getattr(report, other_field) == 0, (
                        f"Expected {other_field}==0 when category='{category}'"
                    )

    def test_view_error_counters_are_thread_safe(self) -> None:
        """All four view error counters remain consistent under thread contention."""
        report = TeradataReport()
        n_threads, m_per_thread = 8, 250

        def worker(_: int) -> None:
            for _ in range(m_per_thread):
                with report.atomic():
                    report.view_timeout_errors += 1
                    report.view_parse_errors += 1
                    report.view_permission_errors += 1
                    report.view_unknown_errors += 1

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            list(ex.map(worker, range(n_threads)))

        expected = n_threads * m_per_thread
        assert report.view_timeout_errors == expected
        assert report.view_parse_errors == expected
        assert report.view_permission_errors == expected
        assert report.view_unknown_errors == expected


# ---------------------------------------------------------------------------
# Helpers shared by the end-to-end view / schema-discovery counter tests
# ---------------------------------------------------------------------------


def _make_mock_conn() -> MagicMock:
    """A minimal SQLAlchemy-compatible connection mock."""
    conn = MagicMock()
    conn.execute.return_value = MagicMock()
    return conn


def _mock_retry_connect(conn: MagicMock) -> Any:
    """Return a no-op context manager that yields *conn*."""

    @contextmanager
    def _cm(*_args, **_kwargs):
        yield conn

    return _cm


def _make_mock_inspector(schema: str) -> MagicMock:
    """Inspector mock with the _datahub_database attribute pre-set."""
    inspector = MagicMock()
    inspector._datahub_database = schema
    return inspector


class TestSchemaDiscoveryFailureCounter:
    """schema_discovery_failures is incremented exactly when _get_schema_names_with_retry fails."""

    def test_non_retryable_error_increments_counter(self):
        """A permanent (non-retryable) error increments schema_discovery_failures once."""
        source = _create_source_patched()
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("permission denied")

        with (
            patch("time.sleep"),
            pytest.raises(Exception, match="permission denied"),
        ):
            source._get_schema_names_with_retry(mock_engine)

        assert source.report.schema_discovery_failures == 1

    def test_all_retries_exhausted_increments_counter_once(self):
        """Even when every attempt fails transiently, the counter only goes up by 1."""
        source = _create_source_patched({"retry_max_attempts": 3})
        transient = OperationalError("connect timed out", None, None)
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = [transient, transient, transient]

        with (
            patch("time.sleep"),
            pytest.raises(type(transient)),
        ):
            source._get_schema_names_with_retry(mock_engine)

        assert source.report.schema_discovery_failures == 1

    def test_successful_discovery_does_not_increment_counter(self):
        """A successful schema discovery call leaves schema_discovery_failures at zero."""
        source = _create_source_patched()
        good_conn = MagicMock()
        good_inspector = MagicMock()
        good_inspector.get_schema_names.return_value = ["db1"]
        mock_engine = MagicMock()
        mock_engine.connect.return_value = good_conn

        with patch(
            "datahub.ingestion.source.sql.teradata.inspect",
            return_value=good_inspector,
        ):
            result = source._get_schema_names_with_retry(mock_engine)

        assert result == ["db1"]
        assert source.report.schema_discovery_failures == 0

    def test_transient_then_success_does_not_increment_counter(self):
        """A retry that eventually succeeds must not increment schema_discovery_failures."""
        source = _create_source_patched()
        good_conn = MagicMock()
        good_inspector = MagicMock()
        good_inspector.get_schema_names.return_value = ["db1"]
        transient = OperationalError("connect timed out", None, None)
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = [transient, good_conn]

        with (
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=good_inspector,
            ),
            patch("time.sleep"),
        ):
            result = source._get_schema_names_with_retry(mock_engine)

        assert result == ["db1"]
        assert source.report.schema_discovery_failures == 0


class TestViewProcessingErrorCounters:
    """num_view_processing_failures and per-category counters are updated correctly.

    Both the single-threaded path (_process_views_single_threaded, max_workers=1)
    and the multi-threaded path (_loop_views_with_connection_pool, max_workers>1)
    use _categorize_view_error to route failures to the right sub-counter.
    """

    # ------------------------------------------------------------------
    # Single-threaded path (_process_views_single_threaded)
    # ------------------------------------------------------------------

    def _run_single_threaded(
        self, source: "TeradataSource", view_exc: BaseException
    ) -> None:
        """Drive _process_views_single_threaded with one view that raises *view_exc*."""
        mock_conn = _make_mock_conn()
        mock_inspector = _make_mock_inspector("testdb")

        with (
            patch.object(source, "get_metadata_engine", return_value=MagicMock()),
            patch.object(
                source, "_retry_connect", side_effect=_mock_retry_connect(mock_conn)
            ),
            patch.object(source, "_retry_execute"),
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=mock_inspector,
            ),
            patch.object(source, "_process_view", side_effect=view_exc),
        ):
            # Consume the generator so the exception handler fires.
            list(
                source._process_views_single_threaded(
                    ["view_a"], "testdb", source.config
                )
            )

    def test_single_threaded_timeout_increments_timeout_counter(self):
        source = _create_source_patched({"max_workers": 1})
        self._run_single_threaded(source, PoolTimeoutError("pool exhausted"))

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_timeout_errors == 1
        assert source.report.view_parse_errors == 0
        assert source.report.view_permission_errors == 0
        assert source.report.view_unknown_errors == 0

    def test_single_threaded_permission_increments_permission_counter(self):
        source = _create_source_patched({"max_workers": 1})
        exc = OperationalError("permission denied for table foo", None, None)
        self._run_single_threaded(source, exc)

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_permission_errors == 1
        assert source.report.view_timeout_errors == 0
        assert source.report.view_parse_errors == 0
        assert source.report.view_unknown_errors == 0

    def test_single_threaded_parse_increments_parse_counter(self):
        source = _create_source_patched({"max_workers": 1})
        exc = DatabaseError("syntax error in SQL statement", None, None)
        self._run_single_threaded(source, exc)

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_parse_errors == 1
        assert source.report.view_timeout_errors == 0
        assert source.report.view_permission_errors == 0
        assert source.report.view_unknown_errors == 0

    def test_single_threaded_unknown_increments_unknown_counter(self):
        source = _create_source_patched({"max_workers": 1})
        self._run_single_threaded(source, RuntimeError("unexpected crash"))

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_unknown_errors == 1
        assert source.report.view_timeout_errors == 0
        assert source.report.view_parse_errors == 0
        assert source.report.view_permission_errors == 0

    def test_single_threaded_multiple_views_accumulate_counters(self):
        """Two views failing with different errors produce independent sub-counts."""
        source = _create_source_patched({"max_workers": 1})
        errors = [
            OperationalError("permission denied", None, None),
            DatabaseError("syntax error near token", None, None),
        ]
        mock_conn = _make_mock_conn()
        mock_inspector = _make_mock_inspector("testdb")

        with (
            patch.object(source, "get_metadata_engine", return_value=MagicMock()),
            patch.object(
                source, "_retry_connect", side_effect=_mock_retry_connect(mock_conn)
            ),
            patch.object(source, "_retry_execute"),
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=mock_inspector,
            ),
            patch.object(source, "_process_view", side_effect=errors),
        ):
            list(
                source._process_views_single_threaded(
                    ["view_a", "view_b"], "testdb", source.config
                )
            )

        assert source.report.num_view_processing_failures == 2
        assert source.report.view_permission_errors == 1
        assert source.report.view_parse_errors == 1
        assert source.report.view_timeout_errors == 0
        assert source.report.view_unknown_errors == 0

    def test_single_threaded_successful_view_does_not_increment_failures(self):
        """No counters change when all views are processed without error."""
        source = _create_source_patched({"max_workers": 1})
        mock_conn = _make_mock_conn()
        mock_inspector = _make_mock_inspector("testdb")

        with (
            patch.object(source, "get_metadata_engine", return_value=MagicMock()),
            patch.object(
                source, "_retry_connect", side_effect=_mock_retry_connect(mock_conn)
            ),
            patch.object(source, "_retry_execute"),
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=mock_inspector,
            ),
            patch.object(source, "_process_view", return_value=iter([])),
        ):
            list(
                source._process_views_single_threaded(
                    ["view_ok"], "testdb", source.config
                )
            )

        assert source.report.num_view_processing_failures == 0
        assert source.report.view_timeout_errors == 0
        assert source.report.view_parse_errors == 0
        assert source.report.view_permission_errors == 0
        assert source.report.view_unknown_errors == 0

    # ------------------------------------------------------------------
    # Multi-threaded path (_loop_views_with_connection_pool, max_workers > 1)
    # ------------------------------------------------------------------

    def _run_multi_threaded(
        self, source: "TeradataSource", view_exc: BaseException
    ) -> None:
        """Drive the multi-threaded path with one view that raises *view_exc*."""
        mock_conn = _make_mock_conn()
        mock_inspector = _make_mock_inspector("testdb")
        mock_engine = MagicMock()

        with (
            patch.object(
                source, "_get_or_create_pooled_engine", return_value=mock_engine
            ),
            patch.object(
                source, "_retry_connect", side_effect=_mock_retry_connect(mock_conn)
            ),
            patch.object(source, "_retry_execute"),
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=mock_inspector,
            ),
            patch.object(source, "_process_view", side_effect=view_exc),
        ):
            list(
                source._loop_views_with_connection_pool(
                    ["view_a"], "testdb", source.config
                )
            )

    def test_multi_threaded_timeout_increments_timeout_counter(self):
        source = _create_source_patched({"max_workers": 2})
        source._effective_max_workers = 2
        self._run_multi_threaded(source, PoolTimeoutError("pool exhausted"))

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_timeout_errors == 1
        assert source.report.view_parse_errors == 0
        assert source.report.view_permission_errors == 0
        assert source.report.view_unknown_errors == 0

    def test_multi_threaded_permission_increments_permission_counter(self):
        source = _create_source_patched({"max_workers": 2})
        source._effective_max_workers = 2
        exc = DatabaseError("[Error 3523] user has no SELECT access", None, None)
        self._run_multi_threaded(source, exc)

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_permission_errors == 1
        assert source.report.view_timeout_errors == 0
        assert source.report.view_parse_errors == 0
        assert source.report.view_unknown_errors == 0

    def test_multi_threaded_parse_increments_parse_counter(self):
        source = _create_source_patched({"max_workers": 2})
        source._effective_max_workers = 2
        exc = DatabaseError("[Error 3706] Syntax error in view definition", None, None)
        self._run_multi_threaded(source, exc)

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_parse_errors == 1
        assert source.report.view_timeout_errors == 0
        assert source.report.view_permission_errors == 0
        assert source.report.view_unknown_errors == 0

    def test_multi_threaded_unknown_increments_unknown_counter(self):
        source = _create_source_patched({"max_workers": 2})
        source._effective_max_workers = 2
        self._run_multi_threaded(source, ValueError("schema mismatch"))

        assert source.report.num_view_processing_failures == 1
        assert source.report.view_unknown_errors == 1
        assert source.report.view_timeout_errors == 0
        assert source.report.view_parse_errors == 0
        assert source.report.view_permission_errors == 0

    def test_multi_threaded_successful_view_does_not_increment_failures(self):
        source = _create_source_patched({"max_workers": 2})
        source._effective_max_workers = 2
        mock_conn = _make_mock_conn()
        mock_inspector = _make_mock_inspector("testdb")
        mock_engine = MagicMock()

        with (
            patch.object(
                source, "_get_or_create_pooled_engine", return_value=mock_engine
            ),
            patch.object(
                source, "_retry_connect", side_effect=_mock_retry_connect(mock_conn)
            ),
            patch.object(source, "_retry_execute"),
            patch(
                "datahub.ingestion.source.sql.teradata.inspect",
                return_value=mock_inspector,
            ),
            patch.object(source, "_process_view", return_value=iter([])),
        ):
            list(
                source._loop_views_with_connection_pool(
                    ["view_ok"], "testdb", source.config
                )
            )

        assert source.report.num_view_processing_failures == 0
        assert source.report.view_timeout_errors == 0
        assert source.report.view_parse_errors == 0
        assert source.report.view_permission_errors == 0
        assert source.report.view_unknown_errors == 0


# ---------------------------------------------------------------------------
# Helpers for _fetch_lineage_entries_chunked tests
# ---------------------------------------------------------------------------


def _make_lineage_source(
    extra_config: Optional[Dict[str, Any]] = None,
) -> TeradataSource:
    """Lineage source with time-window config set so _fetch_lineage_entries_chunked works."""
    cfg = {
        **_base_config(),
        "include_usage_statistics": True,
        "start_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "end_time": datetime(2024, 1, 2, tzinfo=timezone.utc),
        **(extra_config or {}),
    }
    return _create_source_patched(cfg)


@contextmanager
def _patch_lineage_fetch(
    source: TeradataSource,
    rows: list,
    query_sql: str = "SELECT 1",
    sleep_seconds: float = 0.0,
) -> Iterator[None]:
    """Context manager that patches the minimal set of methods so that
    _fetch_lineage_entries_chunked returns *rows* without hitting a real DB.

    *sleep_seconds* lets tests simulate a slow query by injecting a
    time.sleep() call inside the patched _execute_with_cursor_fallback.
    """
    mock_conn = _make_mock_conn()
    result_mock = MagicMock()
    # fetchmany: first call returns rows, second signals end-of-results.
    result_mock.fetchmany.side_effect = [rows, []]

    def _fake_execute(conn, sql, **kwargs):
        if sleep_seconds:
            time.sleep(sleep_seconds)
        return result_mock

    patches = [
        patch.object(source, "get_metadata_engine", return_value=MagicMock()),
        patch.object(
            source, "_retry_connect", side_effect=_mock_retry_connect(mock_conn)
        ),
        patch.object(
            source, "_execute_with_cursor_fallback", side_effect=_fake_execute
        ),
        patch.object(source, "_make_lineage_queries", return_value=[query_sql]),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield


class TestLineageQueryTimingReport:
    """lineage_query_timings and lineage_slow_queries_detected are populated
    correctly by _fetch_lineage_entries_chunked."""

    def test_timing_recorded_for_single_query(self):
        """After a successful fetch the timing dict has one entry."""
        source = _make_lineage_source()

        with _patch_lineage_fetch(source, rows=[("r1",), ("r2",)]):
            results = list(source._fetch_lineage_entries_chunked())

        assert results == [("r1",), ("r2",)]
        assert len(source.report.lineage_query_timings) == 1
        label, elapsed = next(iter(source.report.lineage_query_timings.items()))
        assert label.startswith("query_1 (")
        assert elapsed >= 0.0

    def test_label_is_current_only_when_no_pdcrinfo(self):
        """A query that does not reference PDCRINFO gets the 'current_only' label."""
        source = _make_lineage_source()
        current_sql = "SELECT * FROM DBC.QryLogV WHERE ts > '2024-01-01'"

        with _patch_lineage_fetch(source, rows=[], query_sql=current_sql):
            list(source._fetch_lineage_entries_chunked())

        assert "query_1 (current_only)" in source.report.lineage_query_timings

    def test_label_is_historical_union_when_pdcrinfo_present(self):
        """A query referencing PDCRINFO gets the 'historical_union' label."""
        source = _make_lineage_source()
        union_sql = (
            "SELECT * FROM PDCRINFO.DBQLSqlTbl_Hst UNION SELECT * FROM DBC.QryLogV"
        )

        with _patch_lineage_fetch(source, rows=[], query_sql=union_sql):
            list(source._fetch_lineage_entries_chunked())

        assert "query_1 (historical_union)" in source.report.lineage_query_timings

    def test_multiple_queries_produce_one_timing_entry_each(self):
        """When _make_lineage_queries returns N queries, N entries appear in the dict."""
        source = _make_lineage_source()
        mock_conn = _make_mock_conn()
        result_mock = MagicMock()
        result_mock.fetchmany.side_effect = [[], [], []]  # empty result for each query

        with (
            patch.object(source, "get_metadata_engine", return_value=MagicMock()),
            patch.object(
                source, "_retry_connect", side_effect=_mock_retry_connect(mock_conn)
            ),
            patch.object(
                source,
                "_execute_with_cursor_fallback",
                return_value=result_mock,
            ),
            patch.object(
                source,
                "_make_lineage_queries",
                return_value=[
                    "SELECT * FROM DBC.QryLogV",
                    "SELECT * FROM PDCRINFO.DBQLSqlTbl_Hst",
                ],
            ),
        ):
            list(source._fetch_lineage_entries_chunked())

        assert len(source.report.lineage_query_timings) == 2
        assert "query_1 (current_only)" in source.report.lineage_query_timings
        assert "query_2 (historical_union)" in source.report.lineage_query_timings

    def test_no_slow_query_detected_below_threshold(self):
        """A fast query does not increment lineage_slow_queries_detected."""
        source = _make_lineage_source({"lineage_slow_query_log_seconds": 60.0})

        with _patch_lineage_fetch(source, rows=[], sleep_seconds=0.0):
            list(source._fetch_lineage_entries_chunked())

        assert source.report.lineage_slow_queries_detected == 0

    def test_slow_query_increments_counter_and_warns(self, caplog):
        """A query exceeding the threshold increments the counter and logs a warning."""
        source = _make_lineage_source({"lineage_slow_query_log_seconds": 0.001})

        sql = "SELECT * FROM DBC.QryLogV WHERE ts > '2024-01-01'"
        with (
            _patch_lineage_fetch(source, rows=[], query_sql=sql, sleep_seconds=0.05),
            caplog.at_level(
                logging.WARNING, logger="datahub.ingestion.source.sql.teradata"
            ),
        ):
            list(source._fetch_lineage_entries_chunked())

        assert source.report.lineage_slow_queries_detected == 1

        slow_warnings = [r for r in caplog.records if "Slow lineage query" in r.message]
        assert len(slow_warnings) == 1
        warning_msg = slow_warnings[0].message
        assert "query_1 (current_only)" in warning_msg
        assert "threshold" in warning_msg

    def test_slow_query_warning_includes_sql_snippet(self, caplog):
        """The warning message contains a prefix of the SQL text."""
        source = _make_lineage_source({"lineage_slow_query_log_seconds": 0.001})

        distinctive = "SELECT distinctive_marker FROM DBC.QryLogV"
        with (
            _patch_lineage_fetch(
                source, rows=[], query_sql=distinctive, sleep_seconds=0.05
            ),
            caplog.at_level(
                logging.WARNING, logger="datahub.ingestion.source.sql.teradata"
            ),
        ):
            list(source._fetch_lineage_entries_chunked())

        slow_warnings = [r for r in caplog.records if "Slow lineage query" in r.message]
        assert len(slow_warnings) == 1
        assert "distinctive_marker" in slow_warnings[0].message

    def test_slow_query_sql_truncated_to_500_chars(self, caplog):
        """SQL longer than 500 characters is truncated with an ellipsis in the warning."""
        source = _make_lineage_source({"lineage_slow_query_log_seconds": 0.001})

        long_sql = "SELECT " + "x" * 600 + " FROM DBC.QryLogV"
        with (
            _patch_lineage_fetch(
                source, rows=[], query_sql=long_sql, sleep_seconds=0.05
            ),
            caplog.at_level(
                logging.WARNING, logger="datahub.ingestion.source.sql.teradata"
            ),
        ):
            list(source._fetch_lineage_entries_chunked())

        slow_warnings = [r for r in caplog.records if "Slow lineage query" in r.message]
        assert len(slow_warnings) == 1
        assert "…" in slow_warnings[0].message

    def test_disabled_threshold_never_warns(self, caplog):
        """Setting lineage_slow_query_log_seconds=0 disables slow-query logging."""
        source = _make_lineage_source({"lineage_slow_query_log_seconds": 0.0})

        with (
            _patch_lineage_fetch(source, rows=[], sleep_seconds=0.05),
            caplog.at_level(
                logging.WARNING, logger="datahub.ingestion.source.sql.teradata"
            ),
        ):
            list(source._fetch_lineage_entries_chunked())

        slow_warnings = [r for r in caplog.records if "Slow lineage query" in r.message]
        assert len(slow_warnings) == 0
        assert source.report.lineage_slow_queries_detected == 0

    def test_new_report_fields_default_to_zero(self):
        """TeradataReport defaults: empty timings dict, zero slow-query counter."""
        report = TeradataReport()
        assert report.lineage_query_timings == {}
        assert report.lineage_slow_queries_detected == 0

    def test_config_default_threshold_is_60_seconds(self):
        """lineage_slow_query_log_seconds defaults to 60.0."""
        config = TeradataConfig.model_validate(_base_config())
        assert config.lineage_slow_query_log_seconds == 60.0

    def test_config_threshold_accepts_zero(self):
        """lineage_slow_query_log_seconds=0 is accepted (disables the feature)."""
        config = TeradataConfig.model_validate(
            {**_base_config(), "lineage_slow_query_log_seconds": 0}
        )
        assert config.lineage_slow_query_log_seconds == 0.0
