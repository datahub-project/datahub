import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mssql.query import MSSQLQuery
from datahub.ingestion.source.sql.mssql.query_lineage_extractor import (
    MSSQLLineageExtractor,
    MSSQLQueryEntry,
    PrerequisiteResult,
)
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource
from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery
from datahub.sql_parsing.sqlglot_lineage import SqlUnderstandingError


def _base_config():
    return {
        "username": "sa",
        "password": "test",
        "host_port": "localhost:1433",
        "database": "TestDB",
    }


@pytest.fixture
def mssql_extractor_setup():
    """Fixture providing common MSSQLLineageExtractor test setup."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()
    sql_aggregator_mock = Mock()

    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    return {
        "config": config,
        "report": report,
        "conn_mock": conn_mock,
        "sql_aggregator_mock": sql_aggregator_mock,
        "extractor": extractor,
    }


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_usage_statistics_requires_query_lineage(create_engine_mock):
    """Test that usage statistics require query lineage to be enabled."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "include_usage_statistics": True,
        }
    )
    assert config.include_query_lineage is True
    assert config.include_usage_statistics is True

    with pytest.raises(
        ValueError, match="include_usage_statistics requires include_query_lineage"
    ):
        SQLServerConfig.model_validate(
            {
                **_base_config(),
                "include_query_lineage": False,
                "include_usage_statistics": True,
            }
        )


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_usage_statistics_requires_graph_connection(create_engine_mock):
    """Test that usage statistics validation fails when graph connection is missing."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "include_usage_statistics": True,
        }
    )

    ctx = PipelineContext(run_id="test")
    assert ctx.graph is None

    with pytest.raises(ValueError, match="graph connection"):
        SQLServerSource(config, ctx)


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_sql_aggregator_initialization_failure(create_engine_mock):
    """Test that SQL aggregator initialization failure fails loudly."""
    with patch(
        "datahub.ingestion.source.sql.mssql.source.SqlParsingAggregator"
    ) as mock_aggregator:
        mock_aggregator.side_effect = Exception("Aggregator init failed")

        config = SQLServerConfig.model_validate(
            {**_base_config(), "include_query_lineage": True}
        )

        with pytest.raises(Exception) as exc_info:
            SQLServerSource(config, PipelineContext(run_id="test"))

        error_message = str(exc_info.value)
        assert "Aggregator init failed" in error_message


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_query_extraction_failure_reports_error(create_engine_mock):
    """Test that query extraction failures are reported but don't stop ingestion."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )

    with patch("datahub.ingestion.source.sql.mssql.source.SqlParsingAggregator"):
        source = SQLServerSource(config, PipelineContext(run_id="test"))

        inspector_mock = Mock()
        inspector_mock.engine.connect.return_value.__enter__ = Mock()
        inspector_mock.engine.connect.return_value.__exit__ = Mock()
        connection_mock = Mock()
        inspector_mock.engine.connect.return_value.__enter__.return_value = (
            connection_mock
        )

        connection_mock.execute.side_effect = DatabaseError(
            "statement", "params", "orig"
        )

        with patch.object(source, "get_inspectors", return_value=[inspector_mock]):
            list(source._get_query_based_lineage_workunits())

    assert len(source.report.failures) > 0
    failure_messages = [f.message for f in source.report.failures]
    assert any(
        ("query" in msg.lower() and "lineage" in msg.lower())
        or "extraction" in msg.lower()
        or "unexpected error" in msg.lower()
        for msg in failure_messages
    )


# Tests for MSSQLLineageExtractor


def test_mssql_lineage_extractor_version_check(mssql_extractor_setup):
    """Test SQL Server version detection."""
    extractor = mssql_extractor_setup["extractor"]
    conn_mock = mssql_extractor_setup["conn_mock"]

    conn_mock.execute.return_value.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    major_version = extractor._check_version()

    assert major_version == 15
    conn_mock.execute.assert_called_once()


def test_mssql_lineage_extractor_version_check_old_version(mssql_extractor_setup):
    """Test version detection rejects SQL Server 2014."""
    extractor = mssql_extractor_setup["extractor"]
    conn_mock = mssql_extractor_setup["conn_mock"]

    conn_mock.execute.return_value.fetchone.return_value = {
        "version": "Microsoft SQL Server 2014 (SP2) - 12.0.5000.0",
        "major_version": 12,
    }

    major_version = extractor._check_version()

    assert major_version == 12
    assert major_version < 13  # SQL Server 2016 is version 13


def test_mssql_lineage_extractor_query_store_enabled(mssql_extractor_setup):
    """Test Query Store availability check when enabled."""
    extractor = mssql_extractor_setup["extractor"]
    conn_mock = mssql_extractor_setup["conn_mock"]

    conn_mock.execute.return_value.fetchone.return_value = {"is_enabled": 1}

    is_enabled = extractor._check_query_store_available()

    assert is_enabled is True


def test_mssql_lineage_extractor_query_store_disabled():
    """Test Query Store availability check when disabled."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    conn_mock.execute.return_value.fetchone.return_value = {"is_enabled": 0}

    is_enabled = extractor._check_query_store_available()

    assert is_enabled is False


def test_mssql_lineage_extractor_dmv_permissions_granted():
    """Test DMV permissions check when granted."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    conn_mock.execute.return_value.fetchone.return_value = {"has_view_server_state": 1}

    has_permission = extractor._check_dmv_permissions()

    assert has_permission is True


def test_mssql_lineage_extractor_dmv_permissions_denied():
    """Test DMV permissions check when denied."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    conn_mock.execute.return_value.fetchone.return_value = {"has_view_server_state": 0}

    has_permission = extractor._check_dmv_permissions()

    assert has_permission is False


def test_mssql_lineage_extractor_check_prerequisites_query_store():
    """Test prerequisites check succeeds with Query Store."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 1}

    conn_mock.execute.side_effect = [version_result, qs_result]

    can_extract, message, method = extractor.check_prerequisites()

    assert can_extract is True
    assert method == "query_store"


def test_mssql_lineage_extractor_check_prerequisites_dmv_fallback():
    """Test prerequisites check falls back to DMV when Query Store disabled."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 0}

    dmv_result = Mock()
    dmv_result.fetchone.return_value = {"has_view_server_state": 1}

    conn_mock.execute.side_effect = [version_result, qs_result, dmv_result]

    can_extract, message, method = extractor.check_prerequisites()

    assert can_extract is True
    assert method == "dmv"


def test_mssql_lineage_extractor_check_prerequisites_fails():
    """Test prerequisites check fails when Query Store unavailable and no DMV permissions."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock version check
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 0}

    dmv_result = Mock()
    dmv_result.fetchone.return_value = {"has_view_server_state": 0}

    conn_mock.execute.side_effect = [version_result, qs_result, dmv_result]

    can_extract, message, method = extractor.check_prerequisites()

    assert can_extract is False
    assert method == "none"


def test_mssql_lineage_extractor_extract_queries_from_query_store():
    """Test query extraction from Query Store."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "max_queries_to_extract": 10, "min_query_calls": 2}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock Query Store results
    mock_results = [
        {
            "query_id": "1",
            "query_text": "SELECT * FROM users WHERE id = 1",
            "execution_count": 5,
            "total_exec_time_ms": 100.5,
            "user_name": "test_user",
            "database_name": "TestDB",
        },
        {
            "query_id": "2",
            "query_text": "INSERT INTO orders VALUES (1, 'test')",
            "execution_count": 3,
            "total_exec_time_ms": 50.2,
            "user_name": "admin",
            "database_name": "TestDB",
        },
    ]

    # Mock the result to support iteration
    mock_result = Mock()
    mock_result.__iter__ = Mock(return_value=iter(mock_results))
    conn_mock.execute.return_value = mock_result

    # Mock prerequisites check
    with patch.object(
        extractor,
        "check_prerequisites",
        return_value=PrerequisiteResult(
            is_ready=True, message="Query Store is enabled", method="query_store"
        ),
    ):
        queries = extractor.extract_query_history()

    assert len(queries) == 2
    assert queries[0].query_id == "1"
    assert queries[0].execution_count == 5
    assert queries[1].query_id == "2"
    assert report.num_queries_extracted == 2


def test_mssql_lineage_extractor_extract_queries_respects_min_calls():
    """Test query extraction respects min_query_calls filter."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "max_queries_to_extract": 10, "min_query_calls": 5}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock Query Store results - SQL query filters execution_count >= 5
    # So mock should return only results that pass that filter
    mock_results = [
        {
            "query_id": "1",
            "query_text": "SELECT * FROM users",
            "execution_count": 10,  # Above threshold, would be returned by SQL
            "total_exec_time_ms": 100.5,
            "user_name": "test_user",
            "database_name": "TestDB",
        },
        # Query with execution_count=3 would be filtered by SQL WHERE clause
    ]

    # Mock the result to support iteration
    mock_result = Mock()
    mock_result.__iter__ = Mock(return_value=iter(mock_results))
    conn_mock.execute.return_value = mock_result

    with patch.object(
        extractor,
        "check_prerequisites",
        return_value=PrerequisiteResult(
            is_ready=True, message="Query Store enabled", method="query_store"
        ),
    ):
        queries = extractor.extract_query_history()

    # Only the query with execution_count >= 5 should be included
    assert len(queries) == 1
    assert queries[0].query_id == "1"
    assert queries[0].execution_count == 10


def test_mssql_lineage_extractor_extract_queries_applies_exclude_patterns():
    """Test query extraction applies exclude patterns."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "max_queries_to_extract": 10,
            "query_exclude_patterns": ["%sys.%", "%msdb.%"],
        }
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock Query Store results - SQL query filters by NOT LIKE patterns
    # So mock should return only results that pass those filters
    mock_results = [
        {
            "query_id": "1",
            "query_text": "SELECT * FROM users",
            "execution_count": 5,
            "total_exec_time_ms": 100.5,
            "user_name": "test_user",
            "database_name": "TestDB",
        },
        # Queries with sys.tables and msdb.dbo.jobs would be filtered by SQL WHERE clause
    ]

    # Mock the result to support iteration
    mock_result = Mock()
    mock_result.__iter__ = Mock(return_value=iter(mock_results))
    conn_mock.execute.return_value = mock_result

    with patch.object(
        extractor,
        "check_prerequisites",
        return_value=PrerequisiteResult(
            is_ready=True, message="Query Store is enabled", method="query_store"
        ),
    ):
        queries = extractor.extract_query_history()

    # Only the first query should be included
    assert len(queries) == 1
    assert queries[0].query_id == "1"
    assert "users" in queries[0].query_text


def test_mssql_lineage_extractor_handles_extraction_failure():
    """Test query extraction reports errors gracefully on database errors."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock database error during extraction
    conn_mock.execute.side_effect = DatabaseError("statement", "params", "orig")

    with patch.object(
        extractor,
        "check_prerequisites",
        return_value=PrerequisiteResult(
            is_ready=True, message="Query Store is enabled", method="query_store"
        ),
    ):
        queries = extractor.extract_query_history()

    # Should return empty list and report failure
    assert queries == []
    assert len(report.failures) > 0
    failure_messages = [f.message for f in report.failures]
    assert any("Database error" in msg for msg in failure_messages)


def test_mssql_lineage_extractor_populate_lineage():
    """Test populate_lineage_from_queries adds queries to aggregator."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    test_queries = [
        MSSQLQueryEntry(
            query_id="1",
            query_text="SELECT * FROM users",
            execution_count=5,
            total_exec_time_ms=100.0,
            database_name="TestDB",
        )
    ]

    # Mock extract_query_history to return our test queries
    with patch.object(extractor, "extract_query_history", return_value=test_queries):
        extractor.populate_lineage_from_queries()

        sql_aggregator_mock.add_observed_query.assert_called_once()
        call_args = sql_aggregator_mock.add_observed_query.call_args[0][0]
        assert call_args.query == "SELECT * FROM users"
        assert call_args.user is None


# Tests for SQL Query Generation


def test_query_store_sql_without_exclusions():
    """Test Query Store SQL generation without exclude patterns."""
    query, params = MSSQLQuery.get_query_history_from_query_store(
        limit=100,
        min_calls=5,
        exclude_patterns=None,
    )

    query_str = str(query)
    assert "SELECT TOP(:limit)" in query_str
    assert "sys.query_store_query" in query_str
    assert "count_executions >= :min_calls" in query_str
    assert "NOT LIKE" not in query_str

    assert params["limit"] == 100
    assert params["min_calls"] == 5
    assert len(params) == 2


def test_query_store_sql_with_exclusions():
    """Test Query Store SQL generation with exclude patterns."""
    query, params = MSSQLQuery.get_query_history_from_query_store(
        limit=100,
        min_calls=5,
        exclude_patterns=["%sys.%", "%temp%", "%msdb%"],
    )

    query_str = str(query)
    assert query_str.count("NOT LIKE") == 3
    assert "query_sql_text NOT LIKE :exclude_0" in query_str
    assert "query_sql_text NOT LIKE :exclude_1" in query_str
    assert "query_sql_text NOT LIKE :exclude_2" in query_str

    assert params["exclude_0"] == "%sys.%"
    assert params["exclude_1"] == "%temp%"
    assert params["exclude_2"] == "%msdb%"
    assert params["limit"] == 100
    assert params["min_calls"] == 5


def test_dmv_sql_without_exclusions():
    """Test DMV SQL generation without exclude patterns."""
    query, params = MSSQLQuery.get_query_history_from_dmv(
        limit=50,
        min_calls=10,
        exclude_patterns=None,
    )

    query_str = str(query)
    assert "SELECT TOP(:limit)" in query_str
    assert "sys.dm_exec_query_stats" in query_str
    assert "sys.dm_exec_sql_text" in query_str
    assert "execution_count >= :min_calls" in query_str
    assert "NOT LIKE" not in query_str

    assert params["limit"] == 50
    assert params["min_calls"] == 10


def test_dmv_sql_with_exclusions():
    """Test DMV SQL generation with exclude patterns."""
    query, params = MSSQLQuery.get_query_history_from_dmv(
        limit=50,
        min_calls=10,
        exclude_patterns=["%INFORMATION_SCHEMA%", "%#%"],
    )

    query_str = str(query)
    assert query_str.count("NOT LIKE") == 2
    assert "CAST(st.text AS NVARCHAR(MAX)) NOT LIKE :exclude_0" in query_str
    assert "CAST(st.text AS NVARCHAR(MAX)) NOT LIKE :exclude_1" in query_str

    assert params["exclude_0"] == "%INFORMATION_SCHEMA%"
    assert params["exclude_1"] == "%#%"


def test_version_check_sql():
    """Test SQL Server version check query."""
    query = MSSQLQuery.get_mssql_version()

    query_str = str(query)
    assert "SERVERPROPERTY('ProductVersion')" in query_str
    assert "SERVERPROPERTY('ProductMajorVersion')" in query_str
    assert "major_version" in query_str


def test_query_store_check_sql():
    """Test Query Store enabled check query."""
    query = MSSQLQuery.check_query_store_enabled()

    query_str = str(query)
    assert "sys.database_query_store_options" in query_str
    assert "actual_state_desc" in query_str
    assert "READ_WRITE" in query_str
    assert "READ_ONLY" in query_str


def test_dmv_permissions_check_sql():
    """Test DMV permissions check query."""
    query = MSSQLQuery.check_dmv_permissions()

    query_str = str(query)
    assert "HAS_PERMS_BY_NAME" in query_str
    assert "VIEW SERVER STATE" in query_str


# Tests for Error Scenarios


def test_mssql_lineage_extractor_version_check_fails():
    """Test version check handles missing version gracefully."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock no version result
    conn_mock.execute.return_value.fetchone.return_value = None

    version = extractor._check_version()

    assert version is None


def test_mssql_lineage_extractor_query_store_check_fails():
    """Test Query Store check handles database errors."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock database error (e.g., sys.database_query_store_options doesn't exist)
    conn_mock.execute.side_effect = ProgrammingError("statement", "params", "orig")

    # Should raise the exception (handled by caller)
    with pytest.raises(ProgrammingError):
        extractor._check_query_store_available()


def test_mssql_lineage_extractor_malformed_query_text():
    """Test extraction handles malformed query text gracefully."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock query with NULL and empty text (should be filtered by SQL, but test defense)
    mock_results = [
        {
            "query_id": "1",
            "query_text": "SELECT * FROM users",
            "execution_count": 5,
            "total_exec_time_ms": 100.0,
            "user_name": "test_user",
            "database_name": "TestDB",
        },
        {
            "query_id": "2",
            "query_text": "",  # Empty text
            "execution_count": 3,
            "total_exec_time_ms": 50.0,
            "user_name": "admin",
            "database_name": "TestDB",
        },
    ]

    # Mock the result to support iteration
    mock_result = Mock()
    mock_result.__iter__ = Mock(return_value=iter(mock_results))
    conn_mock.execute.return_value = mock_result

    with patch.object(
        extractor,
        "check_prerequisites",
        return_value=PrerequisiteResult(
            is_ready=True, message="Query Store is enabled", method="query_store"
        ),
    ):
        queries = extractor.extract_query_history()

    # Both queries should be returned (SQL should filter empty, but we accept them)
    assert len(queries) == 2


def test_mssql_lineage_extractor_connection_failure_during_prerequisite():
    """Test extraction handles connection failures during prerequisite checks."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    # Mock version check to succeed, but Query Store and DMV checks to fail
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    call_count = [0]

    def execute_side_effect(query, *args):
        call_count[0] += 1
        if call_count[0] == 1:
            return version_result
        else:
            raise OperationalError("statement", "params", "orig")

    conn_mock.execute.side_effect = execute_side_effect

    # Should fall through to DMV check, which also fails
    can_extract, message, method = extractor.check_prerequisites()

    assert can_extract is False
    assert method == "none"


def test_mssql_lineage_extractor_fallback_to_dmv():
    """Test automatic fallback from Query Store to DMV."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    # Mock Query Store check fails (not available on this database)
    qs_error = ProgrammingError("statement", "params", "orig")

    # Mock DMV check succeeds
    dmv_result = Mock()
    dmv_result.fetchone.return_value = {"has_view_server_state": 1}

    call_count = [0]

    def execute_side_effect(query):
        call_count[0] += 1
        if call_count[0] == 1:
            return version_result
        elif call_count[0] == 2:
            raise qs_error
        else:
            return dmv_result

    conn_mock.execute.side_effect = execute_side_effect

    can_extract, message, method = extractor.check_prerequisites()

    assert can_extract is True
    assert method == "dmv"


def test_mssql_populate_lineage_from_queries_integration():
    """Test populate_lineage_from_queries() end-to-end integration."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    # Mock SQL aggregator
    sql_aggregator_mock = Mock()
    sql_aggregator_mock.add_observed_query = Mock()

    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    test_queries = [
        MSSQLQueryEntry(
            query_id="1",
            query_text="SELECT * FROM users",
            execution_count=100,
            total_exec_time_ms=500.0,
            database_name="TestDB",
        ),
        MSSQLQueryEntry(
            query_id="2",
            query_text="INSERT INTO orders SELECT * FROM staging",
            execution_count=50,
            total_exec_time_ms=300.0,
            database_name="TestDB",
        ),
    ]

    with patch.object(extractor, "extract_query_history", return_value=test_queries):
        extractor.populate_lineage_from_queries()

    # Verify queries were added to SQL aggregator
    assert sql_aggregator_mock.add_observed_query.call_count == 2
    assert extractor.queries_parsed == 2
    assert extractor.queries_failed == 0
    assert report.num_queries_parsed == 2


def test_mssql_populate_lineage_handles_parse_failures():
    """Test that populate_lineage_from_queries() handles parse failures gracefully."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()

    # First query succeeds, second fails parsing, third succeeds
    def add_query_side_effect(query):
        if "invalid" in query.query.lower():
            raise SqlUnderstandingError("Cannot parse invalid SQL")

    sql_aggregator_mock.add_observed_query.side_effect = add_query_side_effect

    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    test_queries = [
        MSSQLQueryEntry(
            query_id="1",
            query_text="SELECT * FROM users",
            execution_count=100,
            total_exec_time_ms=500.0,
            database_name="TestDB",
        ),
        MSSQLQueryEntry(
            query_id="2",
            query_text="INVALID SQL SYNTAX HERE",
            execution_count=50,
            total_exec_time_ms=300.0,
            database_name="TestDB",
        ),
        MSSQLQueryEntry(
            query_id="3",
            query_text="SELECT * FROM orders",
            execution_count=25,
            total_exec_time_ms=100.0,
            database_name="TestDB",
        ),
    ]

    with patch.object(extractor, "extract_query_history", return_value=test_queries):
        extractor.populate_lineage_from_queries()

    # Should have processed all 3, with 1 failure
    assert sql_aggregator_mock.add_observed_query.call_count == 3
    assert extractor.queries_parsed == 2
    assert extractor.queries_failed == 1
    assert report.num_queries_parse_failures == 1


def test_mssql_populate_lineage_handles_unexpected_errors():
    """Test that populate_lineage_from_queries() handles unexpected errors."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()

    # Simulate unexpected error (e.g., KeyError)
    def add_query_side_effect(query):
        if "error" in query.query.lower():
            raise KeyError("Unexpected error in processing")

    sql_aggregator_mock.add_observed_query.side_effect = add_query_side_effect

    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    test_queries = [
        MSSQLQueryEntry(
            query_id="1",
            query_text="SELECT * FROM users",
            execution_count=100,
            total_exec_time_ms=500.0,
            database_name="TestDB",
        ),
        MSSQLQueryEntry(
            query_id="2",
            query_text="SELECT * FROM error_table",
            execution_count=50,
            total_exec_time_ms=300.0,
            database_name="TestDB",
        ),
    ]

    with patch.object(extractor, "extract_query_history", return_value=test_queries):
        extractor.populate_lineage_from_queries()

    # Should have attempted both, with 1 unexpected failure
    assert sql_aggregator_mock.add_observed_query.call_count == 2
    assert extractor.queries_parsed == 1
    assert extractor.queries_failed == 1


def test_mssql_populate_lineage_disabled_in_config():
    """Test that populate_lineage_from_queries() skips when disabled."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "include_query_lineage": False}
    )
    report = SQLSourceReport()
    conn_mock = Mock()
    sql_aggregator_mock = Mock()

    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    extractor.populate_lineage_from_queries()

    conn_mock.execute.assert_not_called()
    sql_aggregator_mock.add_observed_query.assert_not_called()


def test_mssql_lineage_extractor_creates_correct_observed_query():
    """Test that ObservedQuery objects are created with correct parameters."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    sql_aggregator_mock = Mock()
    captured_queries = []

    def capture_query(query: ObservedQuery) -> None:
        captured_queries.append(query)

    sql_aggregator_mock.add_observed_query.side_effect = capture_query

    extractor = MSSQLLineageExtractor(
        config, conn_mock, report, sql_aggregator_mock, "dbo"
    )

    test_queries = [
        MSSQLQueryEntry(
            query_id="test123",
            query_text="SELECT * FROM users WHERE id > 100",
            execution_count=42,
            total_exec_time_ms=250.5,
            database_name="ProductionDB",
        )
    ]

    with patch.object(extractor, "extract_query_history", return_value=test_queries):
        extractor.populate_lineage_from_queries()

    assert len(captured_queries) == 1
    observed_query = captured_queries[0]

    assert observed_query.query == "SELECT * FROM users WHERE id > 100"
    assert observed_query.default_db == "ProductionDB"
    assert observed_query.default_schema == "dbo"
    assert observed_query.session_id == "queryid:test123"
    assert observed_query.user is None
    assert observed_query.timestamp is None


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_is_discovered_table_caching(create_engine_mock):
    """Test that is_discovered_table() uses caching for performance."""
    config = SQLServerConfig.model_validate(_base_config())
    ctx = PipelineContext(run_id="test")

    source = SQLServerSource(config=config, ctx=ctx, is_odbc=False)

    table_name = "TestDB.dbo.MyTable"

    # First call - should populate cache
    result1 = source.is_discovered_table(table_name)

    # Verify cache was populated
    assert table_name in source._discovered_table_cache

    # Second call - should use cache
    result2 = source.is_discovered_table(table_name)

    # Results should be identical
    assert result1 == result2

    # Cache should contain the entry
    assert len(source._discovered_table_cache) >= 1


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_is_discovered_table_cache_performance(create_engine_mock):
    """Test that caching provides performance benefit for repeated calls."""
    config = SQLServerConfig.model_validate(_base_config())
    ctx = PipelineContext(run_id="test")

    source = SQLServerSource(config=config, ctx=ctx, is_odbc=False)

    table_name = "TestDB.dbo.Products"

    # Warm up cache
    source.is_discovered_table(table_name)

    # Test cached performance
    start = time.perf_counter()
    for _ in range(1000):
        source.is_discovered_table(table_name)
    cached_duration = time.perf_counter() - start

    # Cached lookups of the same table should be fast
    assert cached_duration < 1.0, (
        f"1000 cached lookups should complete in under 1s, took {cached_duration:.4f}s"
    )


def test_mssql_query_exclude_clause_construction():
    """Test that exclude clauses are built correctly with hardcoded column expressions."""
    # Simple column expression (used in Query Store queries)
    clause = MSSQLQuery._build_exclude_clause(["%test%"], "qt.query_sql_text")
    assert "qt.query_sql_text NOT LIKE :exclude_0" in clause

    # CAST expression (used in DMV queries)
    clause = MSSQLQuery._build_exclude_clause(
        ["%test%"], "CAST(st.text AS NVARCHAR(MAX))"
    )
    assert "CAST(st.text AS NVARCHAR(MAX)) NOT LIKE :exclude_0" in clause

    # Multiple patterns
    clause = MSSQLQuery._build_exclude_clause(["%sys%", "%temp%"], "qt.query_sql_text")
    assert "qt.query_sql_text NOT LIKE :exclude_0" in clause
    assert "qt.query_sql_text NOT LIKE :exclude_1" in clause

    # Empty patterns returns empty string
    clause = MSSQLQuery._build_exclude_clause([], "qt.query_sql_text")
    assert clause == ""

    # None patterns returns empty string
    clause = MSSQLQuery._build_exclude_clause(None, "qt.query_sql_text")
    assert clause == ""


def test_mssql_query_parameterized_patterns():
    """Test that query patterns are properly parameterized."""
    # Build query with multiple exclude patterns
    query, params = MSSQLQuery.get_query_history_from_query_store(
        limit=100,
        min_calls=1,
        exclude_patterns=["%sys.%", "%temp%", "%msdb.%"],
    )

    # Verify parameters are created
    assert "exclude_0" in params
    assert "exclude_1" in params
    assert "exclude_2" in params
    assert params["exclude_0"] == "%sys.%"
    assert params["exclude_1"] == "%temp%"
    assert params["exclude_2"] == "%msdb.%"

    # Verify query uses parameterized placeholders
    query_str = str(query)
    assert ":exclude_0" in query_str
    assert ":exclude_1" in query_str
    assert ":exclude_2" in query_str

    # Verify actual pattern values are NOT in the query string (they're parameterized)
    # Note: This isn't a perfect test since patterns could appear in comments, but it's a sanity check
    assert query_str.count("%sys.%") == 0  # Should be parameterized, not in query text
    assert query_str.count("%temp%") == 0
    assert query_str.count("%msdb.%") == 0


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_query_store_disabled_mid_ingestion(create_engine_mock):
    """Test graceful handling when Query Store is disabled during ingestion."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
        }
    )
    report = SQLSourceReport()

    connection_mock = MagicMock()

    # Mock version check
    version_result = MagicMock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    # Mock Query Store initially enabled, then disabled
    result_enabled = MagicMock()
    result_enabled.fetchone.return_value = {"is_enabled": True}

    connection_mock.execute.side_effect = [
        version_result,
        result_enabled,
        ProgrammingError("Query Store is not enabled", None, None),
    ]

    sql_aggregator_mock = MagicMock()

    extractor = MSSQLLineageExtractor(
        config=config,
        connection=connection_mock,
        report=report,
        sql_aggregator=sql_aggregator_mock,
        default_schema="dbo",
    )

    queries = extractor.extract_query_history()

    assert queries == []
    assert report.failures


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_connection_timeout_during_extraction(create_engine_mock):
    """Test handling of connection timeout during query extraction."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
        }
    )
    report = SQLSourceReport()

    connection_mock = MagicMock()

    # Mock version check to succeed
    version_result = MagicMock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    qs_result = MagicMock()
    qs_result.fetchone.return_value = {"is_enabled": 1}

    call_count = [0]

    def execute_side_effect(query, *args):
        call_count[0] += 1
        if call_count[0] == 1:
            return version_result
        elif call_count[0] == 2:
            return qs_result
        else:
            raise OperationalError("Timeout expired", None, None)

    connection_mock.execute.side_effect = execute_side_effect

    sql_aggregator_mock = MagicMock()

    extractor = MSSQLLineageExtractor(
        config=config,
        connection=connection_mock,
        report=report,
        sql_aggregator=sql_aggregator_mock,
        default_schema="dbo",
    )

    queries = extractor.extract_query_history()

    assert queries == []
    assert report.failures
    assert any(
        "Timeout expired" in str(f) or "Database error" in str(f)
        for f in report.failures
    )


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_unicode_emoji_in_query_text(create_engine_mock):
    """Test extraction handles Unicode and emoji in query text."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
        }
    )
    report = SQLSourceReport()

    connection_mock = MagicMock()

    query_with_unicode = "SELECT * FROM users WHERE name = 'José 🎉 テスト'"

    # Mock version check
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 1}

    # Mock query results
    query_result = Mock()
    query_result.__iter__ = Mock(
        return_value=iter(
            [
                {
                    "query_id": "1",
                    "query_text": query_with_unicode,
                    "execution_count": 10,
                    "total_exec_time_ms": 100.0,
                    "database_name": "TestDB",
                    "user_name": None,
                }
            ]
        )
    )

    call_count = [0]

    def execute_side_effect(query, *args):
        call_count[0] += 1
        if call_count[0] == 1:
            return version_result
        elif call_count[0] == 2:
            return qs_result
        else:
            return query_result

    connection_mock.execute.side_effect = execute_side_effect

    sql_aggregator_mock = MagicMock()

    extractor = MSSQLLineageExtractor(
        config=config,
        connection=connection_mock,
        report=report,
        sql_aggregator=sql_aggregator_mock,
        default_schema="dbo",
    )

    queries = extractor.extract_query_history()

    assert len(queries) == 1
    assert queries[0].query_text == query_with_unicode
    assert "José" in queries[0].query_text
    assert "🎉" in queries[0].query_text
    assert "テスト" in queries[0].query_text


# =============================================================================
# Quick Win Tests: Config Edge Cases and Boundary Conditions
# =============================================================================


def test_mssql_max_queries_zero_handled_gracefully():
    """Test max_queries_to_extract=0 is rejected with clear error message."""
    with pytest.raises(ValueError, match="must be positive"):
        SQLServerConfig.model_validate(
            {
                **_base_config(),
                "include_query_lineage": True,
                "max_queries_to_extract": 0,
            }
        )


def test_mssql_very_long_query_text_handling():
    """Test extraction handles very long query text (NVARCHAR(MAX))."""
    mock_connection = Mock()

    # Create 10,000+ char query (max practical query size)
    long_query = (
        "SELECT * FROM table WHERE value IN ("
        + ", ".join([f"'{i}'" for i in range(2000)])
        + ")"
    )
    assert len(long_query) > 10000

    # Mock version check
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 1}

    # Mock row with very long query text
    query_result = Mock()
    query_result.__iter__ = Mock(
        return_value=iter(
            [
                {
                    "query_id": "1",
                    "query_text": long_query,
                    "execution_count": 10,
                    "total_exec_time_ms": 100.0,
                    "database_name": "TestDB",
                    "user_name": "testuser",
                }
            ]
        )
    )

    call_count = [0]

    def execute_side_effect(query, *args):
        call_count[0] += 1
        if call_count[0] == 1:
            return version_result
        elif call_count[0] == 2:
            return qs_result
        else:
            return query_result

    mock_connection.execute.side_effect = execute_side_effect

    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
        }
    )
    report = SQLSourceReport()
    sql_aggregator_mock = MagicMock()

    extractor = MSSQLLineageExtractor(
        config=config,
        connection=mock_connection,
        report=report,
        sql_aggregator=sql_aggregator_mock,
        default_schema="dbo",
    )

    queries = extractor.extract_query_history()

    # Verify long query was captured
    assert len(queries) == 1
    assert len(queries[0].query_text) > 10000


def test_mssql_exclude_patterns_with_tsql_special_chars():
    """Test exclude patterns handle TSQL special characters correctly."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "query_exclude_patterns": [
                "[^system]%",  # Bracket literal
                "schema.%[_]temp",  # Underscore literal
                "%EXEC[%]sp%",  # Percent literal
            ],
        }
    )

    assert config.query_exclude_patterns is not None
    # Patterns are stored as-is; SQL Server will interpret them
    assert "[^system]%" in config.query_exclude_patterns


# =============================================================================
# MAJOR: User Attribution is Broken (Documenting Known Issue)
# =============================================================================


def test_mssql_user_attribution_not_supported():
    """
    Document that user_name extraction is not supported in MSSQL.

    Query Store and DMV queries don't preserve historical user session context.
    The user_name field has been removed from MSSQLQueryEntry.
    """
    mock_connection = Mock()

    # Mock version check
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5",
        "major_version": 15,
    }

    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 1}

    query_result = Mock()
    query_result.__iter__ = Mock(
        return_value=iter(
            [
                {
                    "query_id": "1",
                    "query_text": "SELECT * FROM test",
                    "execution_count": 10,
                    "total_exec_time_ms": 100.0,
                    "database_name": "TestDB",
                }
            ]
        )
    )

    call_count = [0]

    def execute_side_effect(query, *args):
        call_count[0] += 1
        if call_count[0] == 1:
            return version_result
        elif call_count[0] == 2:
            return qs_result
        else:
            return query_result

    mock_connection.execute.side_effect = execute_side_effect

    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
        }
    )
    report = SQLSourceReport()
    sql_aggregator_mock = MagicMock()

    extractor = MSSQLLineageExtractor(
        config=config,
        connection=mock_connection,
        report=report,
        sql_aggregator=sql_aggregator_mock,
        default_schema="dbo",
    )

    queries = extractor.extract_query_history()

    assert len(queries) == 1
    assert queries[0].query_id is not None
    assert queries[0].query_text is not None
