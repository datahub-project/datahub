from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mssql.lineage import (
    MSSQLLineageExtractor,
    MSSQLQueryEntry,
)
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource
from datahub.ingestion.source.sql.sql_common import SQLSourceReport


def _base_config():
    return {
        "username": "sa",
        "password": "test",
        "host_port": "localhost:1433",
        "database": "TestDB",
    }


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_config_with_query_lineage(create_engine_mock):
    """Test MS SQL config with query lineage enabled."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "max_queries_to_extract": 500,
        }
    )
    assert config.include_query_lineage is True
    assert config.max_queries_to_extract == 500


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

        with pytest.raises(RuntimeError) as exc_info:
            SQLServerSource(config, PipelineContext(run_id="test"))

        error_message = str(exc_info.value)
        assert "explicitly enabled" in error_message.lower()
        assert "include_query_lineage: true" in error_message


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_max_queries_validation(create_engine_mock):
    """Test max_queries_to_extract validation."""
    with pytest.raises(ValueError, match="must be positive"):
        SQLServerConfig.model_validate({**_base_config(), "max_queries_to_extract": 0})

    with pytest.raises(ValueError, match="<= 10000"):
        SQLServerConfig.model_validate(
            {**_base_config(), "max_queries_to_extract": 20000}
        )

    config = SQLServerConfig.model_validate(
        {**_base_config(), "max_queries_to_extract": 5000}
    )
    assert config.max_queries_to_extract == 5000


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_min_query_calls_validation(create_engine_mock):
    """Test min_query_calls validation."""
    with pytest.raises(ValueError, match="non-negative"):
        SQLServerConfig.model_validate({**_base_config(), "min_query_calls": -1})

    config = SQLServerConfig.model_validate({**_base_config(), "min_query_calls": 5})
    assert config.min_query_calls == 5


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_query_exclude_patterns_empty_string(create_engine_mock):
    """Test that empty strings in query_exclude_patterns are rejected."""
    with pytest.raises(ValueError, match="empty or contains only whitespace"):
        SQLServerConfig.model_validate(
            {**_base_config(), "query_exclude_patterns": ["%sys%", ""]}
        )

    with pytest.raises(ValueError, match="empty or contains only whitespace"):
        SQLServerConfig.model_validate(
            {**_base_config(), "query_exclude_patterns": ["   "]}
        )


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_query_exclude_patterns_unmatched_brackets(create_engine_mock):
    """Test that unmatched brackets in query_exclude_patterns are rejected."""
    with pytest.raises(ValueError, match="unmatched opening bracket"):
        SQLServerConfig.model_validate(
            {**_base_config(), "query_exclude_patterns": ["%[abc%"]}
        )

    with pytest.raises(ValueError, match="unmatched closing bracket"):
        SQLServerConfig.model_validate(
            {**_base_config(), "query_exclude_patterns": ["%abc]%"]}
        )


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_query_exclude_patterns_valid(create_engine_mock):
    """Test that valid query_exclude_patterns are accepted."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "query_exclude_patterns": [
                "%sys.%",
                "%temp_%",
                "%[0-9]%",  # Valid bracket pattern
                "%\\[escaped\\]%",  # Escaped brackets
            ],
        }
    )
    assert len(config.query_exclude_patterns) == 4


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_source_has_close_method(create_engine_mock):
    """Test that MSSQLSource has close() method."""
    config = SQLServerConfig.model_validate({**_base_config()})
    source = SQLServerSource(config, PipelineContext(run_id="test"))

    assert hasattr(source, "close")
    assert callable(source.close)

    # Should not crash when calling close
    source.close()


# Tests for MSSQLLineageExtractor


def test_mssql_lineage_extractor_version_check():
    """Test SQL Server version detection."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock version query result - SQL Server 2019
    conn_mock.execute.return_value.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5"
    }

    is_supported, message = extractor._check_version()

    assert is_supported is True
    assert "2019" in message
    conn_mock.execute.assert_called_once()


def test_mssql_lineage_extractor_version_check_old_version():
    """Test version detection rejects SQL Server 2014."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock version query result - SQL Server 2014
    conn_mock.execute.return_value.fetchone.return_value = {
        "version": "Microsoft SQL Server 2014 (SP2) - 12.0.5000.0"
    }

    is_supported, message = extractor._check_version()

    assert is_supported is False
    assert "2016" in message


def test_mssql_lineage_extractor_query_store_enabled():
    """Test Query Store availability check when enabled."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock Query Store enabled
    conn_mock.execute.return_value.fetchone.return_value = {"is_enabled": 1}

    is_enabled, message = extractor._check_query_store_available()

    assert is_enabled is True
    assert "enabled" in message.lower()


def test_mssql_lineage_extractor_query_store_disabled():
    """Test Query Store availability check when disabled."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock Query Store disabled
    conn_mock.execute.return_value.fetchone.return_value = {"is_enabled": 0}

    is_enabled, message = extractor._check_query_store_available()

    assert is_enabled is False
    assert "not enabled" in message.lower()


def test_mssql_lineage_extractor_dmv_permissions_granted():
    """Test DMV permissions check when granted."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock successful DMV query (no exception)
    conn_mock.execute.return_value.fetchone.return_value = {"plan_count": 10}

    has_permission, message, method = extractor._check_dmv_permissions()

    assert has_permission is True
    assert method == "dmv"
    assert "available" in message.lower()


def test_mssql_lineage_extractor_dmv_permissions_denied():
    """Test DMV permissions check when denied."""
    from sqlalchemy.exc import DatabaseError

    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock permission denied error
    conn_mock.execute.side_effect = DatabaseError("statement", "params", "orig")

    has_permission, message, method = extractor._check_dmv_permissions()

    assert has_permission is False
    assert method == "none"
    assert "permission" in message.lower() or "failed" in message.lower()


def test_mssql_lineage_extractor_check_prerequisites_query_store():
    """Test prerequisites check succeeds with Query Store."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock successful version check
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5"
    }

    # Mock Query Store enabled
    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 1}

    conn_mock.execute.side_effect = [version_result, qs_result]

    can_extract, method = extractor.check_prerequisites()

    assert can_extract is True
    assert method == "query_store"


def test_mssql_lineage_extractor_check_prerequisites_dmv_fallback():
    """Test prerequisites check falls back to DMV when Query Store disabled."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock successful version check
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5"
    }

    # Mock Query Store disabled
    qs_result = Mock()
    qs_result.fetchone.return_value = {"is_enabled": 0}

    # Mock DMV permissions granted
    dmv_result = Mock()
    dmv_result.fetchone.return_value = {"plan_count": 10}

    conn_mock.execute.side_effect = [version_result, qs_result, dmv_result]

    can_extract, method = extractor.check_prerequisites()

    assert can_extract is True
    assert method == "dmv"


def test_mssql_lineage_extractor_check_prerequisites_fails():
    """Test prerequisites check fails when version too old."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock old version
    version_result = Mock()
    version_result.fetchone.return_value = {
        "version": "Microsoft SQL Server 2014 (SP2) - 12.0.5000.0"
    }

    conn_mock.execute.return_value = version_result

    can_extract, method = extractor.check_prerequisites()

    assert can_extract is False
    assert method == "none"
    assert len(report.failures) > 0


def test_mssql_lineage_extractor_extract_queries_from_query_store():
    """Test query extraction from Query Store."""
    config = SQLServerConfig.model_validate(
        {**_base_config(), "max_queries_to_extract": 10, "min_query_calls": 2}
    )
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

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

    conn_mock.execute.return_value.fetchall.return_value = mock_results

    # Mock prerequisites check
    with patch.object(
        extractor, "check_prerequisites", return_value=(True, "query_store")
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

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock Query Store results - one below threshold
    mock_results = [
        {
            "query_id": "1",
            "query_text": "SELECT * FROM users",
            "execution_count": 10,  # Above threshold
            "total_exec_time_ms": 100.5,
            "user_name": "test_user",
            "database_name": "TestDB",
        },
        {
            "query_id": "2",
            "query_text": "SELECT * FROM orders",
            "execution_count": 3,  # Below threshold
            "total_exec_time_ms": 50.2,
            "user_name": "admin",
            "database_name": "TestDB",
        },
    ]

    conn_mock.execute.return_value.fetchall.return_value = mock_results

    with patch.object(
        extractor, "check_prerequisites", return_value=(True, "query_store")
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

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock Query Store results
    mock_results = [
        {
            "query_id": "1",
            "query_text": "SELECT * FROM users",
            "execution_count": 5,
            "total_exec_time_ms": 100.5,
            "user_name": "test_user",
            "database_name": "TestDB",
        },
        {
            "query_id": "2",
            "query_text": "SELECT * FROM sys.tables",  # Should be excluded
            "execution_count": 10,
            "total_exec_time_ms": 50.2,
            "user_name": "admin",
            "database_name": "TestDB",
        },
        {
            "query_id": "3",
            "query_text": "SELECT * FROM msdb.dbo.jobs",  # Should be excluded
            "execution_count": 8,
            "total_exec_time_ms": 75.0,
            "user_name": "admin",
            "database_name": "TestDB",
        },
    ]

    conn_mock.execute.return_value.fetchall.return_value = mock_results

    with patch.object(
        extractor, "check_prerequisites", return_value=(True, "query_store")
    ):
        queries = extractor.extract_query_history()

    # Only the first query should be included
    assert len(queries) == 1
    assert queries[0].query_id == "1"
    assert "users" in queries[0].query_text


def test_mssql_lineage_extractor_handles_extraction_failure():
    """Test query extraction handles database errors gracefully."""
    from sqlalchemy.exc import DatabaseError

    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Mock database error during extraction
    conn_mock.execute.side_effect = DatabaseError("statement", "params", "orig")

    with patch.object(
        extractor, "check_prerequisites", return_value=(True, "query_store")
    ):
        queries = extractor.extract_query_history()

    assert len(queries) == 0
    assert len(report.failures) > 0
    assert any("failed" in f.message.lower() for f in report.failures)


def test_mssql_lineage_extractor_populate_lineage():
    """Test populate_lineage_from_queries adds queries to aggregator."""
    config = SQLServerConfig.model_validate(_base_config())
    report = SQLSourceReport()
    conn_mock = Mock()
    aggregator_mock = Mock()

    extractor = MSSQLLineageExtractor(config, report, conn_mock)

    # Create test queries
    queries = [
        MSSQLQueryEntry(
            query_id="1",
            query_text="SELECT * FROM users",
            execution_count=5,
            total_exec_time_ms=100.0,
            user_name="test_user",
            database_name="TestDB",
        )
    ]

    extractor.populate_lineage_from_queries(queries, aggregator_mock, "mssql")

    # Verify aggregator was called
    aggregator_mock.add.assert_called_once()
    call_args = aggregator_mock.add.call_args[0][0]
    assert call_args.query == "SELECT * FROM users"
    assert call_args.user.urn() == "urn:li:corpuser:test_user"


def test_mssql_query_entry_avg_exec_time():
    """Test MSSQLQueryEntry calculates average execution time correctly."""
    entry = MSSQLQueryEntry(
        query_id="1",
        query_text="SELECT 1",
        execution_count=10,
        total_exec_time_ms=500.0,
        user_name="user",
        database_name="db",
    )

    assert entry.avg_exec_time_ms == 50.0

    # Test zero execution count
    entry_zero = MSSQLQueryEntry(
        query_id="2",
        query_text="SELECT 1",
        execution_count=0,
        total_exec_time_ms=0.0,
        user_name="user",
        database_name="db",
    )

    assert entry_zero.avg_exec_time_ms == 0.0
