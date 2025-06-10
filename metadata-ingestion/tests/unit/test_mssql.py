from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource


@pytest.fixture
def mssql_source():
    config = SQLServerConfig(
        host_port="localhost:1433",
        username="test",
        password="test",
        database="test_db",
        temporary_tables_pattern=["^temp_"],
        include_descriptions=False,  # Disable description loading to avoid DB connections
    )

    # Mock the parent class's __init__ to avoid DB connections
    with patch("datahub.ingestion.source.sql.sql_common.SQLAlchemySource.__init__"):
        source = SQLServerSource(config, MagicMock())
        source.discovered_datasets = {"test_db.dbo.regular_table"}
        source.report = MagicMock()
        return source


def test_is_temp_table(mssql_source):
    # Test tables matching temporary table patterns
    assert mssql_source.is_temp_table("test_db.dbo.temp_table") is True

    # Test tables starting with # (handled by startswith check in is_temp_table)
    assert mssql_source.is_temp_table("test_db.dbo.#some_table") is True

    # Test tables that are not in discovered_datasets
    assert mssql_source.is_temp_table("test_db.dbo.unknown_table") is True

    # Test regular tables that should return False
    assert mssql_source.is_temp_table("test_db.dbo.regular_table") is False

    # Test invalid table name format
    assert mssql_source.is_temp_table("invalid_table_name") is False


def test_detect_rds_environment_on_premises(mssql_source):
    """Test environment detection for on-premises SQL Server"""
    mock_conn = MagicMock()
    # Mock successful query execution (on-premises)
    mock_conn.execute.return_value = True

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is False
    mock_conn.execute.assert_called_once_with("SELECT TOP 1 * FROM msdb.dbo.sysjobs")


def test_detect_rds_environment_rds(mssql_source):
    """Test environment detection for RDS/managed SQL Server"""
    mock_conn = MagicMock()
    # Mock failed query execution (RDS)
    mock_conn.execute.side_effect = Exception("Access denied")

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is True
    mock_conn.execute.assert_called_once_with("SELECT TOP 1 * FROM msdb.dbo.sysjobs")


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_rds_environment_success(mock_logger, mssql_source):
    """Test successful job retrieval in RDS environment using stored procedures"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock RDS environment detection
    with patch.object(
        mssql_source, "_detect_rds_environment", return_value=True
    ), patch.object(
        mssql_source, "_get_jobs_via_stored_procedures", return_value=mock_jobs
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.info.assert_called_with(
        "Successfully retrieved jobs using stored procedures (RDS/managed environment)"
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_success(mock_logger, mssql_source):
    """Test successful job retrieval in on-premises environment using direct query"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock on-premises environment detection
    with patch.object(
        mssql_source, "_detect_rds_environment", return_value=False
    ), patch.object(mssql_source, "_get_jobs_via_direct_query", return_value=mock_jobs):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.info.assert_called_with(
        "Successfully retrieved jobs using direct query (on-premises environment)"
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_rds_fallback_success(mock_logger, mssql_source):
    """Test RDS environment with stored procedure failure but direct query success"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock RDS environment detection
    with patch.object(
        mssql_source, "_detect_rds_environment", return_value=True
    ), patch.object(
        mssql_source,
        "_get_jobs_via_stored_procedures",
        side_effect=Exception("SP failed"),
    ), patch.object(mssql_source, "_get_jobs_via_direct_query", return_value=mock_jobs):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.warning.assert_called_with(
        "Failed to retrieve jobs via stored procedures in RDS environment: SP failed"
    )
    mock_logger.info.assert_called_with(
        "Successfully retrieved jobs using direct query fallback in RDS environment"
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_fallback_success(mock_logger, mssql_source):
    """Test on-premises environment with direct query failure but stored procedure success"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock on-premises environment detection
    with patch.object(
        mssql_source, "_detect_rds_environment", return_value=False
    ), patch.object(
        mssql_source,
        "_get_jobs_via_direct_query",
        side_effect=Exception("Direct query failed"),
    ), patch.object(
        mssql_source, "_get_jobs_via_stored_procedures", return_value=mock_jobs
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.warning.assert_called_with(
        "Failed to retrieve jobs via direct query in on-premises environment: Direct query failed"
    )
    mock_logger.info.assert_called_with(
        "Successfully retrieved jobs using stored procedures fallback in on-premises environment"
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_rds_both_methods_fail(mock_logger, mssql_source):
    """Test RDS environment where both methods fail"""
    mock_conn = MagicMock()

    # Mock RDS environment detection
    with patch.object(
        mssql_source, "_detect_rds_environment", return_value=True
    ), patch.object(
        mssql_source,
        "_get_jobs_via_stored_procedures",
        side_effect=Exception("SP failed"),
    ), patch.object(
        mssql_source,
        "_get_jobs_via_direct_query",
        side_effect=Exception("Direct failed"),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == {}
    mock_logger.error.assert_called_with("Both methods failed in RDS environment")
    mssql_source.report.report_failure.assert_called_once_with(
        "jobs",
        "Failed to retrieve jobs in RDS environment. "
        "Stored procedure error: SP failed. Direct query error: Direct failed",
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_both_methods_fail(mock_logger, mssql_source):
    """Test on-premises environment where both methods fail"""
    mock_conn = MagicMock()

    # Mock on-premises environment detection
    with patch.object(
        mssql_source, "_detect_rds_environment", return_value=False
    ), patch.object(
        mssql_source,
        "_get_jobs_via_direct_query",
        side_effect=Exception("Direct failed"),
    ), patch.object(
        mssql_source,
        "_get_jobs_via_stored_procedures",
        side_effect=Exception("SP failed"),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == {}
    mock_logger.error.assert_called_with(
        "Both methods failed in on-premises environment"
    )
    mssql_source.report.report_failure.assert_called_once_with(
        "jobs",
        "Failed to retrieve jobs in on-premises environment. "
        "Direct query error: Direct failed. Stored procedure error: SP failed",
    )
