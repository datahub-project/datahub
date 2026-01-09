from unittest.mock import ANY, MagicMock, patch

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
        source.ctx = MagicMock()
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

    # Test 1-part names - treated as aliases since they can't be verified
    # Common TSQL aliases like "dst", "src" are 1-part names
    assert mssql_source.is_temp_table("invalid_table_name") is True


def test_detect_rds_environment_on_premises(mssql_source):
    """Test environment detection for on-premises SQL Server"""
    mock_conn = MagicMock()
    # Mock server name query result (on-premises)
    mock_result = MagicMock()
    mock_result.fetchone.return_value = {"server_name": "SQLSERVER01"}
    mock_conn.execute.return_value = mock_result

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is False
    mock_conn.execute.assert_called_once_with("SELECT @@servername AS server_name")


def test_detect_rds_environment_rds(mssql_source):
    """Test environment detection for RDS/managed SQL Server"""
    mock_conn = MagicMock()
    # Mock server name query result (RDS)
    mock_result = MagicMock()
    mock_result.fetchone.return_value = {"server_name": "EC2AMAZ-FOUTLJN"}
    mock_conn.execute.return_value = mock_result

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is True
    mock_conn.execute.assert_called_once_with("SELECT @@servername AS server_name")


def test_detect_rds_environment_explicit_config_true(mssql_source):
    """Test environment detection with explicit is_aws_rds=True configuration"""
    mssql_source.config.is_aws_rds = True
    mock_conn = MagicMock()

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is True
    # Should not execute any queries when explicit config is provided
    mock_conn.execute.assert_not_called()


def test_detect_rds_environment_explicit_config_false(mssql_source):
    """Test environment detection with explicit is_aws_rds=False configuration"""
    mssql_source.config.is_aws_rds = False
    mock_conn = MagicMock()

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is False
    # Should not execute any queries when explicit config is provided
    mock_conn.execute.assert_not_called()


def test_detect_rds_environment_query_failure(mssql_source):
    """Test environment detection when server name query fails"""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Permission denied")

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is False
    mock_conn.execute.assert_called_once_with("SELECT @@servername AS server_name")


def test_detect_rds_environment_no_result(mssql_source):
    """Test environment detection when server name query returns no result"""
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_conn.execute.return_value = mock_result

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is False
    mock_conn.execute.assert_called_once_with("SELECT @@servername AS server_name")


@pytest.mark.parametrize(
    "server_name,expected_rds",
    [
        ("server.amazon.com", True),
        ("server.amzn.com", True),
        ("server.amaz.com", True),
        ("server.ec2.internal", True),
        ("mydb.xyz123.rds.amazonaws.com", True),
        ("SQLSERVER01", False),
        ("sql.corporate.com", False),
        ("database.local", False),
    ],
)
def test_detect_rds_environment_various_aws_indicators(
    mssql_source, server_name, expected_rds
):
    """Test environment detection with various AWS server name patterns"""
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = {"server_name": server_name}
    mock_conn.execute.return_value = mock_result

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is expected_rds


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_managed_environment_success(mock_logger, mssql_source):
    """Test successful job retrieval in managed environment using stored procedures"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock managed environment detection
    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=True),
        patch.object(
            mssql_source, "_get_jobs_via_stored_procedures", return_value=mock_jobs
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.info.assert_called_with(
        "Successfully retrieved jobs using stored procedures (managed environment)"
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_success(mock_logger, mssql_source):
    """Test successful job retrieval in on-premises environment using direct query"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock on-premises environment detection
    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=False),
        patch.object(
            mssql_source, "_get_jobs_via_direct_query", return_value=mock_jobs
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.info.assert_called_with(
        "Successfully retrieved jobs using direct query (on-premises environment)"
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_managed_fallback_success(mock_logger, mssql_source):
    """Test managed environment with stored procedure failure but direct query success"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock managed environment detection
    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=True),
        patch.object(
            mssql_source,
            "_get_jobs_via_stored_procedures",
            side_effect=Exception("SP failed"),
        ),
        patch.object(
            mssql_source, "_get_jobs_via_direct_query", return_value=mock_jobs
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.warning.assert_called_with(
        "Failed to retrieve jobs via stored procedures in managed environment: SP failed"
    )
    mock_logger.info.assert_called_with(
        "Successfully retrieved jobs using direct query fallback in managed environment"
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_fallback_success(mock_logger, mssql_source):
    """Test on-premises environment with direct query failure but stored procedure success"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    # Mock on-premises environment detection
    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=False),
        patch.object(
            mssql_source,
            "_get_jobs_via_direct_query",
            side_effect=Exception("Direct query failed"),
        ),
        patch.object(
            mssql_source, "_get_jobs_via_stored_procedures", return_value=mock_jobs
        ),
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
def test_get_jobs_managed_both_methods_fail(mock_logger, mssql_source):
    """Test managed environment where both methods fail"""
    mock_conn = MagicMock()

    # Mock managed environment detection
    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=True),
        patch.object(
            mssql_source,
            "_get_jobs_via_stored_procedures",
            side_effect=Exception("SP failed"),
        ),
        patch.object(
            mssql_source,
            "_get_jobs_via_direct_query",
            side_effect=Exception("Direct failed"),
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == {}
    mssql_source.report.failure.assert_called_once_with(
        message="Failed to retrieve jobs in managed environment",
        title="SQL Server Jobs Extraction",
        context="Both stored procedures and direct query methods failed",
        exc=ANY,
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_both_methods_fail(mock_logger, mssql_source):
    """Test on-premises environment where both methods fail"""
    mock_conn = MagicMock()

    # Mock on-premises environment detection
    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=False),
        patch.object(
            mssql_source,
            "_get_jobs_via_direct_query",
            side_effect=Exception("Direct failed"),
        ),
        patch.object(
            mssql_source,
            "_get_jobs_via_stored_procedures",
            side_effect=Exception("SP failed"),
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == {}
    mssql_source.report.failure.assert_called_once_with(
        message="Failed to retrieve jobs in on-premises environment",
        title="SQL Server Jobs Extraction",
        context="Both direct query and stored procedures methods failed",
        exc=ANY,
    )


def test_stored_procedure_vs_direct_query_compatibility(mssql_source):
    """Test that both methods return compatible data structures"""
    mock_conn = MagicMock()

    # Mock data that both methods should be able to process
    mock_job_data = {
        "job_id": "12345678-1234-1234-1234-123456789012",
        "name": "TestJob",
        "description": "Test job description",
        "date_created": "2023-01-01 10:00:00",
        "date_modified": "2023-01-02 11:00:00",
        "enabled": 1,
        "step_id": 1,
        "step_name": "Test Step",
        "subsystem": "TSQL",
        "command": "SELECT 1",
        "database_name": "test_db",
    }

    # Test stored procedure method
    with patch.object(mock_conn, "execute") as mock_execute:
        # Mock sp_help_job response
        mock_job_result = MagicMock()
        mock_job_mappings = MagicMock()
        mock_job_mappings.__iter__.return_value = [mock_job_data]
        mock_job_result.mappings.return_value = mock_job_mappings

        # Mock sp_help_jobstep response
        mock_step_result = MagicMock()
        mock_step_mappings = MagicMock()
        mock_step_mappings.__iter__.return_value = [mock_job_data]
        mock_step_result.mappings.return_value = mock_step_mappings

        mock_execute.side_effect = [mock_job_result, mock_step_result]

        sp_result = mssql_source._get_jobs_via_stored_procedures(mock_conn, "test_db")

    # Test direct query method
    with patch.object(mock_conn, "execute") as mock_execute:
        mock_query_result = MagicMock()
        mock_query_result.__iter__.return_value = [mock_job_data]
        mock_execute.return_value = mock_query_result

        direct_result = mssql_source._get_jobs_via_direct_query(mock_conn, "test_db")

    # Verify both methods return data with same structure
    assert len(sp_result) > 0, "Stored procedure method should return data"
    assert len(direct_result) > 0, "Direct query method should return data"

    # Get first job from each method
    sp_job = list(sp_result.values())[0]
    direct_job = list(direct_result.values())[0]

    # Get first step from each method
    sp_step = list(sp_job.values())[0]
    direct_step = list(direct_job.values())[0]

    # Verify both contain critical fields
    required_fields = [
        "job_id",
        "job_name",
        "description",
        "date_created",
        "date_modified",
        "step_id",
        "step_name",
        "subsystem",
        "command",
        "database_name",
    ]

    for field in required_fields:
        assert field in sp_step, f"Stored procedure result missing field: {field}"
        assert field in direct_step, f"Direct query result missing field: {field}"

    # Verify database_name is properly set
    assert sp_step["database_name"] == "test_db"
    assert direct_step["database_name"] == "test_db"
