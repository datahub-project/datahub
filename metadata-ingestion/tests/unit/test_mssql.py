import warnings
from typing import Optional
from unittest.mock import ANY, MagicMock, patch

import pytest
from sqlalchemy.exc import ProgrammingError

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline_config import PipelineConfig
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


@pytest.fixture
def mock_pipeline_context():
    """Factory fixture for creating mock PipelineContext with configurable source type."""

    def _create_context(source_type: Optional[str] = None) -> MagicMock:
        mock_ctx = MagicMock(spec=PipelineContext)

        if source_type is None:
            mock_ctx.pipeline_config = None
        else:
            mock_source_config = MagicMock()
            mock_source_config.type = source_type
            mock_pipeline_config = MagicMock(spec=PipelineConfig)
            mock_pipeline_config.source = mock_source_config
            mock_ctx.pipeline_config = mock_pipeline_config

        return mock_ctx

    return _create_context


def test_is_discovered_table(mssql_source):
    # Test tables matching temporary table patterns - not discovered
    assert mssql_source.is_discovered_table("test_db.dbo.temp_table") is False

    # Test tables starting with # - temp tables, not discovered
    assert mssql_source.is_discovered_table("test_db.dbo.#some_table") is False

    # Test tables that are not in discovered_datasets
    assert mssql_source.is_discovered_table("test_db.dbo.unknown_table") is False

    # Test regular tables that should be discovered
    assert mssql_source.is_discovered_table("test_db.dbo.regular_table") is True

    # Test 1-part names - treated as undiscovered since they can't be verified
    assert mssql_source.is_discovered_table("invalid_table_name") is False


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
    # Mock server name query result (RDS) - use realistic RDS endpoint pattern
    mock_result = MagicMock()
    mock_result.fetchone.return_value = {
        "server_name": "mydb.abc123.us-east-1.rds.amazonaws.com"
    }
    mock_conn.execute.return_value = mock_result

    result = mssql_source._detect_rds_environment(mock_conn)

    assert result is True
    # Note: execute() now wraps SQL with text(), but the test just verifies it's called


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
        # High confidence - official RDS endpoints
        ("mydb.xyz123.us-east-1.rds.amazonaws.com", True),
        ("mydb.abc123.rds.amazonaws.com", True),
        ("mydb.xyz123.rds.amazonaws.com.cn", True),
        ("mycompany-rds-prod.company.com", True),
        ("server.ec2-internal.amazonaws.com", True),
        ("amazon-rds-server", True),
        ("amzn-sqlserver-001", True),
        ("aws-rds-db", True),
        ("SQLSERVER01", False),
        ("sql.corporate.com", False),
        ("database.local", False),
        ("amazing_server", False),  # Should NOT match (avoid false positive)
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

    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=True),
        patch.object(
            mssql_source, "_get_jobs_via_stored_procedures", return_value=mock_jobs
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.info.assert_called_with(
        "Retrieved jobs using %s (%s)",
        "_get_jobs_via_stored_procedures",
        "managed environment",
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_success(mock_logger, mssql_source):
    """Test successful job retrieval in on-premises environment using direct query"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=False),
        patch.object(
            mssql_source, "_get_jobs_via_direct_query", return_value=mock_jobs
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    mock_logger.info.assert_called_with(
        "Retrieved jobs using %s (%s)",
        "_get_jobs_via_direct_query",
        "on-premises environment",
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_managed_fallback_success(mock_logger, mssql_source):
    """Test managed environment with stored procedure failure but direct query success"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=True),
        patch.object(
            mssql_source,
            "_get_jobs_via_stored_procedures",
            side_effect=ProgrammingError("SP failed", None, None),  # Database exception
        ),
        patch.object(
            mssql_source, "_get_jobs_via_direct_query", return_value=mock_jobs
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    # Updated expectations for new error handling logic
    assert mock_logger.info.called


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_fallback_success(mock_logger, mssql_source):
    """Test on-premises environment with direct query failure but stored procedure success"""
    mock_conn = MagicMock()
    mock_jobs = {"TestJob": {1: {"job_name": "TestJob", "step_name": "Step1"}}}

    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=False),
        patch.object(
            mssql_source,
            "_get_jobs_via_direct_query",
            side_effect=ProgrammingError("Direct query failed", None, None),
        ),
        patch.object(
            mssql_source, "_get_jobs_via_stored_procedures", return_value=mock_jobs
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == mock_jobs
    # The new implementation logs warnings for failures and info for success
    assert mock_logger.warning.called
    mock_logger.info.assert_called_with(
        "Retrieved jobs using %s (%s)",
        "_get_jobs_via_stored_procedures",
        "on-premises environment",
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_managed_both_methods_fail(mock_logger, mssql_source):
    """Test managed environment where both methods fail"""
    mock_conn = MagicMock()

    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=True),
        patch.object(
            mssql_source,
            "_get_jobs_via_stored_procedures",
            side_effect=ProgrammingError("SP failed", None, None),
        ),
        patch.object(
            mssql_source,
            "_get_jobs_via_direct_query",
            side_effect=ProgrammingError("Direct failed", None, None),
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == {}
    # Updated expectation for new four-tiered error handling
    mssql_source.report.failure.assert_called_once_with(
        message="Failed to retrieve jobs in managed environment (both stored procedures and direct query). "
        "This is expected on AWS RDS and some managed SQL instances that restrict msdb access. "
        "Jobs extraction will be skipped.",
        title="SQL Server Jobs Extraction",
        context="managed_environment_msdb_restricted",
        exc=ANY,
    )


@patch("datahub.ingestion.source.sql.mssql.source.logger")
def test_get_jobs_on_premises_both_methods_fail(mock_logger, mssql_source):
    """Test on-premises environment where both methods fail"""
    mock_conn = MagicMock()

    with (
        patch.object(mssql_source, "_detect_rds_environment", return_value=False),
        patch.object(
            mssql_source,
            "_get_jobs_via_direct_query",
            side_effect=ProgrammingError("Direct failed", None, None),
        ),
        patch.object(
            mssql_source,
            "_get_jobs_via_stored_procedures",
            side_effect=ProgrammingError("SP failed", None, None),
        ),
    ):
        result = mssql_source._get_jobs(mock_conn, "test_db")

    assert result == {}
    mssql_source.report.failure.assert_called_once_with(
        message="Failed to retrieve jobs in on-premises environment (both direct query and stored procedures). "
        "Verify the DataHub user has SELECT permissions on msdb.dbo.sysjobs and msdb.dbo.sysjobsteps, "
        "or EXECUTE permissions on sp_help_job and sp_help_jobstep.",
        title="SQL Server Jobs Extraction",
        context="on_prem_msdb_permission_denied",
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


@pytest.mark.parametrize(
    "source_type,expected_is_odbc",
    [
        # mssql-odbc enables ODBC mode
        ("mssql-odbc", True),
        # mssql does not enable ODBC mode
        ("mssql", False),
        # No pipeline_config defaults to non-ODBC
        (None, False),
    ],
)
def test_odbc_mode_from_source_type(
    mock_pipeline_context, source_type, expected_is_odbc
):
    """Test ODBC mode is determined by source type (using Pydantic validation context)."""
    mock_ctx = mock_pipeline_context(source_type)

    config_dict = {
        "host_port": "localhost:1433",
        "username": "test",
        "password": "test",
        "database": "test_db",
        "include_descriptions": False,
    }

    # Add uri_args when ODBC is expected (required by validator)
    if expected_is_odbc:
        config_dict["uri_args"] = {"driver": "ODBC Driver 17 for SQL Server"}

    with patch("datahub.ingestion.source.sql.sql_common.SQLAlchemySource.__init__"):
        source = SQLServerSource.create(config_dict, mock_ctx)

    # is_odbc is stored on the source instance (not config)
    assert source._is_odbc is expected_is_odbc


def test_use_odbc_removed_field_warning(mock_pipeline_context):
    """Test that using deprecated use_odbc field emits a warning via pydantic_removed_field."""
    mock_ctx = mock_pipeline_context("mssql-odbc")
    config_dict = {
        "host_port": "localhost:1433",
        "username": "test",
        "password": "test",
        "database": "test_db",
        "use_odbc": True,  # Deprecated field - should trigger warning
        "uri_args": {"driver": "ODBC Driver 17 for SQL Server"},
        "include_descriptions": False,
    }

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        with patch("datahub.ingestion.source.sql.sql_common.SQLAlchemySource.__init__"):
            source = SQLServerSource.create(config_dict, mock_ctx)

        config_warnings = [
            warning
            for warning in w
            if issubclass(warning.category, ConfigurationWarning)
        ]
        assert len(config_warnings) == 1
        assert "use_odbc" in str(config_warnings[0].message)
        assert "removed" in str(config_warnings[0].message)

    # is_odbc is determined by source type, stored on instance
    assert source._is_odbc is True
