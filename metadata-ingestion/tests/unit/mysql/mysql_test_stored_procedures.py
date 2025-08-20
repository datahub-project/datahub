from unittest.mock import MagicMock, patch

from pydantic import SecretStr
from sqlalchemy.engine import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure


def test_stored_procedure_parsing():
    """Test parsing of a stored procedure using BaseProcedure"""
    procedure = BaseProcedure(
        name="test_proc",
        language="sql",
        argument_signature=None,
        return_type=None,
        procedure_definition="""
        CREATE PROCEDURE test_proc()
        BEGIN
            INSERT INTO target_table
            SELECT * FROM source_table;
        END
        """,
        created=None,
        last_altered=None,
        comment=None,
        extra_properties={
            "sql_data_access": "MODIFIES",
            "security_type": "DEFINER",
            "definer": "root@localhost",
        },
    )

    assert procedure.name == "test_proc"
    assert procedure.language == "sql"
    assert procedure.procedure_definition is not None
    assert "CREATE PROCEDURE test_proc()" in procedure.procedure_definition
    assert procedure.extra_properties is not None
    assert procedure.extra_properties["sql_data_access"] == "MODIFIES"


def test_mysql_source_has_stored_procedure_support():
    """Test that MySQL source has stored procedure support"""
    config = MySQLConfig(
        host_port="localhost:3306",
        include_stored_procedures=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"]),
    )
    source = MySQLSource(ctx=PipelineContext(run_id="test"), config=config)

    # Test configuration is properly set
    assert config.include_stored_procedures
    assert config.procedure_pattern.allowed("test_db.my_proc")
    assert not config.procedure_pattern.allowed("other_db.proc")

    # Test source has the required methods
    assert hasattr(source, "loop_stored_procedures")
    assert hasattr(source, "get_schema_level_workunits")


def test_stored_procedure_config():
    """Test stored procedure configuration options"""
    config = MySQLConfig(
        host_port="localhost:3306",
        username="user",
        password=SecretStr("pass"),
        database="test_db",
        include_stored_procedures=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"], deny=[".*_temp"]),
    )

    assert config.include_stored_procedures
    assert config.procedure_pattern.allowed("test_db.my_proc")
    assert not config.procedure_pattern.allowed("test_db.my_proc_temp")
    assert not config.procedure_pattern.allowed("other_db.proc")


def test_temp_table_identification():
    """Test MySQL temporary table pattern matching logic using the actual MySQLSource method"""

    # Create a MySQL source instance to test the actual method
    config = MySQLConfig(
        host_port="localhost:3306", username="test", password="test", database="test"
    )
    ctx = PipelineContext(run_id="test")
    mysql_source = MySQLSource(config, ctx)

    # Test cases that SHOULD match temporary table patterns
    temp_cases = [
        "#temp_table",  # Starts with #
        "#TMP_123",  # Starts with # (case insensitive)
        "tmp_customers",  # Starts with tmp_
        "temp_data",  # Starts with temp_
        "TMP_STAGING",  # Starts with tmp_ (uppercase)
        "TEMP_WORK",  # Starts with temp_ (uppercase)
        "my_table_tmp",  # Ends with _tmp
        "my_table_temp",  # Ends with _temp
        "DATA_TMP",  # Ends with _tmp (uppercase)
        "STAGING_TEMP",  # Ends with _temp (uppercase)
        "stage_tmp_final",  # Contains _tmp_
        "stage_temp_final",  # Contains _temp_
        "ETL_TMP_PROCESS",  # Contains _tmp_ (uppercase)
        "LOAD_TEMP_DATA",  # Contains _temp_ (uppercase)
    ]

    # Test cases that should NOT match
    non_temp_cases = [
        "customers",  # Regular table
        "template_table",  # Contains "temp" but not matching pattern
        "temperature",  # Contains "temp" but not matching pattern
        "temporary",  # Contains "temp" but not matching pattern
        "attempt",  # Contains "temp" but not matching pattern
        "contempt",  # Contains "temp" but not matching pattern
        "tmpfile",  # Starts with "tmp" but no underscore
        "tempfile",  # Starts with "temp" but no underscore
        "my_attempt_table",  # Contains "temp" but not in pattern
        "contemporary_data",  # Contains "temp" but not in pattern
    ]

    # Test qualified table names (database.schema.table format)
    qualified_temp_cases = [
        "mydb.schema.tmp_table",  # Should extract "tmp_table"
        "prod.public.temp_staging",  # Should extract "temp_staging"
        "test.dbo.final_tmp",  # Should extract "final_tmp"
        "warehouse.etl.stage_temp_work",  # Should extract "stage_temp_work"
    ]

    qualified_non_temp_cases = [
        "mydb.schema.customers",  # Should extract "customers"
        "prod.public.template_data",  # Should extract "template_data"
    ]

    # Run all test cases using the actual MySQLSource.is_temp_table method
    for table_name in temp_cases:
        assert mysql_source.is_temp_table(table_name), (
            f"Expected '{table_name}' to be identified as temp table"
        )

    for table_name in non_temp_cases:
        assert not mysql_source.is_temp_table(table_name), (
            f"Expected '{table_name}' to NOT be identified as temp table"
        )

    for table_name in qualified_temp_cases:
        assert mysql_source.is_temp_table(table_name), (
            f"Expected qualified name '{table_name}' to be identified as temp table"
        )

    for table_name in qualified_non_temp_cases:
        assert not mysql_source.is_temp_table(table_name), (
            f"Expected qualified name '{table_name}' to NOT be identified as temp table"
        )


def test_mysql_procedure_pattern_filtering():
    """Test procedure pattern filtering in MySQL source"""
    mock_inspector = MagicMock(spec=Inspector)
    mock_engine = MagicMock()
    mock_inspector.engine = mock_engine
    mock_inspector.engine.url.database = "test_db"

    config = MySQLConfig(
        host_port="localhost:3306",
        include_stored_procedures=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"], deny=[".*_temp"]),
    )

    source = MySQLSource(ctx=PipelineContext(run_id="test"), config=config)

    with patch.object(source, "get_procedures_for_schema") as mock_get_procs:
        mock_get_procs.return_value = [
            BaseProcedure(
                name="test_proc_temp",
                language="sql",
                argument_signature=None,
                return_type=None,
                procedure_definition="CREATE PROCEDURE test_proc_temp() BEGIN SELECT 1; END",
                created=None,
                last_altered=None,
                comment=None,
                extra_properties={},
            )
        ]

        procedures = source.fetch_procedures_for_schema(
            inspector=mock_inspector, schema="test_db", db_name="test_db"
        )

        # Should be filtered out by pattern
        assert len(procedures) == 0


def test_mysql_loop_stored_procedures():
    """Test the loop_stored_procedures method"""
    mock_inspector = MagicMock(spec=Inspector)
    mock_engine = MagicMock()
    mock_inspector.engine = mock_engine

    # Mock get_db_name
    mock_inspector.engine.url.database = "test_db"

    # Configure the source
    config = MySQLConfig(
        host_port="localhost:3306",
        include_stored_procedures=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"]),
    )

    source = MySQLSource(ctx=PipelineContext(run_id="test"), config=config)

    # Mock fetch_procedures_for_schema to return test data
    with patch.object(source, "fetch_procedures_for_schema") as mock_fetch_procs:
        mock_fetch_procs.return_value = [
            BaseProcedure(
                name="test_proc",
                language="sql",
                argument_signature=None,
                return_type=None,
                procedure_definition="CREATE PROCEDURE test_proc() BEGIN SELECT 1; END",
                created=None,
                last_altered=None,
                comment="Test procedure",
                extra_properties={},
            )
        ]

        # Convert generator to list to execute it
        workunits = list(
            source.loop_stored_procedures(
                inspector=mock_inspector, schema="test_db", config=config
            )
        )

        # Verify work units were generated
        assert len(workunits) > 0
        # Verify procedures were fetched
        mock_fetch_procs.assert_called_once()
