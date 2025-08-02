import re
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
    """Test temporary table identification logic"""

    def is_temp_table(name: str) -> bool:
        # Use regex patterns to ensure exact matches
        patterns = [
            r"^temp_",  # Starts with temp_
            r"^tmp_",  # Starts with tmp_
            r"_temp$",  # Ends with _temp
            r"_tmp$",  # Ends with _tmp
            r"_temp_",  # Has _temp_ in middle
            r"_tmp_",  # Has _tmp_ in middle
        ]
        return any(re.search(pattern, name.lower()) for pattern in patterns)

    # Should match temporary table patterns
    assert is_temp_table("temp_customers")
    assert is_temp_table("TMP_DATA")
    assert is_temp_table("my_temp_table")
    assert is_temp_table("my_tmp_table")

    # Should not match non-temporary tables
    assert not is_temp_table("customers")
    assert not is_temp_table("template_table")
    assert not is_temp_table("temperature")
    assert not is_temp_table("temporary")


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
