from unittest.mock import MagicMock, patch

from sqlalchemy.engine import Connection, Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mariadb import MariaDBConfig, MariaDBSource
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure


def test_platform_correctly_set_mariadb():
    source = MariaDBSource(
        ctx=PipelineContext(run_id="mariadb-source-test"),
        config=MariaDBConfig(),
    )
    assert source.platform == "mariadb"


def test_mariadb_stored_procedure_parsing():
    """Test parsing of a MariaDB stored procedure using BaseProcedure"""
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


def test_mariadb_config():
    """Test MariaDB configuration options"""
    config = MariaDBConfig(
        host_port="localhost:3306",
        database="test_db",
        include_stored_procedures=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"], deny=[".*_temp"]),
    )

    assert config.include_stored_procedures
    assert config.procedure_pattern.allowed("test_db.my_proc")
    assert not config.procedure_pattern.allowed("test_db.my_proc_temp")
    assert not config.procedure_pattern.allowed("other_db.proc")
    assert config.host_port == "localhost:3306"


def test_get_stored_procedures():
    """Test fetching stored procedures from MariaDB"""
    mock_conn = MagicMock(spec=Connection)

    # Create mock result for ROUTINES query
    routines_result = MagicMock()
    routines_result.__iter__.return_value = [
        {
            "ROUTINE_SCHEMA": "test_db",
            "ROUTINE_NAME": "test_proc",
            "ROUTINE_DEFINITION": "CREATE PROCEDURE test_proc() BEGIN SELECT 1; END",
            "ROUTINE_COMMENT": "Test procedure",
            "CREATED": "2024-01-01",
            "LAST_ALTERED": "2024-01-02",
            "SQL_DATA_ACCESS": "MODIFIES",
            "SECURITY_TYPE": "DEFINER",
            "DEFINER": "root@localhost",
        }
    ].__iter__()

    # Create mock result for SHOW CREATE PROCEDURE
    show_create_result = MagicMock()
    show_create_result.fetchone.return_value = (
        "test_proc",
        "utf8mb4",
        "CREATE PROCEDURE test_proc() BEGIN SELECT 1; END",
    )

    def mock_execute(query, params=None):
        if "SHOW CREATE PROCEDURE" in str(query):
            return show_create_result
        return routines_result

    mock_conn.execute.side_effect = mock_execute

    source = MariaDBSource(ctx=PipelineContext(run_id="test"), config=MariaDBConfig())

    procedures = source._get_stored_procedures(
        conn=mock_conn, db_name="test_db", schema="test_db"
    )

    assert len(procedures) == 1
    assert isinstance(procedures[0], BaseProcedure)
    assert procedures[0].name == "test_proc"
    assert procedures[0].procedure_definition is not None
    assert "CREATE PROCEDURE" in procedures[0].procedure_definition
    assert procedures[0].comment == "Test procedure"
    assert procedures[0].extra_properties is not None
    assert procedures[0].extra_properties["sql_data_access"] == "MODIFIES"


def test_loop_stored_procedures():
    """Test the loop_stored_procedures method"""
    mock_inspector = MagicMock(spec=Inspector)
    mock_engine = MagicMock()
    mock_inspector.engine = mock_engine

    # Mock get_db_name
    mock_inspector.engine.url.database = "test_db"

    # Configure the source
    config = MariaDBConfig(
        host_port="localhost:3306",
        include_stored_procedures=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"]),
    )

    source = MariaDBSource(ctx=PipelineContext(run_id="test"), config=config)

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
                inspector=mock_inspector, schema="test_db", sql_config=config
            )
        )

        # Verify work units were generated
        assert len(workunits) > 0
        # Verify procedures were fetched
        mock_fetch_procs.assert_called_once()


def test_mariadb_procedure_pattern_filtering():
    """Test procedure pattern filtering in MariaDB source"""
    mock_inspector = MagicMock(spec=Inspector)
    mock_engine = MagicMock()
    mock_inspector.engine = mock_engine
    mock_inspector.engine.url.database = "test_db"

    config = MariaDBConfig(
        host_port="localhost:3306",
        include_stored_procedures=True,
        procedure_pattern=AllowDenyPattern(allow=["test_db.*"], deny=[".*_temp"]),
    )

    source = MariaDBSource(ctx=PipelineContext(run_id="test"), config=config)

    with patch.object(source, "_get_stored_procedures") as mock_get_procs:
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


def test_mariadb_error_handling():
    """Test error handling in MariaDB stored procedure fetching"""
    mock_conn = MagicMock(spec=Connection)

    # Create mock result for ROUTINES query
    routines_result = MagicMock()
    routines_result.__iter__.return_value = [
        {
            "ROUTINE_SCHEMA": "test_db",
            "ROUTINE_NAME": "test_proc",
            "ROUTINE_DEFINITION": "CREATE PROCEDURE test_proc() BEGIN SELECT 1; END",
            "ROUTINE_COMMENT": "Test procedure",
            "CREATED": "2024-01-01",
            "LAST_ALTERED": "2024-01-02",
            "SQL_DATA_ACCESS": "MODIFIES",
            "SECURITY_TYPE": "DEFINER",
            "DEFINER": "root@localhost",
        }
    ].__iter__()

    # Mock execution behavior
    def mock_execute(query, params=None):
        if "SHOW CREATE PROCEDURE" in str(query):
            raise Exception("Failed to get procedure")
        if "FROM information_schema.ROUTINES" in str(query):
            return routines_result
        return MagicMock()

    mock_conn.execute.side_effect = mock_execute

    source = MariaDBSource(ctx=PipelineContext(run_id="test"), config=MariaDBConfig())
    procedures = source._get_stored_procedures(
        conn=mock_conn, db_name="test_db", schema="test_db"
    )

    # Verify the results
    assert len(procedures) == 1
    assert isinstance(procedures[0], BaseProcedure)
    assert procedures[0].name == "test_proc"
    # Should fall back to ROUTINE_DEFINITION when SHOW CREATE PROCEDURE fails
    assert (
        procedures[0].procedure_definition
        == "CREATE PROCEDURE test_proc() BEGIN SELECT 1; END"
    )
