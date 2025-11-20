from unittest.mock import MagicMock, patch

from pydantic import SecretStr
from sqlalchemy.engine import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure


def test_stored_procedure_parsing():
    """Test MySQL-specific parsing of stored procedures from information_schema"""
    from datetime import datetime
    from unittest.mock import MagicMock

    from sqlalchemy.engine.reflection import Inspector

    # Mock inspector and connection
    mock_inspector = MagicMock(spec=Inspector)
    mock_engine = MagicMock()
    mock_conn = MagicMock()

    mock_inspector.engine = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    # Create a MySQL source instance
    config = MySQLConfig(
        host_port="localhost:3306", username="test", password="test", database="test"
    )
    ctx = PipelineContext(run_id="test")
    mysql_source = MySQLSource(config, ctx)

    # Mock SQL result - create mock Row objects that support dict() conversion
    def create_mock_row(data):
        """Create a mock SQLAlchemy Row that supports dict() conversion"""
        mock_row = MagicMock()
        # Make dict(row) return the data
        mock_row.__iter__ = lambda: iter(data.items())
        mock_row.keys = lambda: data.keys()
        mock_row.__getitem__ = lambda self, key: data[key]
        return mock_row

    test_data = [
        {
            "name": "simple_proc",
            "definition": "BEGIN SELECT 1; END",
            "comment": "Simple procedure",
            "CREATED": datetime(2024, 1, 1, 10, 0, 0),
            "LAST_ALTERED": datetime(2024, 1, 2, 11, 30, 0),
            "SQL_DATA_ACCESS": "MODIFIES",
            "SECURITY_TYPE": "DEFINER",
            "DEFINER": "root@localhost",
        },
        {
            "name": "proc_with_nulls",
            "definition": "BEGIN SELECT * FROM users; END",
            "comment": None,  # Test None handling
            "CREATED": None,
            "LAST_ALTERED": None,
            "SQL_DATA_ACCESS": None,
            "SECURITY_TYPE": "INVOKER",
            "DEFINER": "admin@%",
        },
    ]

    test_rows = [create_mock_row(data) for data in test_data]

    mock_result = MagicMock()
    mock_result.__iter__.return_value = iter(test_rows)
    mock_conn.execute.return_value = mock_result

    # Call the actual method
    procedures = mysql_source.get_procedures_for_schema(
        inspector=mock_inspector, schema="test_db", db_name="test_db"
    )

    # Verify parsing
    assert len(procedures) == 2

    # Test first procedure with all fields populated
    proc1 = procedures[0]
    assert proc1.name == "simple_proc"
    assert proc1.language == "SQL"
    assert proc1.procedure_definition == "BEGIN SELECT 1; END"
    assert proc1.comment == "Simple procedure"
    assert proc1.created == datetime(2024, 1, 1, 10, 0, 0)
    assert proc1.last_altered == datetime(2024, 1, 2, 11, 30, 0)
    assert proc1.extra_properties is not None
    assert proc1.extra_properties["sql_data_access"] == "MODIFIES"
    assert proc1.extra_properties["security_type"] == "DEFINER"
    assert proc1.extra_properties["definer"] == "root@localhost"

    # Test second procedure with None/null handling
    proc2 = procedures[1]
    assert proc2.name == "proc_with_nulls"
    assert proc2.comment is None
    assert proc2.created is None
    assert proc2.last_altered is None
    assert proc2.extra_properties is not None
    assert "sql_data_access" not in proc2.extra_properties  # None values excluded
    assert proc2.extra_properties["security_type"] == "INVOKER"


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
