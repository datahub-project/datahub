from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import base, mysqldb
from sqlalchemy.engine import Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source import ge_data_profiler
from datahub.ingestion.source.sql import doris as doris_module
from datahub.ingestion.source.sql.doris import (
    BITMAP,
    DORIS_ARRAY,
    DORIS_DEFAULT_PORT,
    HLL,
    JSONB,
    QUANTILE_STATE,
    DorisConfig,
    DorisSource,
    _get_column_types_to_ignore_with_doris,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.sql_common import _field_type_mapping
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)


def test_platform_correctly_set():
    """Test that Doris and MySQL sources return correct platform identifiers"""
    doris_source = DorisSource(
        ctx=PipelineContext(run_id="doris-test"),
        config=DorisConfig(),
    )
    assert doris_source.platform == "doris"
    assert doris_source.get_platform() == "doris"

    mysql_source = MySQLSource(
        ctx=PipelineContext(run_id="mysql-test"),
        config=MySQLConfig(),
    )
    assert mysql_source.platform == "mysql"


def test_stored_procedures_defaults():
    """Test that stored procedures have different defaults for Doris vs MySQL"""
    doris_config = DorisConfig()
    mysql_config = MySQLConfig()

    assert doris_config.include_stored_procedures is False
    assert mysql_config.include_stored_procedures is True


def test_doris_custom_types_registered():
    """Test that Doris-specific types are properly registered with SQLAlchemy"""
    expected_types = {
        "hll": HLL,
        "bitmap": BITMAP,
        "array": DORIS_ARRAY,
        "jsonb": JSONB,
        "quantile_state": QUANTILE_STATE,
        "HLL": HLL,
        "BITMAP": BITMAP,
        "ARRAY": DORIS_ARRAY,
        "JSONB": JSONB,
        "QUANTILE_STATE": QUANTILE_STATE,
    }

    for type_name, expected_class in expected_types.items():
        assert type_name in base.ischema_names
        assert base.ischema_names[type_name] == expected_class

    assert base.ischema_names["hll"] is base.ischema_names["HLL"]
    assert base.ischema_names["bitmap"] is base.ischema_names["BITMAP"]


def test_doris_custom_types_mapped_to_datahub_types():
    """Test that Doris custom types map to appropriate DataHub types"""
    assert _field_type_mapping[HLL] == BytesTypeClass
    assert _field_type_mapping[BITMAP] == BytesTypeClass
    assert _field_type_mapping[QUANTILE_STATE] == BytesTypeClass
    assert _field_type_mapping[DORIS_ARRAY] == ArrayTypeClass
    assert _field_type_mapping[JSONB] == RecordTypeClass


def test_get_procedures_for_schema_returns_empty_list():
    """Test that get_procedures_for_schema always returns empty list for Doris"""
    config = DorisConfig(include_stored_procedures=False)
    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    mock_inspector = MagicMock(spec=Inspector)
    procedures = source.get_procedures_for_schema(
        inspector=mock_inspector, schema="dorisdb", db_name="dorisdb"
    )

    assert procedures == []


def test_get_procedures_for_schema_warns_when_explicitly_enabled():
    """Test that enabling stored procedures generates a warning for Doris"""
    config = DorisConfig(include_stored_procedures=True)
    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    mock_inspector = MagicMock(spec=Inspector)
    procedures = source.get_procedures_for_schema(
        inspector=mock_inspector, schema="dorisdb", db_name="dorisdb"
    )

    assert procedures == []
    assert len(source.report.warnings) > 0


def test_doris_config_default_port():
    """Test that DorisConfig uses correct default port"""
    config = DorisConfig()
    assert config.host_port == f"localhost:{DORIS_DEFAULT_PORT}"


def test_doris_config_custom_port():
    """Test that DorisConfig accepts custom port"""
    config = DorisConfig(host_port="doris.example.com:9999")
    assert config.host_port == "doris.example.com:9999"


def test_per_engine_dialect_patching():
    """Test that dialect patching is per-engine, not global"""
    # Before creating any Doris source, the global dialect class should be unchanged
    assert mysqldb.MySQLDialect_mysqldb.get_columns.__name__ == "get_columns"

    # Create a regular MySQL engine - should NOT be patched
    mysql_engine = create_engine("mysql+pymysql://localhost:3306/test")
    assert mysql_engine.dialect.get_columns.__name__ == "get_columns"

    # Create a Doris source (this applies profiler patch but not dialect patch)
    config = DorisConfig(host_port="localhost:9030", database="test")
    _ = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    # Note: We can't actually call get_inspectors() without a real database,
    # but we can verify the global class is still unpatched
    assert mysqldb.MySQLDialect_mysqldb.get_columns.__name__ == "get_columns"

    # The global MySQL dialect class should remain unchanged (perfect isolation!)
    assert mysql_engine.dialect.get_columns.__name__ == "get_columns"


def test_monkeypatch_applied_profiler():
    """Test that profiler monkeypatch has been applied after DorisSource creation"""
    # Create a DorisSource to trigger lazy profiler patching
    config = DorisConfig(host_port="localhost:9030", database="test")
    DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    # After DorisSource creation, profiler should be patched
    assert doris_module._profiler_patched is True
    assert (
        ge_data_profiler._get_column_types_to_ignore
        is _get_column_types_to_ignore_with_doris
    )


def test_profiler_excludes_doris_types():
    """Test that profiler excludes Doris-specific types for MySQL dialect"""
    ignored_types = ge_data_profiler._get_column_types_to_ignore("mysql")

    # Verify all Doris-specific types are in the ignored list
    assert "HLL" in ignored_types
    assert "BITMAP" in ignored_types
    assert "QUANTILE_STATE" in ignored_types
    assert "ARRAY" in ignored_types
    assert "JSONB" in ignored_types


def test_describe_query_failure_graceful_degradation():
    """Test that DESCRIBE query failures degrade gracefully to MySQL types."""
    from unittest.mock import Mock

    from datahub.ingestion.source.sql.doris import _patch_doris_dialect

    # Create a mock dialect
    mock_dialect = Mock(spec=mysqldb.MySQLDialect_mysqldb)
    mock_dialect.identifier_preparer = Mock()
    mock_dialect.identifier_preparer.quote_identifier = lambda x: f"`{x}`"

    # Mock original get_columns to return base columns
    base_columns = [
        {"name": "id", "type": "INTEGER", "full_type": "int"},
        {"name": "hll_col", "type": "BLOB", "full_type": "blob"},
    ]
    mock_dialect.get_columns = Mock(return_value=base_columns)

    # Patch the dialect
    _patch_doris_dialect(mock_dialect)

    # Create a mock connection that will fail on DESCRIBE
    mock_connection = Mock()
    mock_connection.engine.url.database = "test_db"
    mock_connection.execute.side_effect = Exception("DESCRIBE query failed")

    # Call get_columns - should gracefully fall back to MySQL types
    columns = mock_dialect.get_columns(
        mock_connection, table_name="test_table", schema="test_db"
    )

    # Should return columns despite DESCRIBE failure
    assert len(columns) == 2
    assert columns[0]["name"] == "id"
    assert columns[1]["name"] == "hll_col"
    # Types remain as MySQL types (graceful degradation)
    assert columns[1]["full_type"] == "blob"
