from unittest.mock import MagicMock, Mock

from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import base, mysqldb
from sqlalchemy.engine import Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source import ge_data_profiler
from datahub.ingestion.source.sql import doris as doris_module
from datahub.ingestion.source.sql.doris import (
    BITMAP,
    DORIS_ARRAY,
    HLL,
    JSONB,
    QUANTILE_STATE,
    DorisConfig,
    DorisSource,
    _get_column_types_to_ignore_with_doris,
    _patch_doris_dialect,
)
from datahub.ingestion.source.sql.sql_common import _field_type_mapping
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)


def test_doris_custom_types_registered():
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
    assert _field_type_mapping[HLL] == BytesTypeClass
    assert _field_type_mapping[BITMAP] == BytesTypeClass
    assert _field_type_mapping[QUANTILE_STATE] == BytesTypeClass
    assert _field_type_mapping[DORIS_ARRAY] == ArrayTypeClass
    assert _field_type_mapping[JSONB] == RecordTypeClass


def test_get_procedures_for_schema_returns_empty_list():
    config = DorisConfig(include_stored_procedures=False)
    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    mock_inspector = MagicMock(spec=Inspector)
    procedures = source.get_procedures_for_schema(
        inspector=mock_inspector, schema="dorisdb", db_name="dorisdb"
    )

    assert procedures == []


def test_get_procedures_for_schema_warns_when_explicitly_enabled():
    config = DorisConfig(include_stored_procedures=True)
    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    mock_inspector = MagicMock(spec=Inspector)
    procedures = source.get_procedures_for_schema(
        inspector=mock_inspector, schema="dorisdb", db_name="dorisdb"
    )

    assert procedures == []
    assert len(source.report.warnings) > 0


def test_per_engine_dialect_patching():
    """Verifies dialect patching is per-engine, not global (prevents affecting MySQL sources)."""
    assert mysqldb.MySQLDialect_mysqldb.get_columns.__name__ == "get_columns"

    mysql_engine = create_engine("mysql+pymysql://localhost:3306/test")
    assert mysql_engine.dialect.get_columns.__name__ == "get_columns"

    config = DorisConfig(host_port="localhost:9030", database="test")
    _ = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    assert mysqldb.MySQLDialect_mysqldb.get_columns.__name__ == "get_columns"
    assert mysql_engine.dialect.get_columns.__name__ == "get_columns"


def test_profiler_patch_applied_on_source_creation():
    config = DorisConfig(host_port="localhost:9030", database="test")
    DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    assert doris_module._profiler_patched is True
    assert (
        ge_data_profiler._get_column_types_to_ignore
        is _get_column_types_to_ignore_with_doris
    )


def test_profiler_excludes_doris_types():
    ignored_types = ge_data_profiler._get_column_types_to_ignore("mysql")

    assert "HLL" in ignored_types
    assert "BITMAP" in ignored_types
    assert "QUANTILE_STATE" in ignored_types
    assert "ARRAY" in ignored_types
    assert "JSONB" in ignored_types


def test_describe_query_failure_graceful_degradation():
    """Verifies graceful fallback to MySQL types when DESCRIBE fails."""
    mock_dialect = Mock(spec=mysqldb.MySQLDialect_mysqldb)
    mock_dialect.identifier_preparer = Mock()
    mock_dialect.identifier_preparer.quote_identifier = lambda x: f"`{x}`"

    base_columns = [
        {"name": "id", "type": "INTEGER", "full_type": "int"},
        {"name": "hll_col", "type": "BLOB", "full_type": "blob"},
    ]
    mock_dialect.get_columns = Mock(return_value=base_columns)

    _patch_doris_dialect(mock_dialect)

    mock_connection = Mock()
    mock_connection.engine.url.database = "test_db"
    mock_connection.execute.side_effect = Exception("DESCRIBE query failed")

    columns = mock_dialect.get_columns(
        mock_connection, table_name="test_table", schema="test_db"
    )

    assert len(columns) == 2
    assert columns[0]["name"] == "id"
    assert columns[1]["name"] == "hll_col"
    assert columns[1]["full_type"] == "blob"
