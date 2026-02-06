from unittest.mock import MagicMock

from sqlalchemy.engine import Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris import (
    BITMAP,
    DORIS_ARRAY,
    DORIS_JSONB,
    HLL,
    QUANTILE_STATE,
    DorisConfig,
    DorisSource,
)
from datahub.ingestion.source.sql.sql_common import _field_type_mapping
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)


def test_doris_custom_types_registered():
    """Types are registered via DorisDialect, not globally."""
    from datahub.ingestion.source.sql.doris import DorisDialect

    dialect = DorisDialect()

    expected_types = {
        "hll": HLL,
        "bitmap": BITMAP,
        "array": DORIS_ARRAY,
        "jsonb": DORIS_JSONB,
        "quantile_state": QUANTILE_STATE,
    }

    for type_name, expected_class in expected_types.items():
        assert type_name in dialect.ischema_names
        assert dialect.ischema_names[type_name] == expected_class


def test_doris_custom_types_mapped_to_datahub_types():
    assert _field_type_mapping[HLL] == BytesTypeClass
    assert _field_type_mapping[BITMAP] == BytesTypeClass
    assert _field_type_mapping[QUANTILE_STATE] == BytesTypeClass
    assert _field_type_mapping[DORIS_ARRAY] == ArrayTypeClass
    assert _field_type_mapping[DORIS_JSONB] == RecordTypeClass


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


def test_doris_uses_native_dialect():
    """Verifies DorisSource uses native DorisDialect without monkey-patching."""
    config = DorisConfig(host_port="localhost:9030", database="test")
    assert config.scheme == "doris+pymysql"

    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
    assert source.config.scheme == "doris+pymysql"
