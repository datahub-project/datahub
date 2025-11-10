from sqlalchemy.dialects.mysql import base

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris import (
    BITMAP,
    DORIS_ARRAY,
    HLL,
    JSONB,
    QUANTILE_STATE,
    DorisConfig,
    DorisSource,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.sql_common import _field_type_mapping
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)


def test_platform_correctly_set_doris():
    source = DorisSource(
        ctx=PipelineContext(run_id="doris-source-test"),
        config=DorisConfig(),
    )
    assert source.platform == "doris"


def test_doris_stored_procedures_disabled_by_default():
    """Test that stored procedures are disabled by default for Doris"""
    config = DorisConfig()
    assert config.include_stored_procedures is False


def test_mysql_stored_procedures_enabled_by_default():
    """Test that stored procedures are enabled by default for MySQL"""
    config = MySQLConfig()
    assert config.include_stored_procedures is True


def test_platform_correctly_set_mysql():
    source = MySQLSource(
        ctx=PipelineContext(run_id="mysql-source-test"),
        config=MySQLConfig(),
    )
    assert source.platform == "mysql"


def test_doris_custom_types_registered():
    """Test that Doris-specific types are properly registered with SQLAlchemy"""
    # Test that custom types are registered in MySQL dialect
    assert "hll" in base.ischema_names
    assert "bitmap" in base.ischema_names
    assert "array" in base.ischema_names
    assert "jsonb" in base.ischema_names
    assert "quantile_state" in base.ischema_names

    # Test case insensitive versions
    assert "HLL" in base.ischema_names
    assert "BITMAP" in base.ischema_names
    assert "ARRAY" in base.ischema_names
    assert "JSONB" in base.ischema_names
    assert "QUANTILE_STATE" in base.ischema_names

    # Verify they map to the correct SQLAlchemy types
    assert base.ischema_names["hll"] == HLL
    assert base.ischema_names["bitmap"] == BITMAP
    assert base.ischema_names["array"] == DORIS_ARRAY
    assert base.ischema_names["jsonb"] == JSONB
    assert base.ischema_names["quantile_state"] == QUANTILE_STATE


def test_doris_custom_types_mapped_to_datahub_types():
    """Test that Doris custom types map to appropriate DataHub types"""
    # HLL and BITMAP should map to BytesTypeClass
    assert _field_type_mapping[HLL] == BytesTypeClass
    assert _field_type_mapping[BITMAP] == BytesTypeClass
    assert _field_type_mapping[QUANTILE_STATE] == BytesTypeClass

    # ARRAY should map to ArrayTypeClass
    assert _field_type_mapping[DORIS_ARRAY] == ArrayTypeClass

    # JSONB should map to RecordTypeClass (like JSON)
    assert _field_type_mapping[JSONB] == RecordTypeClass
