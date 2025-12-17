from unittest.mock import MagicMock

from sqlalchemy.dialects.mysql import base
from sqlalchemy.engine import Inspector

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


def test_get_procedures_for_schema_returns_empty_list():
    """Test that get_procedures_for_schema always returns empty list for Doris"""
    config = DorisConfig(include_stored_procedures=False)
    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    mock_inspector = MagicMock(spec=Inspector)
    procedures = source.get_procedures_for_schema(
        inspector=mock_inspector, schema="dorisdb", db_name="dorisdb"
    )

    assert procedures == []
    assert len(procedures) == 0


def test_get_procedures_for_schema_warns_when_explicitly_enabled():
    """Test that enabling stored procedures generates a warning for Doris"""
    config = DorisConfig(include_stored_procedures=True)
    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    mock_inspector = MagicMock(spec=Inspector)
    procedures = source.get_procedures_for_schema(
        inspector=mock_inspector, schema="dorisdb", db_name="dorisdb"
    )

    # Should still return empty list even when explicitly enabled
    assert procedures == []
    # Should have generated a warning about stored procedures
    assert len(source.report.warnings) > 0


def test_doris_config_default_port():
    """Test that DorisConfig uses correct default port (9030)"""
    config = DorisConfig()
    assert config.host_port == "localhost:9030"
    assert "9030" in config.host_port


def test_doris_config_custom_port():
    """Test that DorisConfig accepts custom port"""
    config = DorisConfig(host_port="doris.example.com:9999")
    assert config.host_port == "doris.example.com:9999"


def test_doris_type_registration_case_insensitive():
    """Test that Doris types work regardless of case"""
    # Test lowercase
    assert base.ischema_names["hll"] == HLL
    assert base.ischema_names["bitmap"] == BITMAP
    assert base.ischema_names["array"] == DORIS_ARRAY
    assert base.ischema_names["jsonb"] == JSONB
    assert base.ischema_names["quantile_state"] == QUANTILE_STATE

    # Test uppercase
    assert base.ischema_names["HLL"] == HLL
    assert base.ischema_names["BITMAP"] == BITMAP
    assert base.ischema_names["ARRAY"] == DORIS_ARRAY
    assert base.ischema_names["JSONB"] == JSONB
    assert base.ischema_names["QUANTILE_STATE"] == QUANTILE_STATE

    # Verify they're the same type instance
    assert base.ischema_names["hll"] is base.ischema_names["HLL"]
    assert base.ischema_names["bitmap"] is base.ischema_names["BITMAP"]


def test_doris_vs_mysql_stored_procedure_defaults():
    """Test that Doris and MySQL have different defaults for stored procedures"""
    doris_config = DorisConfig()
    mysql_config = MySQLConfig()

    # Doris should default to False
    assert doris_config.include_stored_procedures is False

    # MySQL should default to True
    assert mysql_config.include_stored_procedures is True


def test_doris_config_validation():
    """Test DorisConfig validation for edge cases"""
    # Valid minimal config
    config = DorisConfig(username="root")
    assert config.username == "root"

    # Valid config with all Doris-specific fields
    config = DorisConfig(
        host_port="doris-cluster.example.com:9030",
        username="admin",
        password="secret",
        database="analytics",
        include_stored_procedures=False,
    )
    assert config.host_port == "doris-cluster.example.com:9030"
    assert config.database == "analytics"
    assert config.include_stored_procedures is False


def test_doris_source_platform_name():
    """Test that DorisSource returns correct platform identifier"""
    config = DorisConfig()
    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)

    # Platform should be 'doris', not 'mysql'
    assert source.get_platform() == "doris"
    assert source.get_platform() != "mysql"
    assert source.platform == "doris"
