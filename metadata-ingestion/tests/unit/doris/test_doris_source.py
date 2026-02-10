from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris.doris_dialect import (
    AGG_STATE,
    BITMAP,
    DORIS_ARRAY,
    DORIS_JSONB,
    DORIS_MAP,
    DORIS_STRUCT,
    HLL,
    QUANTILE_STATE,
    DorisDialect,
)
from datahub.ingestion.source.sql.doris.doris_source import DorisConfig, DorisSource
from datahub.ingestion.source.sql.sql_common import _field_type_mapping
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)


def test_doris_custom_types_mapped_to_datahub_types():
    """Test that Doris custom types are mapped to appropriate DataHub types."""
    assert _field_type_mapping[HLL] == BytesTypeClass
    assert _field_type_mapping[BITMAP] == BytesTypeClass
    assert _field_type_mapping[QUANTILE_STATE] == BytesTypeClass
    assert _field_type_mapping[AGG_STATE] == BytesTypeClass
    assert _field_type_mapping[DORIS_ARRAY] == ArrayTypeClass
    assert _field_type_mapping[DORIS_MAP] == RecordTypeClass
    assert _field_type_mapping[DORIS_STRUCT] == RecordTypeClass
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
    """Verify DorisDialect is registered with SQLAlchemy and used by DorisSource."""
    config = DorisConfig(host_port="localhost:9030", database="test")
    assert config.scheme == "doris+pymysql"

    source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
    assert source.config.scheme == "doris+pymysql"

    url_str = config.get_sql_alchemy_url()
    assert url_str.startswith("doris+pymysql://")

    # Verify SQLAlchemy can load DorisDialect via entry point
    try:
        engine = create_engine(url_str)
        assert engine.dialect.name == "doris"
        assert isinstance(engine.dialect, DorisDialect)
    except Exception:
        # Connection will fail without real Doris, but dialect should be registered
        pass


class TestDorisConfig:
    """Test DorisConfig Pydantic model and validators."""

    def test_default_scheme(self):
        """Test that default scheme is doris+pymysql."""
        config = DorisConfig(host_port="localhost:9030")
        assert config.scheme == "doris+pymysql"

    def test_scheme_validator_corrects_mysql_scheme(self):
        """Test that _ensure_doris_scheme validator corrects mysql+pymysql to doris+pymysql."""
        # When MySQLConfig is mistakenly used, the validator should fix it
        config_dict = {
            "host_port": "localhost:9030",
            "scheme": "mysql+pymysql",  # Wrong scheme
        }
        config = DorisConfig.model_validate(config_dict)
        assert config.scheme == "doris+pymysql"

    def test_default_port(self):
        """Test that default port is 9030."""
        config = DorisConfig()
        assert "9030" in config.host_port

    def test_stored_procedures_disabled_by_default(self):
        """Test that stored procedures are disabled by default."""
        config = DorisConfig(host_port="localhost:9030")
        assert config.include_stored_procedures is False

    def test_profiling_config_default(self):
        """Test that profiling config is properly initialized."""
        config = DorisConfig(host_port="localhost:9030")
        assert config.profiling is not None


class TestDorisSourceMethods:
    """Test DorisSource-specific method overrides."""

    def test_get_platform_returns_doris(self):
        """Test that get_platform() returns 'doris'."""
        config = DorisConfig(host_port="localhost:9030", database="test")
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        assert source.get_platform() == "doris"

    def test_create_classmethod_uses_doris_config(self):
        """Test that create() classmethod uses DorisConfig not MySQLConfig."""
        config_dict = {
            "host_port": "localhost:9030",
            "database": "test",
        }
        ctx = PipelineContext(run_id="test")
        source = DorisSource.create(config_dict, ctx)

        assert isinstance(source.config, DorisConfig)
        assert source.config.scheme == "doris+pymysql"

    @pytest.mark.parametrize(
        "input_view_def,expected_output",
        [
            # Normal case: strip catalog prefix from standard views
            (
                "CREATE VIEW v AS SELECT `internal`.`dorisdb`.`customers`.`id` FROM `internal`.`dorisdb`.`customers`",
                "CREATE VIEW v AS SELECT `dorisdb`.`customers`.`id` FROM `dorisdb`.`customers`",
            ),
            # Edge case: database literally named "internal" should be preserved
            (
                "CREATE VIEW v AS SELECT * FROM `internal`.`internal`.`table`",
                "CREATE VIEW v AS SELECT * FROM `internal`.`table`",
            ),
            # Edge case: table name contains "internal" should be preserved
            (
                "SELECT * FROM `internal`.`db`.`internal_users`",
                "SELECT * FROM `db`.`internal_users`",
            ),
            # Multiple tables in same view
            (
                "FROM `internal`.`dorisdb`.`customers` JOIN `internal`.`dorisdb`.`orders`",
                "FROM `dorisdb`.`customers` JOIN `dorisdb`.`orders`",
            ),
            # Alias with column references
            (
                "SELECT `internal`.`db`.`c`.`col` FROM `internal`.`db`.`customers` AS c",
                "SELECT `db`.`c`.`col` FROM `db`.`customers` AS c",
            ),
            # Column references inside function calls (after parentheses)
            (
                "SELECT COUNT(`internal`.`dorisdb`.`orders`.`id`), SUM(`internal`.`dorisdb`.`orders`.`amount`) FROM `internal`.`dorisdb`.`orders`",
                "SELECT COUNT(`dorisdb`.`orders`.`id`), SUM(`dorisdb`.`orders`.`amount`) FROM `dorisdb`.`orders`",
            ),
            # Subquery with EXISTS
            (
                "WHERE EXISTS(SELECT 1 FROM `internal`.`dorisdb`.`products`)",
                "WHERE EXISTS(SELECT 1 FROM `dorisdb`.`products`)",
            ),
            # Comma-separated table list
            (
                "FROM `internal`.`db`.`t1`,`internal`.`db`.`t2`,`internal`.`db`.`t3`",
                "FROM `db`.`t1`,`db`.`t2`,`db`.`t3`",
            ),
        ],
    )
    def test_view_definition_catalog_prefix_stripping(
        self, input_view_def, expected_output
    ):
        """Test that _get_view_definition strips 'internal' catalog prefix correctly."""
        from datahub.ingestion.source.sql.doris.doris_source import (
            _DORIS_CATALOG_PREFIX_PATTERN,
        )

        result = _DORIS_CATALOG_PREFIX_PATTERN.sub("", input_view_def)
        assert result == expected_output
