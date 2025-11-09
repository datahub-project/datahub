from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris import DorisConfig, DorisSource
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


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
