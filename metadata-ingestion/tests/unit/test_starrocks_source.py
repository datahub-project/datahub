from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.starrocks import StarRocksSource


def test_platform_correctly_set_mariadb():
    source = StarRocksSource(
        ctx=PipelineContext(run_id="starrocks-source-test"),
        config=MySQLConfig(),
    )
    assert source.platform == "starrocks"


def test_platform_correctly_set_mysql():
    source = MySQLSource(
        ctx=PipelineContext(run_id="mysql-source-test"),
        config=MySQLConfig(),
    )
    assert source.platform == "mysql"
