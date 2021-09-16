import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


def test_underlying_platform_takes_precendence():
    source = MySQLSource(
        ctx=PipelineContext(run_id="mysql-source-test"),
        config=MySQLConfig(underlying_platform="mariadb"),
    )
    assert source.get_underlying_platform() == "mariadb"


def test_underlying_platform_cannot_be_other_than_mariadb():
    with pytest.raises(ValueError):
        MySQLSource(
            ctx=PipelineContext(run_id="mysql-source-test"),
            config=MySQLConfig(underlying_platform="maria"),
        )


def test_without_underlying_platform():
    source = MySQLSource(
        ctx=PipelineContext(run_id="mysql-source-test"),
        config=MySQLConfig(),
    )
    assert source.get_underlying_platform() == "mysql"
