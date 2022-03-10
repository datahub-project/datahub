from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hana import HanaConfig, HanaSource


def test_platform_correctly_set_hana():
    source = HanaSource(
        ctx=PipelineContext(run_id="hana-source-test"),
        config=HanaConfig(),
    )
    assert source.platform == "hana"


def test_hana_uri_native():
    config = HanaConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:39041",
            "scheme": "hana+hdbcli",
        }
    )
    assert config.get_sql_alchemy_url() == "hana+hdbcli://user:password@host:39041"


def test_hana_uri_native_db():
    config = HanaConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:39041",
            "scheme": "hana+hdbcli",
            "database": "database",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "hana+hdbcli://user:password@host:39041/database"
    )
