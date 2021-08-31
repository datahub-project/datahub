import pytest


@pytest.mark.integration
def test_druid_uri():
    from datahub.ingestion.source.sql.druid import DruidConfig

    config = DruidConfig.parse_obj({"host_port": "localhost:8082"})

    assert config.get_sql_alchemy_url() == "druid://localhost:8082/druid/v2/sql/"
