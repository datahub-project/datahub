from datahub.ingestion.source.sql.druid import DruidConfig


def test_druid_uri():
    config = DruidConfig.parse_obj({"host_port": "localhost:8082"})

    assert config.get_sql_alchemy_url() == "druid://localhost:8082/druid/v2/sql/"


def test_druid_get_identifier():
    config = DruidConfig.parse_obj({"host_port": "localhost:8082"})

    assert config.get_identifier("schema", "table") == "table"
