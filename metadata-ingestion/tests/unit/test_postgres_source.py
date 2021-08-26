from datahub.ingestion.source.sql.postgres import PostgresConfig


def _base_config():
    return {"username": "user", "password": "password", "host_port": "host:1521"}


def test_database_alias_takes_precendence():
    config = PostgresConfig.parse_obj(
        {
            **_base_config(),
            "database_alias": "ops_database",
            "database": "postgres",
        }
    )
    assert config.get_identifier("superset", "logs") == "ops_database.superset.logs"


def test_database_in_identifier():
    config = PostgresConfig.parse_obj({**_base_config(), "database": "postgres"})
    assert config.get_identifier("superset", "logs") == "postgres.superset.logs"
