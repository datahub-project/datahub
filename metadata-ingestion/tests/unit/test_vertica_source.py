from datahub.ingestion.source.sql.vertica import VerticaConfig


def test_vertica_uri_https():
    config = VerticaConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:5433",
            "database": "db",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "vertica+vertica_python://user:password@host:5433/db"
    )
