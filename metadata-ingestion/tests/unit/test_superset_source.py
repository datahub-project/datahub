from datahub.ingestion.source.superset import SupersetConfig


def test_default_values():

    config = SupersetConfig.parse_obj({})

    assert config.connect_uri == "localhost:8088"
    assert config.display_uri == "localhost:8088"
    assert config.provider == "db"
    assert config.env == "PROD"
    assert config.username is None
    assert config.password is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = SupersetConfig.parse_obj({"display_uri": display_uri})

    assert config.connect_uri == "localhost:8088"
    assert config.display_uri == display_uri
