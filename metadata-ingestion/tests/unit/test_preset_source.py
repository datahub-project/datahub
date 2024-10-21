from datahub.ingestion.source.preset import PresetConfig


def test_default_values():
    config = PresetConfig.parse_obj({})

    assert config.connect_uri == ""
    assert config.manager_uri == "https://api.app.preset.io"
    assert config.display_uri == ""
    assert config.env == "PROD"
    assert config.api_key is None
    assert config.api_secret is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = PresetConfig.parse_obj({"display_uri": display_uri})

    assert config.connect_uri == ""
    assert config.manager_uri == "https://api.app.preset.io"
    assert config.display_uri == display_uri
