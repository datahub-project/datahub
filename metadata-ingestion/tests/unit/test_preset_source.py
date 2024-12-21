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


def test_preset_config_parsing():
    preset_config = {
        "connect_uri": "https://preset.io",
        "api_key": "dummy_api_key",
        "api_secret": "dummy_api_secret",
        "manager_uri": "https://api.app.preset.io",
    }

    # Tests if SupersetConfig fields are parsed extra fields correctly
    config = PresetConfig.parse_obj(preset_config)

    # Test Preset-specific fields
    assert config.api_key == "dummy_api_key"
    assert config.api_secret == "dummy_api_secret"
    assert config.manager_uri == "https://api.app.preset.io"

    # Test that regular Superset fields are still parsed
    assert config.connect_uri == "https://preset.io"
