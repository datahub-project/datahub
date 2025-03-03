from datahub.configuration.common import AllowDenyPattern
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


def test_patterns():
    config = PresetConfig.parse_obj({})
    assert config.database_pattern == AllowDenyPattern.allow_all()
    assert config.dataset_pattern == AllowDenyPattern.allow_all()
    assert config.chart_pattern == AllowDenyPattern.allow_all()
    assert config.dashboard_pattern == AllowDenyPattern.allow_all()

    db_to_deny = "test_database1"
    dataset_to_deny = "conf"
    chart_to_deny = ".*internal.*"
    dashboard_patterns = [".*dev.*", ".*test.*"]

    config = PresetConfig.parse_obj(
        {
            "database_pattern": {
                "allow": [".*"],
                "deny": [db_to_deny],
                "ignoreCase": False,
            },
            "dataset_pattern": {
                "allow": [".*"],
                "deny": [dataset_to_deny],
                "ignoreCase": True,
            },
            "chart_pattern": {
                "allow": [".*"],
                "deny": [chart_to_deny],
                "ignoreCase": True,
            },
            "dashboard_pattern": {
                "deny": dashboard_patterns,
                "ignoreCase": True,
            },
        }
    )

    assert config.database_pattern.allowed("test_db2") is True
    assert config.database_pattern.allowed(db_to_deny) is False

    assert config.dataset_pattern.allowed("public_data") is True
    assert config.dataset_pattern.allowed(dataset_to_deny) is False
    assert config.dataset_pattern.allowed("CONFIDENTIAL_DATA") is False

    assert config.chart_pattern.allowed("Public Revenue Chart") is True
    assert config.chart_pattern.allowed("Internal Revenue Chart") is False
    assert config.chart_pattern.allowed("INTERNAL User Stats") is False

    assert config.dashboard_pattern.allowed("Production Dashboard") is True
    assert config.dashboard_pattern.allowed("Development Dashboard") is False
    assert config.dashboard_pattern.allowed("TEST Dashboard") is False
    assert config.dashboard_pattern.allowed("User Testing Results") is False
