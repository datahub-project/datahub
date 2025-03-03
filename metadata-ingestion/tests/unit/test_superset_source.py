from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.superset import SupersetConfig


def test_default_values():
    config = SupersetConfig.parse_obj({})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == "http://localhost:8088"
    assert config.provider == "db"
    assert config.env == "PROD"
    assert config.username is None
    assert config.password is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = SupersetConfig.parse_obj({"display_uri": display_uri})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == display_uri


def test_patterns():
    config = SupersetConfig.parse_obj({})
    assert config.database_pattern == AllowDenyPattern.allow_all()
    assert config.dataset_pattern == AllowDenyPattern.allow_all()
    assert config.chart_pattern == AllowDenyPattern.allow_all()
    assert config.dashboard_pattern == AllowDenyPattern.allow_all()

    db_to_deny = "test_database1"
    dataset_to_deny = "conf"
    chart_to_deny = ".*internal.*"
    dashboard_patterns = [".*dev.*", ".*test.*"]

    config = SupersetConfig.parse_obj(
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
