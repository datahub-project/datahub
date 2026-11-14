import requests_mock as rm

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.preset import PresetConfig, PresetSource


def test_default_values():
    config = PresetConfig.model_validate({})

    assert config.connect_uri == ""
    assert config.manager_uri == "https://api.app.preset.io"
    assert config.display_uri == ""
    assert config.env == "PROD"
    assert config.api_key is None
    assert config.api_secret is None
    assert config.dataset_pattern == AllowDenyPattern.allow_all()
    assert config.chart_pattern == AllowDenyPattern.allow_all()
    assert config.dashboard_pattern == AllowDenyPattern.allow_all()
    assert config.database_pattern == AllowDenyPattern.allow_all()


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = PresetConfig.model_validate({"display_uri": display_uri})

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
    config = PresetConfig.model_validate(preset_config)

    # Test Preset-specific fields
    assert config.api_key is not None
    assert config.api_key.get_secret_value() == "dummy_api_key"
    assert config.api_secret is not None
    assert config.api_secret.get_secret_value() == "dummy_api_secret"
    assert config.manager_uri == "https://api.app.preset.io"

    # Test that regular Superset fields are still parsed
    assert config.connect_uri == "https://preset.io"


def _build_preset_source(
    requests_mock: rm.Mocker, platform_instance: str
) -> PresetSource:
    requests_mock.post(
        "http://localhost:9090/v1/auth/",
        json={"payload": {"access_token": "dummy_token"}},
        status_code=200,
    )
    requests_mock.get("http://localhost:8088/version", json={}, status_code=200)
    requests_mock.get(
        "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
    )
    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={},
            status_code=200,
        )
    config = PresetConfig.model_validate(
        {
            "connect_uri": "http://localhost:8088",
            "manager_uri": "http://localhost:9090",
            "api_key": "dummy_api_key",
            "api_secret": "dummy_api_secret",
            "platform_instance": platform_instance,
        }
    )
    return PresetSource(
        ctx=PipelineContext(run_id="preset-platform-instance-test"), config=config
    )


def test_preset_inherits_platform_instance_from_superset(
    requests_mock: rm.Mocker,
) -> None:
    """Preset subclasses the Superset source and inherits its platform instance
    handling with no Preset-specific code. The aspect and the dataset URN must
    name the preset platform, not superset."""
    source = _build_preset_source(requests_mock, platform_instance="my_instance")

    aspect = source.get_data_platform_instance()
    dataset_urn = source.get_datasource_urn_from_id(
        {
            "result": {
                "table_name": "my_table",
                "schema": "public",
                "database": {"id": 1, "database_name": "my_database"},
            }
        },
        source.platform,
        with_instance=True,
    )

    assert aspect.platform == "urn:li:dataPlatform:preset"
    assert (
        aspect.instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:preset,my_instance)"
    )
    assert (
        dataset_urn
        == "urn:li:dataset:(urn:li:dataPlatform:preset,my_instance.my_database.public.my_table,PROD)"
    )
