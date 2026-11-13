import pytest

from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from tests.unit.qlik_sense.qlik_fakes import ON_PREM


def test_stateful_ingestion_supports_stale_removal_config():
    config = QlikSourceConfig.model_validate(
        {
            "tenant_hostname": "tenant.example",
            "api_key": "secret",
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
            },
        }
    )

    assert config.stateful_ingestion is not None
    assert config.stateful_ingestion.remove_stale_metadata is True


def test_cloud_config_requires_api_key():
    with pytest.raises(ValueError, match="tenant_hostname and api_key"):
        QlikSourceConfig.model_validate({"tenant_hostname": "tenant.example"})


def test_cloud_config_error_points_to_on_prem_block():
    with pytest.raises(ValueError, match="on_prem block"):
        QlikSourceConfig.model_validate({})


def test_on_prem_fields_outside_the_on_prem_block_are_rejected():
    with pytest.raises(ValueError, match="server"):
        QlikSourceConfig.model_validate({"server": "qlik.example.com"})


def test_on_prem_block_cannot_be_combined_with_cloud_credentials():
    with pytest.raises(ValueError, match="not both"):
        QlikSourceConfig.model_validate(
            {
                "tenant_hostname": "tenant.example",
                "api_key": "secret",
                "on_prem": ON_PREM,
            }
        )


def test_onprem_config_rejects_invalid_port():
    with pytest.raises(ValueError, match="qrs_port"):
        QlikSourceConfig.model_validate({"on_prem": {**ON_PREM, "qrs_port": 70000}})


def test_onprem_config_requires_certificates():
    with pytest.raises(ValueError, match="client_cert"):
        QlikSourceConfig.model_validate({"on_prem": {"server": "qlik.example.com"}})


@pytest.mark.parametrize("field", ["server", "client_cert", "client_key", "root_cert"])
def test_onprem_config_rejects_empty_connection_fields(field):
    with pytest.raises(ValueError, match=field):
        QlikSourceConfig.model_validate({"on_prem": {**ON_PREM, field: ""}})


def test_onprem_config_accepts_certificate_paths():
    config = QlikSourceConfig.model_validate({"on_prem": ON_PREM})

    assert config.on_prem is not None
    assert config.on_prem.qlik_user_header(config.on_prem.qrs_user_id) == (
        "UserDirectory=INTERNAL; UserId=sa_repository"
    )
    assert config.ui_hostname() == "qlik.example.com"


@pytest.mark.parametrize(
    "config, workers",
    [
        pytest.param(
            {"tenant_hostname": "tenant.example", "api_key": "k"}, 1, id="cloud"
        ),
        # Every app opened with its data is held in Engine memory.
        pytest.param(
            {"on_prem": {**ON_PREM, "read_data_model": True}}, 1, id="on_prem"
        ),
        pytest.param(
            {"on_prem": {**ON_PREM, "read_data_model": False}},
            4,
            id="on_prem_without_data",
        ),
        pytest.param(
            {"tenant_hostname": "tenant.example", "api_key": "k", "max_workers": 3},
            3,
            id="set",
        ),
    ],
)
def test_max_workers_default_depends_on_deployment(config, workers):
    assert QlikSourceConfig.model_validate(config).resolved_max_workers() == workers
