import pytest

from datahub.ingestion.source.qlik_sense.config import QlikDeployment, QlikSourceConfig


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
    assert config.deployment == QlikDeployment.CLOUD


def test_cloud_config_requires_api_key():
    with pytest.raises(ValueError, match="tenant_hostname and api_key"):
        QlikSourceConfig.model_validate({"tenant_hostname": "tenant.example"})


def test_onprem_config_requires_certificates():
    with pytest.raises(ValueError, match="client_cert"):
        QlikSourceConfig.model_validate(
            {"deployment": "on_prem", "server": "qlik.example.com"}
        )


def test_onprem_config_accepts_certificate_paths():
    config = QlikSourceConfig.model_validate(
        {
            "deployment": "on_prem",
            "server": "qlik.example.com",
            "client_cert": "/secrets/client.pem",
            "client_key": "/secrets/client_key.pem",
            "root_cert": "/secrets/root.pem",
        }
    )

    assert config.deployment == QlikDeployment.ON_PREM
    assert config.qrs_port == 4242
    assert config.engine_port == 4747
    assert config.qlik_user_header(config.qrs_user_id) == (
        "UserDirectory=INTERNAL; UserId=sa_repository"
    )
    assert config.ui_hostname() == "qlik.example.com"
