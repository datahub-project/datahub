import pytest
from pydantic import ValidationError

from datahub.ingestion.source.sap_mdg.config import SapMdgSourceConfig

_BASE = {
    "base_url": "https://sap-gw.example.com:44300/",
    "services": ["/sap/opu/odata/sap/ZMDG_DEMO_SRV"],
}


def test_base_url_trailing_slash_stripped():
    config = SapMdgSourceConfig.model_validate(
        {**_BASE, "username": "u", "password": "p"}
    )
    assert config.base_url == "https://sap-gw.example.com:44300"


def test_credentials_required():
    with pytest.raises(ValidationError):
        SapMdgSourceConfig.model_validate(_BASE)


def test_token_auth_accepted():
    config = SapMdgSourceConfig.model_validate({**_BASE, "token": "abc"})
    assert config.token is not None


def test_basic_auth_accepted():
    config = SapMdgSourceConfig.model_validate(
        {**_BASE, "username": "u", "password": "p"}
    )
    assert config.username is not None and config.password is not None


def test_certificate_auth_accepted():
    config = SapMdgSourceConfig.model_validate(
        {**_BASE, "client_certificate_path": "/certs/client.pem"}
    )
    assert config.client_certificate_path == "/certs/client.pem"


def test_empty_services_rejected():
    with pytest.raises(ValidationError):
        SapMdgSourceConfig.model_validate(
            {"base_url": "https://x", "services": [], "token": "abc"}
        )
