import pytest
from pydantic import ValidationError

from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AasAuthType,
    AzureAnalysisServicesConfig,
)

_ASAZURE = "asazure://westeurope.asazure.windows.net/myserver"
_POWERBI = "powerbi://api.powerbi.com/v1.0/myorg/salesws"


def _service_principal(**overrides):
    base = {
        "server": _ASAZURE,
        "auth_type": AasAuthType.SERVICE_PRINCIPAL,
        "tenant_id": "t",
        "client_id": "c",
        "client_secret": "s",
    }
    base.update(overrides)
    return base


def test_service_principal_requires_credentials():
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            {"server": _ASAZURE, "auth_type": AasAuthType.SERVICE_PRINCIPAL}
        )


def test_service_principal_valid():
    config = AzureAnalysisServicesConfig.model_validate(_service_principal())
    assert config.platform == constants.PLATFORM_AAS
    assert config.client_secret is not None
    assert config.client_secret.get_secret_value() == "s"


def test_username_password_requires_fields():
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            {
                "server": _ASAZURE,
                "auth_type": AasAuthType.USERNAME_PASSWORD,
                "client_id": "c",
            }
        )


def test_asazure_endpoint_regex():
    match = constants.ASAZURE_ENDPOINT_RE.match(_ASAZURE)
    assert match is not None
    assert match.group("region") == "westeurope"
    assert match.group("server") == "myserver"


def test_powerbi_endpoint_regex():
    match = constants.POWERBI_ENDPOINT_RE.match(_POWERBI)
    assert match is not None
    assert match.group("workspace") == "salesws"
