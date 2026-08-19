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
    assert config.client_id == "c"


def test_username_password_requires_fields():
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            {
                "server": _ASAZURE,
                "auth_type": AasAuthType.USERNAME_PASSWORD,
                "client_id": "c",
            }
        )


def test_device_code_requires_client_id():
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            {"server": _ASAZURE, "auth_type": AasAuthType.DEVICE_CODE}
        )


def test_interactive_requires_client_id():
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            {"server": _ASAZURE, "auth_type": AasAuthType.INTERACTIVE}
        )


def test_valid_server_endpoints_accepted():
    for server in (_ASAZURE, _POWERBI):
        config = AzureAnalysisServicesConfig.model_validate(
            _service_principal(server=server)
        )
        assert config.server == server


def test_invalid_server_rejected():
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(_service_principal(server="ftp://x"))
