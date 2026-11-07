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


def test_mixed_case_scheme_normalized_to_lowercase():
    config = AzureAnalysisServicesConfig.model_validate(
        _service_principal(server="PowerBI://api.powerbi.com/v1.0/myorg/salesws")
    )
    assert config.server == "powerbi://api.powerbi.com/v1.0/myorg/salesws"


@pytest.mark.parametrize(
    "server",
    [
        f"{_ASAZURE}/",
        f"{_ASAZURE}?foo=bar",
    ],
)
def test_server_with_trailing_suffix_rejected(server):
    # fullmatch means a trailing path/query must be rejected, not silently
    # accepted by matching only the valid prefix.
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(_service_principal(server=server))


def test_platform_powerbi_with_powerbi_endpoint_ok():
    config = AzureAnalysisServicesConfig.model_validate(
        _service_principal(server=_POWERBI, platform=constants.PLATFORM_POWERBI)
    )
    assert config.platform == constants.PLATFORM_POWERBI


def test_platform_powerbi_with_asazure_endpoint_rejected():
    # powerbi URNs only merge against a Power BI Premium XMLA endpoint; pairing
    # the powerbi platform with an asazure endpoint is a misconfiguration.
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            _service_principal(server=_ASAZURE, platform=constants.PLATFORM_POWERBI)
        )


def test_unknown_platform_value_rejected():
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            _service_principal(platform="azure-analysis-service")
        )


def test_empty_secret_rejected():
    with pytest.raises(ValidationError, match="client_secret"):
        AzureAnalysisServicesConfig.model_validate(_service_principal(client_secret=""))


def test_case_insensitive_duplicate_server_keys_rejected():
    with pytest.raises(ValidationError, match="case-insensitive duplicate"):
        AzureAnalysisServicesConfig.model_validate(
            _service_principal(
                server_to_platform_instance={
                    "EDWPSFN": {"platform_instance": "a"},
                    "edwpsfn": {"platform_instance": "b"},
                }
            )
        )


def test_athena_override_unknown_platform_rejected():
    with pytest.raises(ValidationError, match="not a recognized DataHub platform"):
        AzureAnalysisServicesConfig.model_validate(
            _service_principal(
                athena_table_platform_override=[
                    {"database": "db", "table": "t", "platform": "mysqll"}
                ]
            )
        )


def test_athena_override_values_are_stripped_and_blanks_rejected():
    config = AzureAnalysisServicesConfig.model_validate(
        _service_principal(
            athena_table_platform_override=[
                {"database": " sales ", "table": "orders ", "platform": "mysql"}
            ]
        )
    )
    [override] = config.athena_table_platform_override
    assert (override.database, override.table) == ("sales", "orders")
    with pytest.raises(ValidationError):
        AzureAnalysisServicesConfig.model_validate(
            _service_principal(
                athena_table_platform_override=[
                    {"database": "  ", "table": "orders", "platform": "mysql"}
                ]
            )
        )
