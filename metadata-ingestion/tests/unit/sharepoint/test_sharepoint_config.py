from typing import Dict

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.sharepoint.sharepoint_config import (
    SUPPORTED_FILE_TYPES,
    SharePointAuthConfig,
    SharePointSiteConfig,
    SharePointSourceConfig,
)

_BASE_CONFIG: Dict[str, object] = {
    "auth": {
        "tenant_id": "tenant-abc",
        "client_id": "app-123",
        "client_secret": "secret-xyz",
    },
    "site": {
        "hostname": "myorg.sharepoint.com",
    },
}


def test_auth_client_secret_valid() -> None:
    config = SharePointAuthConfig.model_validate(
        {"tenant_id": "t1", "client_id": "c1", "client_secret": "s1"}
    )
    assert config.client_secret is not None
    assert config.client_secret.get_secret_value() == "s1"
    assert "s1" not in repr(config.client_secret)
    assert config.certificate_path is None


def test_auth_certificate_valid() -> None:
    config = SharePointAuthConfig.model_validate(
        {"tenant_id": "t1", "client_id": "c1", "certificate_path": "/path/cert.pem"}
    )
    assert config.certificate_path == "/path/cert.pem"
    assert config.client_secret is None


def test_auth_requires_at_least_one_credential() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SharePointAuthConfig.model_validate({"tenant_id": "t1", "client_id": "c1"})
    assert "Either client_secret or certificate_path" in str(exc_info.value)


def test_auth_rejects_both_credentials() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SharePointAuthConfig.model_validate(
            {
                "tenant_id": "t1",
                "client_id": "c1",
                "client_secret": "s1",
                "certificate_path": "/path/cert.pem",
            }
        )
    assert "not both" in str(exc_info.value)


def test_site_config_normalizes_hostname_protocol() -> None:
    config = SharePointSiteConfig.model_validate(
        {"hostname": "https://myorg.sharepoint.com/"}
    )
    assert config.hostname == "myorg.sharepoint.com"


def test_site_config_normalizes_trailing_slash() -> None:
    config = SharePointSiteConfig.model_validate({"hostname": "myorg.sharepoint.com/"})
    assert config.hostname == "myorg.sharepoint.com"


def test_site_config_skip_personal_sites_default_true() -> None:
    config = SharePointSiteConfig.model_validate({"hostname": "myorg.sharepoint.com"})
    assert config.skip_personal_sites is True


def test_site_config_skip_personal_sites_can_be_disabled() -> None:
    config = SharePointSiteConfig.model_validate(
        {"hostname": "myorg.sharepoint.com", "skip_personal_sites": False}
    )
    assert config.skip_personal_sites is False


def test_site_config_allow_deny_pattern() -> None:
    config = SharePointSiteConfig.model_validate(
        {
            "hostname": "myorg.sharepoint.com",
            "site_pattern": {
                "allow": ["/sites/Engineering"],
                "deny": ["/sites/Archive"],
            },
        }
    )
    assert config.site_pattern.allowed("/sites/Engineering")
    assert not config.site_pattern.allowed("/sites/Archive")
    assert not config.site_pattern.allowed("/sites/Marketing")


def test_full_config_valid() -> None:
    config = SharePointSourceConfig.model_validate(_BASE_CONFIG)
    assert config.auth.tenant_id == "tenant-abc"
    assert config.site.hostname == "myorg.sharepoint.com"
    assert config.mode == "data_lake"
    assert config.env == "PROD"


def test_data_lake_mode_valid() -> None:
    config = SharePointSourceConfig.model_validate(
        {**_BASE_CONFIG, "mode": "data_lake", "file_types": ["csv", "parquet"]}
    )
    assert config.mode == "data_lake"
    assert "csv" in config.file_types
    assert "parquet" in config.file_types


def test_invalid_mode_raises_error() -> None:
    with pytest.raises(ValidationError):
        SharePointSourceConfig.model_validate({**_BASE_CONFIG, "mode": "invalid_mode"})


def test_platform_instance_valid_characters() -> None:
    config = SharePointSourceConfig.model_validate(
        {**_BASE_CONFIG, "platform_instance": "my-org_prod"}
    )
    assert config.platform_instance == "my-org_prod"


def test_platform_instance_invalid_characters() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SharePointSourceConfig.model_validate(
            {**_BASE_CONFIG, "platform_instance": "my org!"}
        )
    assert "alphanumeric" in str(exc_info.value)


def test_platform_instance_empty_string_raises_error() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SharePointSourceConfig.model_validate({**_BASE_CONFIG, "platform_instance": ""})
    assert "empty" in str(exc_info.value)


def test_unsupported_file_type_raises_error() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SharePointSourceConfig.model_validate(
            {**_BASE_CONFIG, "mode": "data_lake", "file_types": ["csv", "docx"]}
        )
    assert "Unsupported file_types" in str(exc_info.value)


def test_document_id_pattern_default_override() -> None:
    config = SharePointSourceConfig.model_validate(_BASE_CONFIG)
    assert config.document_mapping.id_pattern == "{source_type}-{basename}"


def test_max_documents_disabled_with_minus_one() -> None:
    config = SharePointSourceConfig.model_validate(
        {**_BASE_CONFIG, "max_documents": -1}
    )
    assert config.max_documents == -1


def test_max_documents_disabled_with_zero() -> None:
    config = SharePointSourceConfig.model_validate({**_BASE_CONFIG, "max_documents": 0})
    assert config.max_documents == 0


def test_default_file_types_are_supported() -> None:
    config = SharePointSourceConfig.model_validate(_BASE_CONFIG)
    for ft in config.file_types:
        assert ft in SUPPORTED_FILE_TYPES


def test_env_defaults_to_prod() -> None:
    config = SharePointSourceConfig.model_validate(_BASE_CONFIG)
    assert config.env == "PROD"


def test_env_can_be_overridden() -> None:
    config = SharePointSourceConfig.model_validate({**_BASE_CONFIG, "env": "DEV"})
    assert config.env == "DEV"


def test_both_mode_valid() -> None:
    config = SharePointSourceConfig.model_validate(
        {**_BASE_CONFIG, "mode": "both", "file_types": ["csv", "parquet"]}
    )
    assert config.mode == "both"
    assert config.uses_data_lake()
    assert config.uses_document()


def test_data_lake_mode_uses_data_lake_not_document() -> None:
    config = SharePointSourceConfig.model_validate(
        {**_BASE_CONFIG, "mode": "data_lake"}
    )
    assert config.uses_data_lake()
    assert not config.uses_document()


def test_document_mode_uses_document_not_data_lake() -> None:
    config = SharePointSourceConfig.model_validate({**_BASE_CONFIG, "mode": "document"})
    assert not config.uses_data_lake()
    assert config.uses_document()


def test_both_mode_empty_file_types_raises_error() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SharePointSourceConfig.model_validate(
            {**_BASE_CONFIG, "mode": "both", "file_types": []}
        )
    assert "file_types cannot be empty" in str(exc_info.value)
