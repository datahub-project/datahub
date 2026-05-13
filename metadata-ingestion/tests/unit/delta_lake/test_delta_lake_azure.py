import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.delta_lake.config import (
    AzureBlob,
    DeltaLakeSourceConfig,
)
from datahub.ingestion.source.delta_lake.source import DeltaLakeSource


@pytest.mark.parametrize(
    "base_path",
    [
        "abfss://container@acct.dfs.core.windows.net/delta/table",
        "abfs://container@acct.dfs.core.windows.net/delta/table",
        "az://container/delta/table",
        "adl://container/delta/table",
        "https://acct.blob.core.windows.net/container/delta/table",
        "https://acct.dfs.core.windows.net/container/delta/table",
    ],
)
def test_delta_lake_config_detects_azure_paths(base_path: str) -> None:
    config = DeltaLakeSourceConfig.model_validate(
        {
            "base_path": base_path,
            "azure": {
                "account_name": "acct",
            },
        }
    )
    assert config.is_azure is True


def test_delta_lake_azure_config_rejects_partial_service_principal() -> None:
    with pytest.raises(
        ValueError,
        match="Service principal auth requires `client_id`, `client_secret`, and `tenant_id`",
    ):
        AzureBlob.model_validate({"client_id": "my-client-id"})


@pytest.mark.parametrize(
    "azure_config",
    [
        {"account_key": "my-secret", "sas_token": "my-sas"},
        {
            "account_key": "my-secret",
            "credential": AzureCredentialConfig(
                authentication_method="cli"
            ).model_dump(),
        },
        {
            "sas_token": "my-sas",
            "client_id": "my-client-id",
            "client_secret": "my-client-secret",
            "tenant_id": "my-tenant-id",
        },
    ],
)
def test_delta_lake_azure_config_rejects_multiple_auth_methods(
    azure_config: dict[str, object],
) -> None:
    with pytest.raises(
        ValueError,
        match="Azure auth configuration accepts only one method at a time",
    ):
        AzureBlob.model_validate(azure_config)


def test_delta_lake_azure_storage_options_from_account_key() -> None:
    config = DeltaLakeSourceConfig.model_validate(
        {
            "base_path": "abfss://container@acct.dfs.core.windows.net/delta/table",
            "azure": {"account_key": "my-secret"},
        }
    )
    source = DeltaLakeSource(config, PipelineContext(run_id="delta-lake-azure-test"))

    storage_options = source.get_storage_options()

    assert storage_options["account_name"] == "acct"
    assert storage_options["container_name"] == "container"
    assert storage_options["access_key"] == "my-secret"


def test_delta_lake_azure_get_folders_uses_shared_helpers() -> None:
    config = DeltaLakeSourceConfig.model_validate(
        {
            "base_path": "abfss://container@acct.dfs.core.windows.net/delta/table",
            "azure": {"account_key": "my-secret", "account_name": "acct"},
        }
    )
    source = DeltaLakeSource(config, PipelineContext(run_id="delta-lake-azure-test"))
    azure_config = source._build_azure_connection_config(config.base_path)

    assert azure_config is not None
    assert azure_config.get_abfss_url("delta/folder") == (
        "abfss://container@acct.dfs.core.windows.net/delta/folder"
    )


def test_delta_lake_folder_listing_rejects_unified_credential() -> None:
    config = DeltaLakeSourceConfig.model_validate(
        {
            "base_path": "abfss://container@acct.dfs.core.windows.net/delta",
            "azure": {
                "credential": AzureCredentialConfig(
                    authentication_method="cli"
                ).model_dump()
            },
        }
    )
    source = DeltaLakeSource(config, PipelineContext(run_id="delta-lake-azure-test"))

    with pytest.raises(
        ValueError,
        match="Azure folder discovery reuses shared Azure Blob helpers",
    ):
        list(source.azure_get_folders(config.base_path))
