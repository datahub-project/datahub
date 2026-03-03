import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.delta_lake.config import (
    AzureBlob,
    DeltaLakeSourceConfig,
)
from datahub.ingestion.source.delta_lake.source import DeltaLakeSource
from datahub.metadata.schema_classes import DomainsClass


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


def test_delta_lake_strip_azure_prefix() -> None:
    config = DeltaLakeSourceConfig.model_validate(
        {
            "base_path": "abfss://container@acct.dfs.core.windows.net/delta/table",
            "azure": {"account_key": "my-secret"},
        }
    )
    source = DeltaLakeSource(config, PipelineContext(run_id="delta-lake-azure-test"))

    browse_path = source.strip_azure_prefix(
        "abfss://container@acct.dfs.core.windows.net/delta/table"
    )
    assert browse_path == "container/delta/table"


def test_delta_lake_az_path_requires_account_name() -> None:
    config = DeltaLakeSourceConfig.model_validate(
        {"base_path": "az://container/delta/table"}
    )
    source = DeltaLakeSource(config, PipelineContext(run_id="delta-lake-azure-test"))

    with pytest.raises(
        ValueError,
        match="For `az://` and `adl://` paths, set `source.config.azure.account_name`.",
    ):
        source.get_storage_options()


def test_delta_lake_domain_assignment_workunit() -> None:
    config = DeltaLakeSourceConfig.model_validate(
        {
            "base_path": "abfss://container@acct.dfs.core.windows.net/delta",
            "azure": {"account_key": "my-secret"},
            "domain": {
                "urn:li:domain:some-test": {
                    "allow": ["container/delta/.*"],
                }
            },
        }
    )
    source = DeltaLakeSource(config, PipelineContext(run_id="delta-lake-domain-test"))
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:delta-lake,container/delta/table,PROD)"
    )

    domain_workunits = list(
        source._get_domain_wu(
            dataset_name="container/delta/table",
            dataset_urn=dataset_urn,
        )
    )

    assert len(domain_workunits) == 1
    domain_aspect = domain_workunits[0].get_aspect_of_type(DomainsClass)
    assert domain_aspect is not None
    assert list(domain_aspect.domains) == ["urn:li:domain:some-test"]
