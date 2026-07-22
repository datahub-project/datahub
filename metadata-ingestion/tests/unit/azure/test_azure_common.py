from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig


def _config() -> AzureConnectionConfig:
    return AzureConnectionConfig(
        account_name="myaccount",
        container_name="mycontainer",
        account_key="fake-key",
    )


def test_blob_service_client_is_cached() -> None:
    config = _config()
    client = config.get_blob_service_client()
    assert config.get_blob_service_client() is client


def test_data_lake_service_client_is_cached() -> None:
    config = _config()
    client = config.get_data_lake_service_client()
    assert config.get_data_lake_service_client() is client
