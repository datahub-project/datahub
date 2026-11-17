import pytest

from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig

_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;AccountName=myaccount;AccountKey=ZmFrZS1rZXk=;"
    "BlobEndpoint=http://127.0.0.1:10000/myaccount;"
)


@pytest.fixture(params=["account_key", "connection_string"])
def config(request: pytest.FixtureRequest) -> AzureConnectionConfig:
    if request.param == "connection_string":
        return AzureConnectionConfig(
            account_name="myaccount",
            container_name="mycontainer",
            connection_string=_CONNECTION_STRING,
        )
    return AzureConnectionConfig(
        account_name="myaccount",
        container_name="mycontainer",
        account_key="fake-key",
    )


def test_blob_service_client_is_cached(config: AzureConnectionConfig) -> None:
    client = config.get_blob_service_client()
    assert config.get_blob_service_client() is client


def test_data_lake_service_client_is_cached(config: AzureConnectionConfig) -> None:
    client = config.get_data_lake_service_client()
    assert config.get_data_lake_service_client() is client
