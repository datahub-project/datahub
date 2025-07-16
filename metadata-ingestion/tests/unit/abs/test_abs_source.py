from unittest.mock import Mock, patch

import pytest
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig


def test_service_principal_credentials_return_objects():
    """Service principal credentials must return ClientSecretCredential objects, not strings"""
    config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        client_id="test-client-id",
        client_secret="test-client-secret",
        tenant_id="test-tenant-id",
    )

    credential = config.get_credentials()

    assert isinstance(credential, ClientSecretCredential)
    assert not isinstance(credential, str)


@pytest.mark.parametrize(
    "auth_type,config_params,expected_type",
    [
        (
            "service_principal",
            {
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "tenant_id": "test-tenant-id",
            },
            ClientSecretCredential,
        ),
        ("account_key", {"account_key": "test-account-key"}, str),
        ("sas_token", {"sas_token": "test-sas-token"}, str),
    ],
)
def test_credential_types_by_auth_method(auth_type, config_params, expected_type):
    """Test that different authentication methods return correct credential types"""
    base_config = {"account_name": "testaccount", "container_name": "testcontainer"}
    config = AzureConnectionConfig(**{**base_config, **config_params})

    credential = config.get_credentials()
    assert isinstance(credential, expected_type)


def test_credential_object_not_converted_to_string():
    """Credential objects should not be accidentally converted to strings via f-string formatting"""
    config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        client_id="test-client-id",
        client_secret="test-client-secret",
        tenant_id="test-tenant-id",
    )

    credential = config.get_credentials()
    credential_as_string = f"{credential}"

    assert isinstance(credential, ClientSecretCredential)
    assert credential != credential_as_string
    assert "ClientSecretCredential" in str(credential)


@pytest.mark.parametrize(
    "service_client_class,method_name",
    [
        (BlobServiceClient, "get_blob_service_client"),
        (DataLakeServiceClient, "get_data_lake_service_client"),
    ],
)
def test_service_clients_receive_credential_objects(service_client_class, method_name):
    """Both BlobServiceClient and DataLakeServiceClient should receive credential objects"""
    config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        client_id="test-client-id",
        client_secret="test-client-secret",
        tenant_id="test-tenant-id",
    )

    with patch(
        f"datahub.ingestion.source.azure.azure_common.{service_client_class.__name__}"
    ) as mock_client:
        getattr(config, method_name)()

        mock_client.assert_called_once()
        credential = mock_client.call_args[1]["credential"]
        assert isinstance(credential, ClientSecretCredential)
        assert not isinstance(credential, str)


@pytest.mark.parametrize(
    "deprecated_param,new_param",
    [
        ("prefix", "name_starts_with"),
    ],
)
def test_azure_sdk_parameter_deprecation(deprecated_param, new_param):
    """Test that demonstrates the Azure SDK parameter deprecation issue"""
    # This test shows why the fix was needed - deprecated params cause errors
    mock_container_client = Mock()

    def list_blobs_with_validation(**kwargs):
        if deprecated_param in kwargs:
            raise ValueError(
                f"Passing '{deprecated_param}' has no effect on filtering, please use the '{new_param}' parameter instead."
            )
        return []

    mock_container_client.list_blobs.side_effect = list_blobs_with_validation

    # Test that the deprecated parameter causes an error (this is what was happening before the fix)
    with pytest.raises(ValueError) as exc_info:
        mock_container_client.list_blobs(
            **{deprecated_param: "test/path", "results_per_page": 1000}
        )

    assert new_param in str(exc_info.value)
    assert deprecated_param in str(exc_info.value)

    # Test that the new parameter works (this is what the fix implemented)
    mock_container_client.list_blobs.side_effect = None
    mock_container_client.list_blobs.return_value = []

    result = mock_container_client.list_blobs(
        **{new_param: "test/path", "results_per_page": 1000}
    )
    assert result == []


@patch("datahub.ingestion.source.azure.azure_common.BlobServiceClient")
def test_datahub_source_uses_correct_azure_parameters(mock_blob_service_client_class):
    """Test that DataHub source code actually uses the correct Azure SDK parameters"""
    # This test verifies that the real DataHub code calls Azure SDK with correct parameters
    mock_container_client = Mock()
    mock_blob_service_client = Mock()
    mock_blob_service_client.get_container_client.return_value = mock_container_client
    mock_blob_service_client_class.return_value = mock_blob_service_client

    # Mock the blob objects returned by list_blobs
    mock_blob = Mock()
    mock_blob.name = "test/path/file.csv"
    mock_blob.size = 1024
    mock_container_client.list_blobs.return_value = [mock_blob]

    # Now test the REAL DataHub code
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.abs.config import DataLakeSourceConfig
    from datahub.ingestion.source.abs.source import ABSSource
    from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

    # Create real DataHub source
    source_config = DataLakeSourceConfig(
        platform="abs",
        azure_config=AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
        ),
        path_specs=[
            PathSpec(
                include="https://testaccount.blob.core.windows.net/testcontainer/test/*.*",
                exclude=[],
                file_types=["csv"],
                sample_files=False,
            )
        ],
    )

    pipeline_context = PipelineContext(run_id="test-run-id", pipeline_name="abs-source")
    pipeline_context.graph = Mock()
    source = ABSSource(source_config, pipeline_context)

    # Call the REAL DataHub method
    with patch(
        "datahub.ingestion.source.abs.source.get_container_relative_path",
        return_value="test/path",
    ):
        path_spec = source_config.path_specs[0]
        list(source.abs_browser(path_spec, 100))

    # NOW verify the real DataHub code called Azure SDK with correct parameters
    mock_container_client.list_blobs.assert_called_once_with(
        name_starts_with="test/path", results_per_page=1000
    )

    # Verify the fix worked - no deprecated 'prefix' parameter
    call_args = mock_container_client.list_blobs.call_args
    assert "name_starts_with" in call_args[1]
    assert "prefix" not in call_args[1]


def test_account_key_authentication():
    """Test that account key authentication returns string credentials"""
    config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        account_key="test-account-key",
    )

    credential = config.get_credentials()
    assert isinstance(credential, str)
    assert credential == "test-account-key"


def test_sas_token_authentication():
    """Test that SAS token authentication returns string credentials"""
    config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        sas_token="test-sas-token",
    )

    credential = config.get_credentials()
    assert isinstance(credential, str)
    assert credential == "test-sas-token"
