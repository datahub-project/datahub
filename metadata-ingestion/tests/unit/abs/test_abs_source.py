from unittest.mock import Mock, patch

import pytest
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure.abs_folder_utils import get_abs_tags
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig

# AccountName matches the account_name used throughout so the connection-string
# vs account_name divergence validator accepts these fixtures.
_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;AccountName=testaccount;"
    "AccountKey=c3VwZXJzZWNyZXQ=;EndpointSuffix=core.windows.net"
)


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
    "service_client_class,method_name",
    [
        (BlobServiceClient, "get_blob_service_client"),
        (DataLakeServiceClient, "get_data_lake_service_client"),
    ],
)
def test_service_clients_use_connection_string_when_set(
    service_client_class, method_name
):
    """When connection_string is set, both getters build via from_connection_string
    and never the account_url constructor — including the ADLS Gen2
    DataLakeServiceClient branch, which has no other coverage."""
    config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        connection_string=_CONNECTION_STRING,
    )

    with patch(
        f"datahub.ingestion.source.azure.azure_common.{service_client_class.__name__}"
    ) as mock_client:
        getattr(config, method_name)()

        mock_client.from_connection_string.assert_called_once_with(_CONNECTION_STRING)
        # The account_url + credential constructor must not be used.
        mock_client.assert_not_called()


def test_connection_string_takes_precedence_over_account_key():
    """The connection_string description claims precedence; enforce it. With both
    connection_string and account_key set, the client is built from the connection
    string, not the account_url + account_key path."""
    config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        account_key="test-account-key",
        connection_string=_CONNECTION_STRING,
    )

    with patch(
        "datahub.ingestion.source.azure.azure_common.BlobServiceClient"
    ) as mock_client:
        config.get_blob_service_client()

        mock_client.from_connection_string.assert_called_once_with(_CONNECTION_STRING)
        mock_client.assert_not_called()


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


@patch("datahub.ingestion.source.azure.azure_common.BlobServiceClient")
def test_abs_source_wires_file_profiler_when_profiling_enabled(mock_blob_client):
    """With profiling enabled, ABSSource wires the pure-Python FileProfiler
    (no PySpark) and hands it the Azure connection config."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.abs.config import DataLakeSourceConfig
    from datahub.ingestion.source.abs.source import ABSSource
    from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
    from datahub.ingestion.source.data_lake_common.profiling.profiler import (
        FileProfiler,
    )

    azure_config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        account_key="test-account-key",
    )
    source_config = DataLakeSourceConfig(
        platform="abs",
        azure_config=azure_config,
        path_specs=[
            PathSpec(
                include="https://testaccount.blob.core.windows.net/testcontainer/test/*.*",
                exclude=[],
                file_types=["csv"],
                sample_files=False,
            )
        ],
        profiling={"enabled": True},
    )

    ctx = PipelineContext(run_id="test-run-id", pipeline_name="abs-source")
    ctx.graph = Mock()
    source = ABSSource(source_config, ctx)

    assert source.source_config.is_profiling_enabled()
    assert isinstance(source.profiler, FileProfiler)
    assert source.profiler.azure_config is azure_config


@patch("datahub.ingestion.source.azure.azure_common.BlobServiceClient")
def test_get_abs_tags_emits_blob_tags(mock_blob_service_client_class):
    """get_abs_tags maps blob tags to a GlobalTags aspect when use_abs_blob_tags=True.

    This is the only coverage of the use_abs_blob_tags / get_abs_tags path — the ABS
    integration test skips it because floci-az's get_blob_tags response can't be
    decoded by the Azure SDK.
    """
    mock_blob_client = Mock()
    mock_blob_client.get_blob_tags.return_value = {"env": "prod", "team": "data"}
    mock_container_client = Mock()
    mock_container_client.get_blob_client.return_value = mock_blob_client
    mock_service_client = Mock()
    mock_service_client.get_container_client.return_value = mock_container_client
    mock_blob_service_client_class.return_value = mock_service_client

    azure_config = AzureConnectionConfig(
        account_name="testaccount",
        container_name="testcontainer",
        account_key="test-account-key",
    )

    tags = get_abs_tags(
        container_name="testcontainer",
        key_name="folder/data.csv",
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:abs,testcontainer/folder/data.csv,PROD)",
        azure_config=azure_config,
        ctx=PipelineContext(run_id="test-abs-tags"),
        use_abs_blob_tags=True,
    )

    assert tags is not None
    assert {t.tag for t in tags.tags} == {
        "urn:li:tag:env:prod",
        "urn:li:tag:team:data",
    }
    mock_blob_client.get_blob_tags.assert_called_once()


def test_connection_string_account_matches_account_name():
    """A connection string whose AccountName matches account_name is accepted."""
    config = AzureConnectionConfig(
        account_name="myacct",
        container_name="c",
        connection_string=(
            "DefaultEndpointsProtocol=https;AccountName=myacct;"
            "AccountKey=c3VwZXJzZWNyZXQ=;EndpointSuffix=core.windows.net"
        ),
    )
    assert config.connection_string is not None


def test_connection_string_account_diverges_from_account_name():
    """A connection string whose AccountName differs from account_name is rejected.

    Reads would use the connection string's account while dataset URNs would name
    account_name — silent wrong lineage — so config parsing must fail fast.
    """
    with pytest.raises(ConfigurationError, match="does not match AccountName"):
        AzureConnectionConfig(
            account_name="prod",
            container_name="c",
            connection_string=(
                "DefaultEndpointsProtocol=https;AccountName=staging;"
                "AccountKey=c3VwZXJzZWNyZXQ=;EndpointSuffix=core.windows.net"
            ),
        )


def test_connection_string_without_account_name_is_accepted():
    """A connection string with no AccountName (e.g. SAS-based) can't diverge, so
    it must be accepted rather than false-rejected."""
    config = AzureConnectionConfig(
        account_name="myacct",
        container_name="c",
        connection_string=(
            "BlobEndpoint=https://myacct.blob.core.windows.net/;"
            "SharedAccessSignature=sv=2022-11-02&ss=b&sig=abc%3D%3D"
        ),
    )
    assert config.connection_string is not None


def test_connection_string_account_check_handles_base64_key_padding():
    """AccountKey base64 padding ('==') must not break parsing of AccountName."""
    key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    # Matching AccountName is accepted despite the '==' padding elsewhere.
    ok = AzureConnectionConfig(
        account_name="devstoreaccount1",
        container_name="c",
        connection_string=f"AccountName=devstoreaccount1;AccountKey={key};",
    )
    assert ok.connection_string is not None
    # And a mismatch is still caught (parsing wasn't derailed by the padding).
    with pytest.raises(ConfigurationError, match="does not match AccountName"):
        AzureConnectionConfig(
            account_name="other",
            container_name="c",
            connection_string=f"AccountName=devstoreaccount1;AccountKey={key};",
        )
