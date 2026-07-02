import io
import zipfile
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.abs.config import DataLakeSourceConfig
from datahub.ingestion.source.abs.source import ABSSource, SeekableABSFile, TableData
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

_ABS_INCLUDE = "https://testaccount.blob.core.windows.net/testcontainer/*.*"
_ABS_ZIP_INCLUDE = "https://testaccount.blob.core.windows.net/testcontainer/*.zip"


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


def _make_service_client(content: bytes) -> Mock:
    def _download(offset: int = 0, length: int = -1) -> Mock:
        chunk = content[offset : offset + length] if length >= 0 else content[offset:]
        stream = Mock()
        stream.readall.return_value = chunk
        return stream

    blob_client = Mock()
    blob_client.get_blob_properties.return_value = {"size": len(content)}
    blob_client.download_blob.side_effect = _download
    service_client = Mock()
    service_client.get_blob_client.return_value = blob_client
    return service_client


def _make_abs_source() -> ABSSource:
    config = DataLakeSourceConfig(
        platform="abs",
        azure_config=AzureConnectionConfig(
            account_name="testaccount",
            container_name="testcontainer",
            account_key="dGVzdA==",
        ),
        path_specs=[PathSpec(include=_ABS_INCLUDE)],
    )
    ctx = PipelineContext(run_id="test-abs-zip")
    ctx.graph = Mock()
    return ABSSource(config, ctx)


def _table_data(full_path: str, rel_path: str = "data/file.zip") -> TableData:
    return TableData(
        display_name="test_table",
        is_abs=False,
        full_path=full_path,
        rel_path=rel_path,
        partitions=None,
        timestamp=datetime.now(),
        table_path=full_path,
        size_in_bytes=1024,
        number_of_files=1,
    )


def _make_zip(entries: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


class TestSeekableABSFile:
    def test_initial_state(self):
        content = b"abcde"
        f = SeekableABSFile(_make_service_client(content), "container", "blob/key")
        assert f._size == len(content)
        assert f.tell() == 0
        assert f.seekable()
        assert f.readable()

    def test_sequential_reads(self):
        f = SeekableABSFile(
            _make_service_client(b"Hello, World!"), "container", "blob/key"
        )
        assert f.read(5) == b"Hello"
        assert f.tell() == 5
        assert f.read(2) == b", "
        assert f.tell() == 7

    def test_read_all(self):
        content = b"complete content"
        f = SeekableABSFile(_make_service_client(content), "container", "blob/key")
        assert f.read() == content

    def test_read_past_eof_returns_empty(self):
        f = SeekableABSFile(_make_service_client(b"short"), "container", "blob/key")
        f.read()
        assert f.read(10) == b""

    def test_seek_set(self):
        f = SeekableABSFile(
            _make_service_client(b"0123456789"), "container", "blob/key"
        )
        f.seek(4)
        assert f.tell() == 4
        assert f.read(3) == b"456"

    def test_seek_from_current(self):
        f = SeekableABSFile(
            _make_service_client(b"0123456789"), "container", "blob/key"
        )
        f.read(3)
        f.seek(2, 1)
        assert f.tell() == 5

    def test_seek_from_end(self):
        f = SeekableABSFile(
            _make_service_client(b"0123456789"), "container", "blob/key"
        )
        f.seek(-3, 2)
        assert f.tell() == 7
        assert f.read() == b"789"

    def test_zipfile_round_trip(self):
        csv_content = b"name,age\nAlice,30\n"
        f = SeekableABSFile(
            _make_service_client(_make_zip({"data.csv": csv_content})),
            "container",
            "data.csv.zip",
        )
        with zipfile.ZipFile(f) as zf:
            assert zf.namelist() == ["data.csv"]
            assert zf.read("data.csv") == csv_content


class TestABSOpenZipEntry:
    def test_single_csv_entry(self, tmp_path):
        csv_bytes = b"name,age\nAlice,30\n"
        zip_path = tmp_path / "data.csv.zip"
        zip_path.write_bytes(_make_zip({"data.csv": csv_bytes}))

        source = _make_abs_source()
        file, ext = source._open_zip_entry(
            _table_data(str(zip_path)), None, PathSpec(include=_ABS_ZIP_INCLUDE)
        )

        assert file is not None
        assert ext == ".csv"
        assert file.read() == csv_bytes

    def test_no_supported_files_returns_none(self, tmp_path):
        zip_path = tmp_path / "data.zip"
        zip_path.write_bytes(_make_zip({"README.txt": b"nothing here"}))

        source = _make_abs_source()
        file, ext = source._open_zip_entry(
            _table_data(str(zip_path)), None, PathSpec(include=_ABS_ZIP_INCLUDE)
        )

        assert file is None
        assert source.report.warnings

    def test_bad_zip_returns_none(self, tmp_path):
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_bytes(b"not a zip")

        source = _make_abs_source()
        file, ext = source._open_zip_entry(
            _table_data(str(bad_zip)), None, PathSpec(include=_ABS_ZIP_INCLUDE)
        )

        assert file is None
        assert source.report.warnings


def test_abs_get_fields_csv_zip(tmp_path):
    zip_path = tmp_path / "products.csv.zip"
    zip_path.write_bytes(_make_zip({"products.csv": b"product,price\nwidget,9.99\n"}))

    source = _make_abs_source()
    # Returning None forces get_fields to use the local-file code path, which is
    # all we want to test here (the ABS network path is covered by SeekableABSFile tests).
    with patch(
        "datahub.ingestion.source.azure.azure_common.AzureConnectionConfig.get_blob_service_client",
        return_value=None,
    ):
        fields = source.get_fields(
            _table_data(str(zip_path)),
            PathSpec(
                include="https://testaccount.blob.core.windows.net/testcontainer/*.csv.zip",
                enable_compression=True,
            ),
        )

    field_names = {f.fieldPath for f in fields}
    assert {"product", "price"}.issubset(field_names)
