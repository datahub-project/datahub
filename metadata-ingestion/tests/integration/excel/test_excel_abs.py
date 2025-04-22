import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2025-01-01 01:00:00"
CONTAINER_NAME = "test-abs"
ABS_PREFIX = "excel/test/"
ACCOUNT_NAME = "teststorageaccount"
SAS_TOKEN = "?sv=2020-08-04&ss=b&srt=co&sp=rwlacx&sig=fakeSignature123456789"


@pytest.fixture(scope="module")
def mock_azure_blob_setup():
    mock_blob_service_client = MagicMock()
    mock_container_client = MagicMock()
    mock_blob_client = MagicMock()

    mock_blob_service_client.get_container_client.return_value = mock_container_client
    mock_container_client.get_blob_client.return_value = mock_blob_client

    blob_list = []  # type: ignore

    return {
        "mock_blob_service_client": mock_blob_service_client,
        "mock_container_client": mock_container_client,
        "mock_blob_client": mock_blob_client,
        "blob_list": blob_list,
    }


@pytest.fixture(scope="module")
def abs_populate(pytestconfig, mock_azure_blob_setup):
    mock_container_client = mock_azure_blob_setup["mock_container_client"]
    mock_blob_client = mock_azure_blob_setup["mock_blob_client"]
    blob_list = mock_azure_blob_setup["blob_list"]

    data_dir = pytestconfig.rootpath / "tests/integration/excel/data"
    logging.info(
        f"Loading Excel files from {data_dir} to Azure Blob container: {CONTAINER_NAME}"
    )

    current_time = datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S")

    for file_path in data_dir.glob("*.xlsx"):
        blob_path = f"{ABS_PREFIX}{file_path.name}"

        with open(file_path, "rb") as f:
            file_content = f.read()

        download_mock = MagicMock()
        download_mock.readall.return_value = file_content

        mock_blob_client.download_blob.return_value = download_mock

        blob_props = MagicMock()
        blob_props.name = blob_path
        blob_props.last_modified = current_time
        blob_props.size = len(file_content)

        blob_list.append(blob_props)

        current_time = current_time.replace(second=current_time.second + 10)
        logging.info(
            f"Uploaded {file_path.name} to https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{blob_path}"
        )

    def mock_list_blobs_filter(name_starts_with=None, **kwargs):
        logging.info(f"list_blobs called with name_starts_with={name_starts_with}")
        if name_starts_with:
            return [
                blob for blob in blob_list if blob.name.startswith(name_starts_with)
            ]
        return blob_list

    mock_container_client.list_blobs.side_effect = mock_list_blobs_filter


@pytest.mark.integration
def test_excel_abs(
    pytestconfig, abs_populate, tmp_path, mock_time, mock_azure_blob_setup
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/excel"
    mock_blob_service_client = mock_azure_blob_setup["mock_blob_service_client"]

    with patch(
        "azure.storage.blob.BlobServiceClient.__init__", return_value=None
    ), patch(
        "azure.storage.blob.BlobServiceClient", return_value=mock_blob_service_client
    ), patch(
        "datahub.ingestion.source.azure.azure_common.AzureConnectionConfig.get_blob_service_client",
        return_value=mock_blob_service_client,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "excel-abs-test",
                "source": {
                    "type": "excel",
                    "config": {
                        "path_list": [
                            f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{ABS_PREFIX}*.xlsx",
                        ],
                        "azure_config": {
                            "account_name": ACCOUNT_NAME,
                            "sas_token": SAS_TOKEN,
                            "container_name": CONTAINER_NAME,
                        },
                        "profiling": {
                            "enabled": True,
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/excel_abs_test.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "excel_abs_test.json",
        golden_path=test_resources_dir / "excel_abs_test_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
        ],
    )
