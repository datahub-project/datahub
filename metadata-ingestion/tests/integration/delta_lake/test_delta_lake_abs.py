import logging
import os
import subprocess

import freezegun
import pytest
from azure.storage.blob import BlobServiceClient

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2020-04-14 07:00:00"
AZURITE_BLOB_PORT = 10000


def is_azurite_up(container_name: str) -> bool:
    """Check if Azurite blob storage is responsive on a container"""
    cmd = f"docker logs {container_name} 2>&1 | grep 'Azurite Blob service is successfully listening'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/delta_lake"


@pytest.fixture(scope="module")
def azurite_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    container_name = "azurite_test"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", container_name
    ) as docker_services:
        wait_for_port(
            docker_services,
            container_name,
            AZURITE_BLOB_PORT,
            timeout=120,
            checker=lambda: is_azurite_up(container_name),
        )
        yield docker_services


@pytest.fixture(scope="module", autouse=True)
def azure_container(azurite_runner):
    connection_string = (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://localhost:{AZURITE_BLOB_PORT}/devstoreaccount1"
    )

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "test-container"
    container_client = blob_service_client.create_container(container_name)
    return container_client


@pytest.fixture(scope="module", autouse=True)
def populate_azure_storage(pytestconfig, azure_container):
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/delta_lake/test_data/"
    )

    logger.info(f"Files in test_resources_dir {test_resources_dir}:")
    for root, _dirs, files in os.walk(test_resources_dir):
        for file in files:
            logger.info(f"Found file: {os.path.join(root, file)}")

    for root, _dirs, files in os.walk(test_resources_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, test_resources_dir)
            try:
                with open(full_path, "rb") as data:
                    azure_container.upload_blob(
                        name=rel_path, data=data, overwrite=True
                    )
                logger.info(f"Uploaded {rel_path}")
            except Exception as e:
                logger.error(f"Failed to upload {rel_path}: {e}")

    blobs = list(azure_container.list_blobs())
    logger.info("Uploaded blobs:")
    for blob in blobs:
        logger.info(f"  {blob.name}")

    yield


@freezegun.freeze_time("2023-01-01 00:00:00+00:00")
def test_delta_lake_ingest_azure(pytestconfig, tmp_path, test_resources_dir):
    pipeline = Pipeline.create(
        {
            "run_id": "delta-lake-azure-test",
            "source": {
                "type": "delta-lake",
                "config": {
                    "env": "DEV",
                    "base_path": f"http://localhost:{AZURITE_BLOB_PORT}/devstoreaccount1/test-container/delta_tables/sales",
                    "azure": {
                        "azure_config": {
                            "account_name": "devstoreaccount1",
                            "container_name": "test-container",
                            "account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/delta_lake_azure_mces.json",
                },
            },
        }
    )

    logger.info(
        f"Starting pipeline run with base_path: {pipeline.config['source']['config']['base_path']}"
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "delta_lake_azure_mces.json",
        golden_path=test_resources_dir / "delta_lake_azure_mces_golden.json",
    )
