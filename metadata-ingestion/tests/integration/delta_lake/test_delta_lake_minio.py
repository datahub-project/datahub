import os
import subprocess

import boto3
import freezegun
import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2020-04-14 07:00:00"
MINIO_PORT = 9000


def is_minio_up(container_name: str) -> bool:
    """A cheap way to figure out if postgres is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep '1 Online'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/delta_lake"


@pytest.fixture(scope="module")
def minio_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    container_name = "minio_test"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", container_name
    ) as docker_services:
        wait_for_port(
            docker_services,
            container_name,
            MINIO_PORT,
            timeout=120,
            checker=lambda: is_minio_up(container_name),
        )
        yield docker_services


@pytest.fixture(scope="module", autouse=True)
def s3_bkt(minio_runner):
    s3 = boto3.resource(
        "s3",
        endpoint_url=f"http://localhost:{MINIO_PORT}",
        aws_access_key_id="miniouser",
        aws_secret_access_key="miniopassword",
    )
    bkt = s3.Bucket("my-test-bucket")
    bkt.create()
    return bkt


@pytest.fixture(scope="module", autouse=True)
def populate_minio(pytestconfig, s3_bkt):
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/delta_lake/test_data/"
    )

    for root, _dirs, files in os.walk(test_resources_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, test_resources_dir)
            s3_bkt.upload_file(full_path, rel_path)
    yield


@freezegun.freeze_time("2023-01-01 00:00:00+00:00")
def test_delta_lake_ingest(pytestconfig, tmp_path, test_resources_dir):
    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "delta-lake-test",
            "source": {
                "type": "delta-lake",
                "config": {
                    "env": "DEV",
                    "base_path": "s3://my-test-bucket/delta_tables/sales",
                    "s3": {
                        "aws_config": {
                            "aws_access_key_id": "miniouser",
                            "aws_secret_access_key": "miniopassword",
                            "aws_endpoint_url": f"http://localhost:{MINIO_PORT}",
                            "aws_region": "us-east-1",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/delta_lake_minio_mces.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "delta_lake_minio_mces.json",
        golden_path=test_resources_dir / "delta_lake_minio_mces_golden.json",
    )
