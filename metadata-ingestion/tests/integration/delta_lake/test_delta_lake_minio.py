import os
import subprocess

import boto3
import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.fixture(scope="module", autouse=True)
def minio_startup():
    cmd = "./tests/integration/delta_lake/minio/setup_minio.sh"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    assert ret.returncode == 0
    yield

    # Shutdown minio server
    cmd = "./tests/integration/delta_lake/minio/kill_minio.sh"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    assert ret.returncode == 0


@pytest.fixture(scope="module", autouse=True)
def bucket_name():
    return "my-test-bucket"


@pytest.fixture(scope="module", autouse=True)
def s3_bkt(bucket_name, minio_startup):
    s3 = boto3.resource(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    bkt = s3.Bucket(bucket_name)
    bkt.create()
    return bkt


@pytest.fixture(scope="module", autouse=True)
def populate_minio(pytestconfig, s3_bkt):
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/delta_lake/test_data/"
    )

    for root, dirs, files in os.walk(test_resources_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, test_resources_dir)
            s3_bkt.upload_file(full_path, rel_path)
    yield


@pytest.mark.slow_integration
@pytest.mark.integration
def test_delta_lake_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/delta_lake/"

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
                            "aws_access_key_id": "minioadmin",
                            "aws_secret_access_key": "minioadmin",
                            "aws_endpoint_url": "http://localhost:9000",
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
