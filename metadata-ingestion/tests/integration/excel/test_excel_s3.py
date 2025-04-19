import logging
from datetime import datetime

import moto
import pytest
from boto3.session import Session
from moto import mock_s3

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2025-01-01 01:00:00"
BUCKET_NAME = "test-bucket"
S3_PREFIX = "data/test/"


@pytest.fixture(scope="module")
def s3_setup():
    with mock_s3():
        session = Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        s3_resource = session.resource("s3")
        s3_client = session.client("s3")

        s3_resource.create_bucket(Bucket=BUCKET_NAME)

        yield session, s3_resource, s3_client


@pytest.fixture(scope="module")
def s3_populate(pytestconfig, s3_setup):
    _, s3_resource, s3_client = s3_setup

    data_dir = pytestconfig.rootpath / "tests/integration/excel/data"
    logging.info(f"Loading Excel files from {data_dir} to S3 bucket: {BUCKET_NAME}")

    current_time = datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S").timestamp()

    for file_path in data_dir.glob("*.xlsx"):
        s3_key = f"{S3_PREFIX}{file_path.name}"

        s3_resource.Bucket(BUCKET_NAME).upload_file(str(file_path), s3_key)

        key = (
            moto.s3.models.s3_backends["123456789012"]["global"]
            .buckets[BUCKET_NAME]
            .keys[s3_key]
        )
        current_time += 10
        key.last_modified = datetime.fromtimestamp(current_time)

        logging.info(f"Uploaded {file_path.name} to s3://{BUCKET_NAME}/{s3_key}")

    return BUCKET_NAME


@pytest.mark.integration
def test_excel_s3(pytestconfig, s3_populate, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/excel"

    pipeline = Pipeline.create(
        {
            "run_id": "excel-s3-test",
            "source": {
                "type": "excel",
                "config": {
                    "path_list": [
                        f"s3://{BUCKET_NAME}/{S3_PREFIX}*.xlsx",
                    ],
                    "aws_config": {
                        "aws_access_key_id": "test",
                        "aws_secret_access_key": "test",
                        "aws_region": "us-east-1",
                    },
                    "profiling": {
                        "enabled": True,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/excel_s3_test.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "excel_s3_test.json",
        golden_path=test_resources_dir / "excel_s3_test_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
        ],
    )
