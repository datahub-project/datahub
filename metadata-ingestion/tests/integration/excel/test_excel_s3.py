import logging
import os
from datetime import datetime

import moto
import pytest
from boto3.session import Session
from moto import mock_s3

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2025-01-01 01:00:00"


@pytest.fixture(scope="module", autouse=True)
def s3():
    with mock_s3():
        conn = Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        yield conn


@pytest.fixture(scope="module", autouse=True)
def s3_resource(s3):
    with mock_s3():
        conn = s3.resource("s3")
        yield conn


@pytest.fixture(scope="module", autouse=True)
def s3_client(s3):
    with mock_s3():
        conn = s3.client("s3")
        yield conn


@pytest.fixture(scope="module", autouse=True)
def s3_populate(pytestconfig, s3_resource, s3_client):
    bucket_name = "test-bucket"
    logging.info(f"Populating s3 bucket: {bucket_name}")
    s3_resource.create_bucket(Bucket=bucket_name)
    bkt = s3_resource.Bucket(bucket_name)
    bkt.Tagging().put(Tagging={"TagSet": [{"Key": "foo", "Value": "bar"}]})
    test_resources_dir = pytestconfig.rootpath / "tests/integration/excel/data/"

    current_time_sec = datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S").timestamp()
    file_list = []
    for root, _dirs, files in os.walk(test_resources_dir):
        _dirs.sort()
        for file in sorted(files):
            full_path = os.path.join(root, file)
            basename = os.path.basename(full_path)
            rel_path = "data/test/" + basename
            file_list.append(rel_path)
            bkt.upload_file(
                str(full_path),
                rel_path,
                ExtraArgs=({"ContentType": "text/csv"} if "." not in rel_path else {}),
            )
            s3_client.put_object_tagging(
                Bucket=bucket_name,
                Key=rel_path,
                Tagging={"TagSet": [{"Key": "test", "Value": "data"}]},
            )
            key = (
                moto.s3.models.s3_backends["123456789012"]["global"]
                .buckets[bucket_name]
                .keys[rel_path]
            )
            current_time_sec += 10
            key.last_modified = datetime.fromtimestamp(current_time_sec)

    yield


@pytest.mark.integration
def test_excel_s3(pytestconfig, s3_populate, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/excel"
    test_file = "business_report.xlsx"

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "excel-test",
            "source": {
                "type": "excel",
                "config": {
                    "path_list": [
                        "s3://test-bucket/data/test/" + test_file,
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
