"""Integration test for the GCS (Google Cloud Storage) source.

The GCS connector reaches storage through boto3's S3 client against GCS's
S3-compatible XML endpoint (using HMAC keys), so it is exercised here against an
S3-compatible mock: ``moto``. This mirrors how the S3 source is tested, but drives
``GCSSource`` — the recipe ``type`` is ``gcs``, ``s3://`` path specs are rewritten
to ``gs://``, AWS keys are mapped to HMAC credentials, and the output is real
``gcs``-platform metadata compared against ``golden-files/gcs``.

Why moto and not the Floci ``floci-gcp`` emulator: ``floci-gcp`` only implements
the native GCS JSON API, not the S3-compatible XML API the connector requires, so
it cannot back this connector. moto stands in for GCS's S3-compatible endpoint.
"""

import json
import logging
import os
from datetime import datetime
from unittest.mock import patch

import moto.s3
import pytest
from boto3.session import Session
from moto import mock_aws

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"

SHARED_SOURCE_FILES_PATH = "./tests/integration/s3/sources/shared"
S3_SOURCE_FILES_PATH = "./tests/integration/s3/sources/s3"
shared_source_files = [
    (SHARED_SOURCE_FILES_PATH, p) for p in os.listdir(SHARED_SOURCE_FILES_PATH)
]
s3_source_files = [(S3_SOURCE_FILES_PATH, p) for p in os.listdir(S3_SOURCE_FILES_PATH)]


def get_descriptive_id(source_tuple):
    source_dir, source_file = source_tuple
    return f"{os.path.basename(source_dir)}_{source_file.replace('.json', '')}"


@pytest.fixture(scope="module", autouse=True)
def bucket_names():
    return ["my-test-bucket", "my-test-bucket-2"]


@pytest.fixture(scope="module", autouse=True)
def s3():
    with mock_aws():
        yield Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )


@pytest.fixture(scope="module", autouse=True)
def s3_resource(s3):
    with mock_aws():
        yield s3.resource("s3")


@pytest.fixture(scope="module", autouse=True)
def s3_client(s3):
    with mock_aws():
        yield s3.client("s3")


@pytest.fixture(scope="module", autouse=True)
def s3_populate(pytestconfig, s3_resource, s3_client, bucket_names):
    # Seed the same local_system tree the S3 suite uses, with deterministic
    # per-object last_modified (poked into moto's backend) so partition min/max
    # selection is stable.
    data_dir = pytestconfig.rootpath / "tests/integration/s3/test_data/local_system/"
    for bucket_name in bucket_names:
        logging.info(f"Populating s3 bucket: {bucket_name}")
        s3_resource.create_bucket(Bucket=bucket_name)
        bkt = s3_resource.Bucket(bucket_name)
        bkt.Tagging().put(Tagging={"TagSet": [{"Key": "foo", "Value": "bar"}]})
        current_time_sec = datetime.strptime(
            FROZEN_TIME, "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        for root, dirs, files in os.walk(data_dir):
            dirs.sort()
            for file in sorted(files):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, data_dir)
                bkt.upload_file(
                    full_path,
                    rel_path,
                    ExtraArgs=(
                        {"ContentType": "text/csv"} if "." not in rel_path else {}
                    ),
                )
                s3_client.put_object_tagging(
                    Bucket=bucket_name,
                    Key=rel_path,
                    Tagging={"TagSet": [{"Key": "baz", "Value": "bob"}]},
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
@pytest.mark.parametrize(
    "source_file_tuple", shared_source_files + s3_source_files, ids=get_descriptive_id
)
def test_data_lake_gcs_ingest(
    pytestconfig, s3_populate, source_file_tuple, tmp_path, mock_time
):
    """Ingest each shared object-store recipe as GCS and compare to the golden.

    Rewrites the S3 recipe to the GCS connector (gs:// paths, HMAC creds) and runs
    it against the S3-compatible moto mock, asserting the emitted gcs-platform
    metadata matches ``golden-files/gcs``.
    """
    source_dir, source_file = source_file_tuple
    test_resources_dir = pytestconfig.rootpath / "tests/integration/gcs/"

    with open(os.path.join(source_dir, source_file)) as f:
        source = json.load(f)

    source["type"] = "gcs"
    source["config"]["credential"] = {
        "hmac_access_id": source["config"]["aws_config"]["aws_access_key_id"],
        "hmac_access_secret": source["config"]["aws_config"]["aws_secret_access_key"],
    }
    for path_spec in source["config"]["path_specs"]:
        path_spec["include"] = path_spec["include"].replace("s3://", "gs://")
    source["config"].pop("aws_config")
    source["config"].pop("profiling", None)
    source["config"].pop("sort_schema_fields", None)
    source["config"].pop("use_s3_bucket_tags", None)
    source["config"].pop("use_s3_content_type", None)
    source["config"].pop("use_s3_object_tags", None)

    config_dict = {
        "run_id": source_file,
        "source": source,
        "sink": {"type": "file", "config": {"filename": f"{tmp_path}/{source_file}"}},
    }

    # None routes boto3 at moto's default S3 endpoint instead of GCS's real one.
    with patch("datahub.ingestion.source.gcs.gcs_source.GCS_ENDPOINT_URL", None):
        pipeline = Pipeline.create(config_dict)
        pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{source_file}",
        golden_path=f"{test_resources_dir}/golden-files/gcs/golden_mces_{source_file}",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
        ],
    )
