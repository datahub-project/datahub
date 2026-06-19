"""Integration test for the AWS Kinesis source.

Runs against LocalStack 3.0 (Community) via the standard ``docker_compose_runner``
fixture. Test data is seeded in-process with boto3 (see ``_seed_test_data``) and
the ingestion runs in-process via ``run_datahub_cmd``; the full output is
asserted against a golden file (``kinesis_mces_golden.json``). Regenerate the
golden with ``--update-golden-files``.
"""

import json
import time
from typing import Any, Literal

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

from datahub.testing import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"

# Restricted to the services this module uses; boto3's typed overloads resolve
# via the Literal so we don't fall through to the generic Any fallback.
ServiceName = Literal["kinesis", "s3", "firehose", "iam"]


def _client(service: ServiceName) -> Any:
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _wait_for_kinesis_ready(timeout_s: int = 60) -> None:
    """Poll the LocalStack health endpoint until the kinesis service is up.

    ``wait_for_port`` only confirms 4566 is listening; LocalStack initializes
    individual services lazily, so we additionally wait for kinesis to report
    available/running before seeding."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            body = requests.get(f"{ENDPOINT_URL}/_localstack/health", timeout=2).text
            if '"kinesis": "available"' in body or '"kinesis": "running"' in body:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise TimeoutError("LocalStack kinesis service did not become ready in time")


def _create_stream(kinesis: Any, name: str, shard_count: int = 1) -> None:
    try:
        kinesis.create_stream(StreamName=name, ShardCount=shard_count)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    for _ in range(20):
        desc = kinesis.describe_stream(StreamName=name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.5)


def _create_bucket(s3: Any, name: str) -> None:
    try:
        s3.create_bucket(Bucket=name)
    except ClientError as e:
        if e.response["Error"]["Code"] not in (
            "BucketAlreadyOwnedByYou",
            "BucketAlreadyExists",
        ):
            raise


def _seed_test_data() -> None:
    """Idempotently create the streams, buckets, IAM role, and Firehose delivery
    streams the golden file expects. Idempotent so a left-running LocalStack can
    be reused across runs."""
    kinesis = _client("kinesis")
    s3 = _client("s3")
    firehose = _client("firehose")
    iam = _client("iam")

    _create_stream(kinesis, "events", shard_count=2)
    _create_stream(kinesis, "clicks", shard_count=1)
    _create_stream(kinesis, "_internal_audit", shard_count=1)

    try:
        kinesis.add_tags_to_stream(
            StreamName="events", Tags={"owner": "data-team", "env": "prod"}
        )
        kinesis.add_tags_to_stream(StreamName="clicks", Tags={"owner": "analytics"})
    except ClientError:
        pass

    _create_bucket(s3, "analytics-lake")
    _create_bucket(s3, "audit-archive")

    role_arn = "arn:aws:iam::000000000000:role/firehose-test-role"
    try:
        iam.create_role(
            RoleName="firehose-test-role",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "firehose.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise

    events_arn = kinesis.describe_stream(StreamName="events")["StreamDescription"][
        "StreamARN"
    ]
    clicks_arn = kinesis.describe_stream(StreamName="clicks")["StreamDescription"][
        "StreamARN"
    ]

    for ds_name, source_arn, prefix in [
        ("events-to-s3", events_arn, "events/"),
        ("clicks-to-s3", clicks_arn, "clicks/"),
    ]:
        try:
            firehose.create_delivery_stream(
                DeliveryStreamName=ds_name,
                DeliveryStreamType="KinesisStreamAsSource",
                KinesisStreamSourceConfiguration={
                    "KinesisStreamARN": source_arn,
                    "RoleARN": role_arn,
                },
                S3DestinationConfiguration={
                    "RoleARN": role_arn,
                    "BucketARN": "arn:aws:s3:::analytics-lake",
                    "Prefix": prefix,
                    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
                    "CompressionFormat": "GZIP",
                },
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceInUseException":
                raise


@pytest.mark.integration
def test_kinesis_ingestion_golden_file(docker_compose_runner, pytestconfig, tmp_path):
    """Run the recipe against LocalStack and compare emitted MCPs to the golden file."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kinesis"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kinesis"
    ) as docker_services:
        wait_for_port(docker_services, "localstack", 4566, timeout=120)
        _wait_for_kinesis_ready()
        _seed_test_data()

        # The file sink writes a relative path; run_datahub_cmd executes in an
        # isolated filesystem rooted at tmp_path, so the output lands there.
        config_file = (test_resources_dir / "recipe_file_sink.yml").resolve()
        run_datahub_cmd(
            ["ingest", "-c", str(config_file), "--strict-warnings"],
            tmp_path=tmp_path,
        )

        mce_helpers.check_golden_file(
            pytestconfig=pytestconfig,
            output_path=tmp_path / "kinesis_mces.json",
            golden_path=test_resources_dir / "kinesis_mces_golden.json",
        )
