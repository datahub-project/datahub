"""Integration test for the AWS Kinesis source.

Runs against the Floci AWS emulator (``floci/floci``) via the standard
``docker_compose_runner`` fixture. Test data is seeded in-process with boto3
(see ``_seed_test_data``) and a ``Pipeline`` runs the recipe in-process; the full
output is asserted against a golden file (``kinesis_mces_golden.json``, regenerate
with ``--update-golden-files``) and the emitted warnings are asserted explicitly.
"""

import json
import time
from typing import Any, Literal

import boto3
import pytest
from botocore.exceptions import ClientError

from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

ENDPOINT_URL = "http://localhost:14572"
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


def _create_stream(
    kinesis: Any, name: str, shard_count: int = 1, on_demand: bool = False
) -> None:
    try:
        if on_demand:
            kinesis.create_stream(
                StreamName=name, StreamModeDetails={"StreamMode": "ON_DEMAND"}
            )
        else:
            kinesis.create_stream(StreamName=name, ShardCount=shard_count)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    for _ in range(20):
        desc = kinesis.describe_stream(StreamName=name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.5)
    raise TimeoutError(f"Kinesis stream {name!r} did not become ACTIVE in time")


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
    streams the golden file expects. Idempotent so a left-running Floci emulator
    can be reused across runs."""
    kinesis = _client("kinesis")
    s3 = _client("s3")
    firehose = _client("firehose")
    iam = _client("iam")

    _create_stream(kinesis, "events", shard_count=2)
    _create_stream(kinesis, "clicks", shard_count=1)
    _create_stream(kinesis, "_internal_audit", shard_count=1)
    # On-demand capacity mode (vs the provisioned streams above) — exercises the
    # StreamModeDetails path that the LocalStack-era golden never covered.
    _create_stream(kinesis, "telemetry-ondemand", on_demand=True)

    # Tagging a freshly-created stream should always succeed; let a real failure
    # surface rather than silently producing a golden that's missing tags.
    kinesis.add_tags_to_stream(
        StreamName="events", Tags={"owner": "data-team", "env": "prod"}
    )
    kinesis.add_tags_to_stream(StreamName="clicks", Tags={"owner": "analytics"})

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


# The Floci Firehose emulator returns a null ``Source`` for
# ``KinesisStreamAsSource`` delivery streams, so the connector can't resolve the
# upstream Kinesis→Firehose lineage edge and emits exactly this warning (the edge
# is consequently absent from the golden; it stays covered by the unit tests in
# ``tests/unit/kinesis/test_kinesis_firehose*``). We assert this is the *only*
# warning rather than dropping warning-checking wholesale, so any new/unexpected
# warning still fails the test.
#
# Required Floci fix (tested against floci/floci:1.5.33): CreateDeliveryStream
# accepts a ``KinesisStreamSourceConfiguration`` but DescribeDeliveryStream drops
# it — ``Source`` comes back null instead of echoing
# ``Source.KinesisStreamSourceDescription.KinesisStreamARN``. Tracked upstream at
# https://github.com/floci-io/floci/issues/2016. Once Floci persists and returns
# that ARN, drop the waiver below and assert the golden carries the
# Kinesis→Firehose upstream edge. The lost golden coverage is recovered locally by
# ``test_source_stream_urn_matches_stream_extractor_urn`` in
# ``tests/unit/kinesis/test_kinesis_firehose.py``.
_EXPECTED_WARNING_TITLE = "Unresolved Firehose source stream"


@pytest.mark.integration
def test_kinesis_ingestion_golden_file(docker_compose_runner, pytestconfig, tmp_path):
    """Run the recipe against the Floci emulator and compare MCPs to the golden.

    Firehose→S3 destination lineage, streams, tags, region containers, stream
    filtering, and provisioned/on-demand stream modes are all exercised
    end-to-end. The only tolerated warning is the documented Firehose
    null-``Source`` gap (see above); any other warning fails the test.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kinesis"
    # `up -d --wait` blocks on the compose healthcheck, so the emulator is ready
    # once the context manager returns — no test-layer readiness polling needed.
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "kinesis",
        setup_command=["up -d --wait"],
    ):
        _seed_test_data()

        output = tmp_path / "kinesis_mces.json"
        config = load_config_file(test_resources_dir / "recipe_file_sink.yml")
        config["sink"] = {"type": "file", "config": {"filename": str(output)}}
        pipeline = Pipeline.create(config)
        pipeline.run()
        pipeline.raise_from_status()  # fails on failures, but not on warnings

        # Warning-checking without a blanket --strict-warnings: tolerate only the
        # documented Firehose null-Source gap; any other warning fails the test.
        warning_titles = {w.title for w in pipeline.source.get_report().warnings}
        assert warning_titles == {_EXPECTED_WARNING_TITLE}, (
            f"unexpected ingestion warnings: {warning_titles}"
        )

        mce_helpers.check_golden_file(
            pytestconfig=pytestconfig,
            output_path=output,
            golden_path=test_resources_dir / "kinesis_mces_golden.json",
        )
