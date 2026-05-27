"""Idempotent setup script for the Kinesis integration test fixtures.

Creates 3 streams (events, clicks, _internal_audit), 2 S3 buckets,
2 Firehose delivery streams (events-to-s3, clicks-to-s3) in LocalStack.

Run via `python setup_test_data.py` after `docker-compose up` is healthy.
The script is intentionally minimal — the real impl will replace this with
something richer once Phase 5 starts.
"""

from __future__ import annotations

import json
import os
import time

import boto3
from botocore.exceptions import ClientError

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = "us-east-1"


def _client(service: str):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _create_stream(kinesis, name: str, shard_count: int = 1) -> None:
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


def _create_bucket(s3, name: str) -> None:
    try:
        s3.create_bucket(Bucket=name)
    except ClientError as e:
        if e.response["Error"]["Code"] not in (
            "BucketAlreadyOwnedByYou",
            "BucketAlreadyExists",
        ):
            raise


def main() -> None:
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

    print("Kinesis test data created successfully.")


if __name__ == "__main__":
    main()
