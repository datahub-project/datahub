import json
from pathlib import Path

import pytest
import time_machine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws_messaging.source import AwsMessagingSource
from datahub.testing import mce_helpers
from tests.unit.aws_messaging.conftest import (
    ACCOUNT,
    REGION,
    base_config,
    sns_client,
    sqs_client,
)

GOLDEN_DIR = Path(__file__).parent / "golden"
FROZEN_TIME = "2026-10-28 12:00:00"


def _seed() -> None:
    sqs = sqs_client()
    dlq = sqs.create_queue(QueueName="orders-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sqs.create_queue(
        QueueName="orders",
        Attributes={
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}
            ),
            "VisibilityTimeout": "60",
        },
        tags={"team": "payments", "pii": ""},
    )
    sqs.create_queue(
        QueueName="events.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )
    sqs.create_queue(QueueName="tmp-scratch")

    sns = sns_client()
    sns.create_topic(
        Name="order-events",
        Attributes={"DisplayName": "Orders"},
        Tags=[{"Key": "team", "Value": "payments"}],
    )
    sns.create_topic(Name="alerts")


def _run(config: dict) -> AwsMessagingSource:
    return AwsMessagingSource.create(
        config, PipelineContext(run_id="aws-messaging-test")
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_golden(aws: None, pytestconfig: pytest.Config, tmp_path: Path) -> None:
    _seed()
    source = _run(base_config(queue_pattern={"deny": ["tmp-.*"]}))
    workunits = [wu.metadata for wu in source.get_workunits()]
    output = tmp_path / "aws_messaging_mces.json"
    write_metadata_file(output, workunits)
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=GOLDEN_DIR / "aws_messaging_mces_golden.json",
    )
    report = source.get_report()
    assert report.account_id == ACCOUNT
    assert (report.queues_scanned, report.topics_scanned) == (3, 2)
    assert list(report.filtered_queues) == ["tmp-scratch"]


def test_platform_instance_defaults_to_account(aws: None) -> None:
    sqs_client().create_queue(QueueName="orders")
    source = _run(base_config(include_sns=False))
    urns = {wu.get_urn() for wu in source.get_workunits()}
    assert (
        f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.orders,PROD)"
        in urns
    )


def test_explicit_platform_instance_wins(aws: None) -> None:
    sns_client().create_topic(Name="alerts")
    source = _run(base_config(include_sqs=False, platform_instance="prod"))
    urns = {wu.get_urn() for wu in source.get_workunits()}
    assert f"urn:li:dataset:(urn:li:dataPlatform:sns,prod.{REGION}.alerts,PROD)" in urns


def test_tags_off_skips_tag_calls(aws: None) -> None:
    sqs_client().create_queue(QueueName="orders", tags={"team": "payments"})
    source = _run(base_config(include_sns=False, extract_tags=False))
    aspects = [
        wu.metadata.aspectName
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]
    assert "globalTags" not in aspects


def test_test_connection(aws: None) -> None:
    report = AwsMessagingSource.test_connection(base_config())
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
