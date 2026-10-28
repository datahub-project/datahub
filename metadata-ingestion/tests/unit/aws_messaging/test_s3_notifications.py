import json
from typing import Any, Dict, List, Set, Type
from unittest.mock import MagicMock

import boto3

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.base import AwsMessagingSourceBase
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from tests.unit.aws_messaging.conftest import (
    ACCOUNT,
    REGION,
    base_config,
    sns_client,
    sqs_client,
)

QUEUE_URN = f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.uploads,PROD)"
TOPIC_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:sns,{ACCOUNT}.{REGION}.uploads-topic,PROD)"
)
PREFIX_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/incoming,PROD)"
BUCKET_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing,PROD)"


def _seed() -> None:
    url = sqs_client().create_queue(QueueName="uploads")["QueueUrl"]
    queue_arn = sqs_client().get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    topic_arn = sns_client().create_topic(Name="uploads-topic")["TopicArn"]
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="landing")
    s3.put_bucket_notification_configuration(
        Bucket="landing",
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [{"Name": "prefix", "Value": "incoming/"}]
                        }
                    },
                }
            ],
            "TopicConfigurations": [
                {"TopicArn": topic_arn, "Events": ["s3:ObjectRemoved:*"]}
            ],
        },
        SkipDestinationValidation=True,
    )


def upstreams(source: AwsMessagingSourceBase) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    for workunit in source.get_workunits():
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "upstreamLineage"
        ):
            assert metadata.entityUrn is not None and metadata.aspect is not None
            result[metadata.entityUrn] = {
                op["value"]["dataset"] for op in json.loads(metadata.aspect.value)
            }
    return result


def _source(
    source_class: Type[AwsMessagingSourceBase], existing: List[str], **lineage: Any
) -> AwsMessagingSourceBase:
    graph = MagicMock()
    graph.exists.side_effect = lambda urn: urn in existing
    graph.get_schema_metadata.return_value = None
    return source_class.create(
        base_config(lineage={"lambda_functions": False, **lineage}),
        PipelineContext(run_id="s3-test", graph=graph),
    )


def test_each_source_links_its_own_targets(aws: None) -> None:
    _seed()
    existing = [PREFIX_URN, BUCKET_URN]
    # The queue's notification has a prefix, so the prefix dataset wins.
    assert upstreams(_source(SqsSource, existing, s3_notifications=True)) == {
        QUEUE_URN: {PREFIX_URN}
    }
    assert upstreams(_source(SnsSource, existing, s3_notifications=True)) == {
        TOPIC_URN: {BUCKET_URN}
    }


def test_no_edge_without_a_matching_s3_dataset(aws: None) -> None:
    _seed()
    source = _source(SqsSource, [], s3_notifications=True)
    assert upstreams(source) == {}
    assert len(source.base_report.lineage_edges_skipped) == 1


def test_off_by_default(aws: None) -> None:
    _seed()
    source = _source(SqsSource, [PREFIX_URN, BUCKET_URN])
    assert upstreams(source) == {}
    assert not source.base_report.lineage_edges_skipped
