import json
from typing import Any, Dict, Iterator, List, Set, Type
from unittest.mock import MagicMock

import boto3
from botocore.stub import Stubber

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.base import AwsMessagingSourceBase
from datahub.ingestion.source.aws_messaging.clients import S3NotificationClient
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)
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
# Named the way the s3 source names them, after a path_spec's {table} folder:
# s3://landing/incoming/{table}/*.parquet
ORDERS_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/incoming/orders,PROD)"
REFUNDS_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/incoming/refunds,PROD)"
ARCHIVE_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/archive/orders,PROD)"
OTHER_BUCKET_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing-old/orders,PROD)"
S3_DATASETS = [ORDERS_URN, REFUNDS_URN, ARCHIVE_URN, OTHER_BUCKET_URN]


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


def search_graph(existing: List[str]) -> MagicMock:
    """A graph whose URN-prefix search runs over `existing`."""
    graph = MagicMock()

    def get_urns_by_filter(**kwargs: Any) -> Iterator[str]:
        prefix = kwargs["extraFilters"][0]["values"][0]
        return iter(urn for urn in existing if urn.startswith(prefix))

    graph.get_urns_by_filter.side_effect = get_urns_by_filter
    graph.get_related_entities.return_value = []
    graph.get_aspect.return_value = None
    graph.get_schema_metadata.return_value = None
    return graph


def _source(
    source_class: Type[AwsMessagingSourceBase], existing: List[str], **lineage: Any
) -> AwsMessagingSourceBase:
    graph = search_graph(existing)
    return source_class.create(
        base_config(lineage={"lambda_functions": False, **lineage}),
        PipelineContext(run_id="s3-test", graph=graph),
    )


def test_each_source_links_its_own_targets(aws: None) -> None:
    _seed()
    # The queue's notification is on `incoming/`, so every table folder under
    # it is an upstream. The topic's has no prefix, so the whole bucket is, but
    # not a bucket whose name merely starts the same.
    assert upstreams(_source(SqsSource, S3_DATASETS, s3_notifications=True)) == {
        QUEUE_URN: {ORDERS_URN, REFUNDS_URN}
    }
    assert upstreams(_source(SnsSource, S3_DATASETS, s3_notifications=True)) == {
        TOPIC_URN: {ORDERS_URN, REFUNDS_URN, ARCHIVE_URN}
    }


def test_no_edge_without_a_matching_s3_dataset(aws: None) -> None:
    _seed()
    source = _source(SqsSource, [], s3_notifications=True)
    assert upstreams(source) == {}
    assert len(source.base_report.lineage_edges_skipped) == 1


def test_off_by_default(aws: None) -> None:
    _seed()
    source = _source(SqsSource, S3_DATASETS)
    assert upstreams(source) == {}
    assert not source.base_report.lineage_edges_skipped


def test_lowercase_matches_the_s3_source_setting(aws: None) -> None:
    url = sqs_client().create_queue(QueueName="uploads")["QueueUrl"]
    queue_arn = sqs_client().get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
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
                            "FilterRules": [{"Name": "prefix", "Value": "Incoming/"}]
                        }
                    },
                }
            ]
        },
        SkipDestinationValidation=True,
    )
    graph = search_graph(S3_DATASETS)

    def source(lowercase: bool) -> AwsMessagingSourceBase:
        return SqsSource.create(
            base_config(
                lineage={"lambda_functions": False, "s3_notifications": True},
                s3_convert_urns_to_lowercase=lowercase,
            ),
            PipelineContext(run_id="s3-test", graph=graph),
        )

    # The S3 source lowercased `landing/Incoming/...` to `landing/incoming/...`.
    assert upstreams(source(lowercase=True)) == {QUEUE_URN: {ORDERS_URN, REFUNDS_URN}}
    assert upstreams(source(lowercase=False)) == {}


def test_bucket_listing_is_filtered_to_the_recipe_region() -> None:
    client = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_buckets",
            {
                "Buckets": [{"Name": "landing", "BucketRegion": REGION}],
                "ContinuationToken": "page-2",
            },
            {"BucketRegion": REGION, "MaxBuckets": 1000},
        )
        stubber.add_response(
            "list_buckets",
            {"Buckets": [{"Name": "elsewhere", "BucketRegion": "eu-west-1"}]},
            {"BucketRegion": REGION, "MaxBuckets": 1000, "ContinuationToken": "page-2"},
        )
        names = list(
            S3NotificationClient(client, SqsSourceReport()).list_bucket_names(
                REGION, on_error=lambda _error: None
            )
        )
    assert names == ["landing"]


def test_old_botocore_lists_without_the_region_filter() -> None:
    # Before botocore 1.35.42, ListBuckets has no BucketRegion parameter; sending it
    # would fail validation and look like a denied listing.
    client = MagicMock()
    client.meta.service_model.operation_model.return_value.input_shape.members = {
        "MaxBuckets": None,
        "ContinuationToken": None,
    }
    client.get_paginator.return_value.paginate.return_value = [
        {"Buckets": [{"Name": "landing"}, {"Name": "elsewhere"}]}
    ]
    report = SqsSourceReport()
    names = list(
        S3NotificationClient(client, report).list_bucket_names(
            REGION, on_error=lambda _error: None
        )
    )
    assert names == ["landing", "elsewhere"]
    client.get_paginator.return_value.paginate.assert_called_once_with(
        PaginationConfig={"PageSize": 1000}
    )
    assert [w.title for w in report.warnings] == ["Bucket region filter unavailable"]
    assert not report.failures


def test_removed_notification_retracts_the_s3_edge(aws: None) -> None:
    sqs_client().create_queue(QueueName="uploads")
    boto3.client("s3", region_name=REGION).create_bucket(Bucket="landing")
    graph = search_graph(S3_DATASETS)
    graph.get_aspect.side_effect = lambda urn, aspect: (
        UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=ORDERS_URN,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    properties={"relationship": "s3_notification"},
                ),
                UpstreamClass(
                    dataset=TOPIC_URN,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    properties={"relationship": "subscription"},
                ),
            ]
        )
        if urn == QUEUE_URN
        else None
    )
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False, "s3_notifications": True}),
        PipelineContext(run_id="s3-test", graph=graph),
    )
    ops = [
        (op["op"], op["path"])
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "upstreamLineage"
        and wu.metadata.aspect is not None
        for op in json.loads(wu.metadata.aspect.value)
    ]
    # The sns source's subscription edge is left alone.
    assert ops == [("remove", "/upstreams/" + ORDERS_URN.replace("/", "~1"))]
