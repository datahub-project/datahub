import io
import json
import zipfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import boto3
import pytest
import time_machine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import SchemaMetadataClass
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
BUCKET_NAME = "orders-bucket"
DLQ_NAME = "orders-dlq"
FUNCTION_NAME = "orders-worker"
PIPE_NAME = "orders-to-topic"
QUEUE_NAME = "orders"
RULE_NAME = "new-orders"
SCHEMA_NAME = "order-event"
STREAM_NAME = "orders-archive"
TOPIC_NAME = "order-events"

ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "double"},
    ],
}


def _queue_arn(name: str) -> str:
    url = sqs_client().create_queue(QueueName=name)["QueueUrl"]
    return sqs_client().get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]


def _create_function() -> str:
    role = boto3.client("iam", region_name=REGION).create_role(
        RoleName=f"{FUNCTION_NAME}-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )["Role"]["Arn"]
    code = io.BytesIO()
    with zipfile.ZipFile(code, "w") as archive:
        archive.writestr(
            "handler.py", "def handler(event, context):\n    return {'ok': True}\n"
        )
    return boto3.client("lambda", region_name=REGION).create_function(
        FunctionName=FUNCTION_NAME,
        Runtime="python3.12",
        Role=role,
        Handler="handler.handler",
        Code={"ZipFile": code.getvalue()},
    )["FunctionArn"]


def _seed() -> Dict[str, str]:
    dlq_arn = _queue_arn(DLQ_NAME)
    queue_arn = _queue_arn(QUEUE_NAME)
    sqs = sqs_client()
    sqs.set_queue_attributes(
        QueueUrl=sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"],
        Attributes={
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}
            )
        },
    )
    sqs.tag_queue(
        QueueUrl=sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"],
        Tags={"Project": "connector-eval", "Env": "sandbox"},
    )
    sqs.tag_queue(
        QueueUrl=sqs.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"],
        Tags={"Project": "connector-eval", "Env": "sandbox"},
    )

    sns = sns_client()
    topic_arn = sns.create_topic(
        Name=TOPIC_NAME,
        Tags=[
            {"Key": "Project", "Value": "connector-eval"},
            {"Key": "Env", "Value": "sandbox"},
        ],
    )["TopicArn"]
    subscription = sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "true"},
    )["SubscriptionArn"]
    sns.set_subscription_attributes(
        SubscriptionArn=subscription,
        AttributeName="RedrivePolicy",
        AttributeValue=json.dumps({"deadLetterTargetArn": dlq_arn}),
    )

    function_arn = _create_function()
    sns.subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=function_arn)
    lambdas = boto3.client("lambda", region_name=REGION)
    lambdas.create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=FUNCTION_NAME
    )
    lambdas.put_function_event_invoke_config(
        FunctionName=FUNCTION_NAME,
        DestinationConfig={"OnFailure": {"Destination": dlq_arn}},
    )

    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name=RULE_NAME, EventPattern=json.dumps({"source": ["shop"]}))
    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{"Id": "queue", "Arn": queue_arn}, {"Id": "topic", "Arn": topic_arn}],
    )
    boto3.client("pipes", region_name=REGION).create_pipe(
        Name=PIPE_NAME,
        Source=queue_arn,
        Target=topic_arn,
        RoleArn=f"arn:aws:iam::{ACCOUNT}:role/pipes",
    )

    boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET_NAME)
    boto3.client("s3", region_name=REGION).put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration={
            "QueueConfigurations": [
                {"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}
            ]
        },
        SkipDestinationValidation=True,
    )

    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="firehose",
        Endpoint=f"arn:aws:firehose:{REGION}:{ACCOUNT}:deliverystream/{STREAM_NAME}",
        Attributes={"SubscriptionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/sns-firehose"},
    )

    glue = boto3.client("glue", region_name=REGION)
    glue.create_registry(RegistryName="default-registry")
    glue.create_schema(
        RegistryId={"RegistryName": "default-registry"},
        SchemaName=SCHEMA_NAME,
        DataFormat="AVRO",
        Compatibility="NONE",
        SchemaDefinition=json.dumps(ORDER_SCHEMA),
    )
    return {
        # A table folder the s3 source would emit for s3://bucket/{table}/*.
        "bucket": f"urn:li:dataset:(urn:li:dataPlatform:s3,{BUCKET_NAME}/orders,PROD)",
        "firehose": (
            "urn:li:dataJob:(urn:li:dataFlow:(kinesis-firehose,"
            f"{ACCOUNT}.{REGION}.{STREAM_NAME},PROD),delivery)"
        ),
    }


def _graph(
    existing: Dict[str, str], schemas: Dict[str, SchemaMetadataClass]
) -> MagicMock:
    graph = MagicMock()
    graph.exists.side_effect = lambda urn: urn in existing.values()
    graph.get_urns_by_filter.side_effect = lambda **kwargs: iter(
        urn
        for urn in existing.values()
        if urn.startswith(kwargs["extraFilters"][0]["values"][0])
    )
    graph.get_related_entities.return_value = []
    graph.get_aspect.return_value = None
    graph.get_schema_metadata.side_effect = schemas.get
    return graph


def _queue_schemas(existing: Dict[str, str]) -> Dict[str, SchemaMetadataClass]:
    source = SqsSource.create(
        _sqs_config(),
        PipelineContext(run_id="schema-hydration", graph=_graph(existing, {})),
    )
    return {
        workunit.get_urn(): workunit.metadata.aspect
        for workunit in source.get_workunits()
        if isinstance(workunit.metadata, MetadataChangeProposalWrapper)
        and isinstance(workunit.metadata.aspect, SchemaMetadataClass)
    }


def _sqs_config() -> Dict[str, Any]:
    return base_config(
        glue_schema_registry={
            "enabled": True,
            "queue_schema_map": {QUEUE_NAME: SCHEMA_NAME, DLQ_NAME: SCHEMA_NAME},
        },
        lineage={"s3_notifications": True, "lambda_async_destinations": True},
    )


def _sns_config() -> Dict[str, Any]:
    return base_config(
        firehose_lineage=True,
        glue_schema_registry={
            "enabled": True,
            "topic_schema_map": {TOPIC_NAME: SCHEMA_NAME},
        },
        include_subscription_attributes=True,
        lineage={"s3_notifications": True, "lambda_async_destinations": True},
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sqs_end_to_end_golden(
    aws: None, pytestconfig: pytest.Config, tmp_path: Path
) -> None:
    existing = _seed()
    source = SqsSource.create(
        _sqs_config(),
        PipelineContext(run_id="sqs-end-to-end", graph=_graph(existing, {})),
    )
    output = tmp_path / "sqs_end_to_end_mces.json"
    write_metadata_file(output, [wu.metadata for wu in source.get_workunits()])
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=GOLDEN_DIR / "sqs_end_to_end_mces_golden.json",
    )
    report = source.get_report()
    assert report.queues_scanned == 2
    assert report.schemas_attached == 2
    assert report.lineage_edges_emitted == 2
    assert report.column_lineage_edges_emitted == 3
    assert report.lambda_functions_emitted == 1
    assert report.eventbridge_rules_emitted == 1
    assert report.eventbridge_pipes_emitted == 1
    assert not report.failures
    assert not report.warnings


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sns_end_to_end_golden(
    aws: None, pytestconfig: pytest.Config, tmp_path: Path
) -> None:
    existing = _seed()
    schemas = _queue_schemas(existing)
    source = SnsSource.create(
        _sns_config(),
        PipelineContext(run_id="sns-end-to-end", graph=_graph(existing, schemas)),
    )
    output = tmp_path / "sns_end_to_end_mces.json"
    write_metadata_file(output, [wu.metadata for wu in source.get_workunits()])
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=GOLDEN_DIR / "sns_end_to_end_mces_golden.json",
    )
    report = source.get_report()
    assert report.topics_scanned == 1
    assert report.subscriptions_scanned == 3
    assert report.schemas_attached == 1
    assert report.lineage_edges_emitted == 3
    assert report.column_lineage_edges_emitted == 3
    assert report.lambda_functions_emitted == 1
    assert report.eventbridge_rules_emitted == 1
    assert report.eventbridge_pipes_emitted == 1
    assert not report.failures
    assert not report.warnings
