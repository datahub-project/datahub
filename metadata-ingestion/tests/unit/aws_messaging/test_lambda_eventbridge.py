import io
import json
import zipfile
from typing import Any, Dict, List
from unittest.mock import MagicMock

import boto3

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.source import AwsMessagingSource
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    MetadataChangeProposalClass,
)
from tests.unit.aws_messaging.conftest import (
    ACCOUNT,
    REGION,
    base_config,
    sns_client,
    sqs_client,
)

QUEUE_URN = f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.orders,PROD)"
DLQ_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.orders-failed,PROD)"
)
TOPIC_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:sns,{ACCOUNT}.{REGION}.order-events,PROD)"
)


def _lambda_job(name: str) -> str:
    return f"urn:li:dataJob:(urn:li:dataFlow:(lambda,{ACCOUNT}.{REGION}.{name},PROD),invoke)"


def _function(name: str) -> str:
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(
        RoleName=f"{name}-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )["Role"]["Arn"]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "handler.py", "def handler(event, context):\n    return None\n"
        )
    return boto3.client("lambda", region_name=REGION).create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=role,
        Handler="handler.handler",
        Code={"ZipFile": buffer.getvalue()},
    )["FunctionArn"]


def _queue_arn(url: str) -> str:
    return sqs_client().get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]


def _io(source: AwsMessagingSource) -> Dict[str, DataJobInputOutputClass]:
    result: Dict[str, DataJobInputOutputClass] = {}
    for workunit in source.get_workunits():
        metadata = workunit.metadata
        if isinstance(metadata, MetadataChangeProposalWrapper) and isinstance(
            metadata.aspect, DataJobInputOutputClass
        ):
            assert metadata.entityUrn is not None
            result[metadata.entityUrn] = metadata.aspect
    return result


def _run(**overrides: Any) -> AwsMessagingSource:
    return AwsMessagingSource.create(
        base_config(**overrides), PipelineContext(run_id="aws-messaging-test")
    )


def test_lambda_event_source_mapping_and_sns_subscription(aws: None) -> None:
    sqs = sqs_client()
    queue_arn = _queue_arn(sqs.create_queue(QueueName="orders")["QueueUrl"])
    failed_arn = _queue_arn(sqs.create_queue(QueueName="orders-failed")["QueueUrl"])
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    worker = _function("worker")
    notifier = _function("notifier")
    _function("unrelated")
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=worker,
        DestinationConfig={"OnFailure": {"Destination": failed_arn}},
    )
    sns_client().subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=notifier)

    jobs = _io(_run(lineage={"lambda_async_destinations": False}))

    assert set(jobs) == {_lambda_job("worker"), _lambda_job("notifier")}
    assert jobs[_lambda_job("worker")].inputDatasets == [QUEUE_URN]
    assert jobs[_lambda_job("worker")].outputDatasets == [DLQ_URN]
    assert jobs[_lambda_job("notifier")].inputDatasets == [TOPIC_URN]


def test_lambda_off_emits_no_functions(aws: None) -> None:
    queue_arn = _queue_arn(sqs_client().create_queue(QueueName="orders")["QueueUrl"])
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=_function("worker")
    )
    assert _io(_run(lineage={"lambda_functions": False})) == {}


def test_eventbridge_rule_and_pipe(aws: None) -> None:
    queue_arn = _queue_arn(sqs_client().create_queue(QueueName="orders")["QueueUrl"])
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="new-orders", EventPattern=json.dumps({"source": ["shop"]}))
    events.put_targets(Rule="new-orders", Targets=[{"Id": "q", "Arn": queue_arn}])
    events.put_rule(Name="other", EventPattern=json.dumps({"source": ["x"]}))
    events.put_targets(
        Rule="other",
        Targets=[
            {"Id": "s", "Arn": f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:m"}
        ],
    )
    boto3.client("pipes", region_name=REGION).create_pipe(
        Name="orders-to-topic",
        Source=queue_arn,
        Target=topic_arn,
        RoleArn=f"arn:aws:iam::{ACCOUNT}:role/pipe",
    )

    jobs = _io(_run(lineage={"lambda_functions": False}))

    rule_job = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.default.new-orders,PROD),route)"
    pipe_job = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge-pipes,{ACCOUNT}.{REGION}.orders-to-topic,PROD),pipe)"
    assert set(jobs) == {rule_job, pipe_job}
    assert jobs[rule_job].inputDatasets == [
        f"urn:li:dataset:(urn:li:dataPlatform:eventbridge,{ACCOUNT}.{REGION}.default,PROD)"
    ]
    assert jobs[rule_job].outputDatasets == [QUEUE_URN]
    assert (jobs[pipe_job].inputDatasets, jobs[pipe_job].outputDatasets) == (
        [QUEUE_URN],
        [TOPIC_URN],
    )


def _firehose_patches(source: AwsMessagingSource) -> List[MetadataChangeProposalClass]:
    return [
        workunit.metadata
        for workunit in source.get_workunits()
        if isinstance(workunit.metadata, MetadataChangeProposalClass)
        and workunit.metadata.entityType == "dataJob"
    ]


def _subscribe_firehose() -> str:
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    stream = f"arn:aws:firehose:{REGION}:{ACCOUNT}:deliverystream/orders-archive"
    sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol="firehose",
        Endpoint=stream,
        Attributes={"SubscriptionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/sns-firehose"},
    )
    return f"urn:li:dataJob:(urn:li:dataFlow:(kinesis-firehose,{ACCOUNT}.{REGION}.orders-archive,PROD),delivery)"


def test_firehose_edge_needs_the_job_in_datahub(aws: None) -> None:
    job_urn = _subscribe_firehose()
    graph = MagicMock()
    graph.exists.return_value = True
    ctx = PipelineContext(run_id="aws-messaging-test", graph=graph)
    source = AwsMessagingSource.create(base_config(include_sqs=False), ctx)
    patches = _firehose_patches(source)
    assert [patch.entityUrn for patch in patches] == [job_urn]
    graph.exists.assert_called_with(job_urn)

    graph.exists.return_value = False
    source = AwsMessagingSource.create(base_config(include_sqs=False), ctx)
    assert _firehose_patches(source) == []
    assert any(
        job_urn in skipped for skipped in source.get_report().lineage_edges_skipped
    )


def test_firehose_edge_skipped_without_graph(aws: None) -> None:
    _subscribe_firehose()
    source = _run(include_sqs=False)
    assert _firehose_patches(source) == []


def test_lambda_async_destination(aws: None) -> None:
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    _function("unrelated")
    boto3.client("lambda", region_name=REGION).put_function_event_invoke_config(
        FunctionName=_function("worker"),
        DestinationConfig={"OnFailure": {"Destination": topic_arn}},
    )
    jobs = _io(_run(include_sqs=False))
    assert set(jobs) == {_lambda_job("worker")}
    assert jobs[_lambda_job("worker")].outputDatasets == [TOPIC_URN]
