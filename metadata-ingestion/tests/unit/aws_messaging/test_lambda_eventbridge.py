import io
import json
import zipfile
from typing import Any, Dict, List, Set, Type
from unittest.mock import MagicMock

import boto3

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.base import AwsMessagingSourceBase
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    MetadataChangeProposalClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
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
RULE_JOB = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.default.new-orders,PROD),route)"
PIPE_JOB = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge-pipes,{ACCOUNT}.{REGION}.orders-to-topic,PROD),pipe)"
BUS_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:eventbridge,{ACCOUNT}.{REGION}.default,PROD)"
)

Edges = Dict[str, Dict[str, Set[str]]]


def lambda_job(name: str) -> str:
    return f"urn:li:dataJob:(urn:li:dataFlow:(lambda,{ACCOUNT}.{REGION}.{name},PROD),invoke)"


def create_function(name: str) -> str:
    role = boto3.client("iam", region_name=REGION).create_role(
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


def create_queue(name: str) -> str:
    url = sqs_client().create_queue(QueueName=name)["QueueUrl"]
    return sqs_client().get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]


def job_edges(workunits: List[Any]) -> Edges:
    """Job urn -> edge kind (inputDatasetEdges, ...) -> urns, from io patches."""
    edges: Edges = {}
    for workunit in workunits:
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "dataJobInputOutput"
        ):
            assert metadata.entityUrn is not None and metadata.aspect is not None
            assert not workunit.is_primary_source
            for op in json.loads(metadata.aspect.value):
                _, kind, target = op["path"].split("/", 2)
                edges.setdefault(metadata.entityUrn, {}).setdefault(kind, set()).add(
                    target
                )
    return edges


def run(source_class: Type[AwsMessagingSourceBase], **overrides: Any) -> Edges:
    source = source_class.create(
        base_config(**overrides), PipelineContext(run_id="lineage")
    )
    return job_edges(list(source.get_workunits()))


def test_lambda_edges_are_split_by_owner(aws: None) -> None:
    queue_arn = create_queue("orders")
    failed_arn = create_queue("orders-failed")
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    worker = create_function("worker")
    notifier = create_function("notifier")
    create_function("unrelated")
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=worker,
        DestinationConfig={"OnFailure": {"Destination": failed_arn}},
    )
    sns_client().subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=notifier)
    boto3.client("lambda", region_name=REGION).put_function_event_invoke_config(
        FunctionName=worker,
        DestinationConfig={"OnSuccess": {"Destination": topic_arn}},
    )

    assert run(SqsSource) == {
        lambda_job("worker"): {
            "inputDatasetEdges": {QUEUE_URN},
            "outputDatasetEdges": {DLQ_URN},
        }
    }
    assert run(SnsSource) == {
        lambda_job("notifier"): {"inputDatasetEdges": {TOPIC_URN}},
        lambda_job("worker"): {"outputDatasetEdges": {TOPIC_URN}},
    }


def test_lambda_off(aws: None) -> None:
    queue_arn = create_queue("orders")
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=create_function("worker")
    )
    assert run(SqsSource, lineage={"lambda_functions": False}) == {}


def test_rule_and_pipe_edges_are_split_by_owner(aws: None) -> None:
    queue_arn = create_queue("orders")
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="new-orders", EventPattern=json.dumps({"source": ["shop"]}))
    events.put_targets(
        Rule="new-orders",
        Targets=[{"Id": "q", "Arn": queue_arn}, {"Id": "t", "Arn": topic_arn}],
    )
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

    assert run(SqsSource, lineage={"lambda_functions": False}) == {
        RULE_JOB: {"inputDatasetEdges": {BUS_URN}, "outputDatasetEdges": {QUEUE_URN}},
        PIPE_JOB: {"inputDatasetEdges": {QUEUE_URN}},
    }
    assert run(SnsSource, lineage={"lambda_functions": False}) == {
        RULE_JOB: {"inputDatasetEdges": {BUS_URN}, "outputDatasetEdges": {TOPIC_URN}},
        PIPE_JOB: {"outputDatasetEdges": {TOPIC_URN}},
    }


def subscribe_firehose() -> str:
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol="firehose",
        Endpoint=f"arn:aws:firehose:{REGION}:{ACCOUNT}:deliverystream/orders-archive",
        Attributes={"SubscriptionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/sns-firehose"},
    )
    return f"urn:li:dataJob:(urn:li:dataFlow:(kinesis-firehose,{ACCOUNT}.{REGION}.orders-archive,PROD),delivery)"


def test_firehose_edge_needs_the_job_in_datahub(aws: None) -> None:
    job_urn = subscribe_firehose()
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    graph.exists.return_value = True
    ctx = PipelineContext(run_id="lineage", graph=graph)
    edges = job_edges(list(SnsSource.create(base_config(), ctx).get_workunits()))
    assert edges == {job_urn: {"inputDatasetEdges": {TOPIC_URN}}}

    graph.exists.return_value = False
    source = SnsSource.create(base_config(), ctx)
    assert job_edges(list(source.get_workunits())) == {}
    assert any(
        job_urn in skipped for skipped in source.get_report().lineage_edges_skipped
    )


def test_firehose_edge_skipped_without_graph(aws: None) -> None:
    subscribe_firehose()
    assert run(SnsSource) == {}


def _schema(*paths: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="s",
        platform="urn:li:dataPlatform:sqs",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=path,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
            for path in paths
        ],
    )


def _pipe_source(enrichment: bool) -> SnsSource:
    queue_arn = create_queue("orders")
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    pipe: Dict[str, Any] = {
        "Name": "orders-to-topic",
        "Source": queue_arn,
        "Target": topic_arn,
        "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/pipe",
    }
    if enrichment:
        pipe["Enrichment"] = create_function("enrich")
    boto3.client("pipes", region_name=REGION).create_pipe(**pipe)
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = lambda urn: {
        QUEUE_URN: _schema("order_id", "amount"),
        TOPIC_URN: _schema("ORDER_ID", "other"),
    }.get(urn)
    return SnsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage", graph=graph),
    )


def test_pipe_without_enrichment_gets_column_lineage(aws: None) -> None:
    source = _pipe_source(enrichment=False)
    edges = job_edges(list(source.get_workunits()))
    assert set(edges[PIPE_JOB]["fineGrainedLineages"]) == {
        f"NONE/urn:li:schemaField:({TOPIC_URN},ORDER_ID)/NONE/urn:li:schemaField:({QUEUE_URN},order_id)"
    }


def test_pipe_with_enrichment_gets_table_lineage_only(aws: None) -> None:
    source = _pipe_source(enrichment=True)
    edges = job_edges(list(source.get_workunits()))
    assert "fineGrainedLineages" not in edges[PIPE_JOB]
    assert source.get_report().column_lineage_skipped == {
        "pipe has an enrichment step": 1
    }
