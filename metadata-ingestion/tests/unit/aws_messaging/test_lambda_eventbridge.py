import io
import json
import zipfile
from typing import Any, Dict, Iterator, List, Optional, Set, Type
from unittest.mock import MagicMock

import boto3
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.base import AwsMessagingSourceBase
from datahub.ingestion.source.aws_messaging.clients import EventBridgeClient
from datahub.ingestion.source.aws_messaging.eventbridge import (
    event_path_prefix,
    template_field_map,
)
from datahub.ingestion.source.aws_messaging.models import RuleTarget
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
                # JSON Pointer escapes `/` inside a key as `~1`.
                target = target.replace("~1", "/").replace("~0", "~")
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
    config = base_config(firehose_lineage=True)
    edges = job_edges(list(SnsSource.create(config, ctx).get_workunits()))
    assert edges == {job_urn: {"inputDatasetEdges": {TOPIC_URN}}}

    graph.exists.return_value = False
    source = SnsSource.create(config, ctx)
    assert job_edges(list(source.get_workunits())) == {}
    assert any(
        job_urn in skipped for skipped in source.get_report().lineage_edges_skipped
    )


def test_firehose_edge_skipped_without_graph(aws: None) -> None:
    subscribe_firehose()
    assert run(SnsSource, firehose_lineage=True) == {}


def test_firehose_lineage_is_off_by_default(aws: None) -> None:
    subscribe_firehose()
    graph = MagicMock()
    graph.exists.return_value = True
    graph.get_schema_metadata.return_value = None
    source = SnsSource.create(
        base_config(), PipelineContext(run_id="lineage", graph=graph)
    )
    assert job_edges(list(source.get_workunits())) == {}
    graph.exists.assert_not_called()


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


def _graph(schemas: Dict[str, SchemaMetadataClass]) -> MagicMock:
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = schemas.get
    return graph


def _pipe_source(enrichment: bool = False, template: Optional[str] = None) -> SnsSource:
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
    if template is not None:
        pipe["TargetParameters"] = {"InputTemplate": template}
    boto3.client("pipes", region_name=REGION).create_pipe(**pipe)
    graph = _graph(
        {
            QUEUE_URN: _schema("order_id", "amount", "customer.id"),
            TOPIC_URN: _schema("ORDER_ID", "total", "customer_id", "other"),
        }
    )
    return SnsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage", graph=graph),
    )


def _pairs(edges: Edges, job: str) -> Set[str]:
    return {
        f"{path.rsplit(',', 1)[-1].rstrip(')')} -> {path.split('/')[1].rsplit(',', 1)[-1].rstrip(')')}"
        for path in edges.get(job, {}).get("fineGrainedLineages", set())
    }


def test_pipe_with_plain_template_maps_fields(aws: None) -> None:
    source = _pipe_source(
        template='{"order_id": <$.body.order_id>, "total": "<$.body.amount>", "customer_id": <$.body.customer.id>}'
    )
    edges = job_edges(list(source.get_workunits()))
    assert _pairs(edges, PIPE_JOB) == {
        "order_id -> ORDER_ID",
        "amount -> total",
        "customer.id -> customer_id",
    }


def test_pipe_without_template_gets_table_lineage_only(aws: None) -> None:
    # Without a template the target receives the SQS record, not the message.
    source = _pipe_source()
    edges = job_edges(list(source.get_workunits()))
    assert "fineGrainedLineages" not in edges[PIPE_JOB]
    assert source.get_report().column_lineage_skipped == {
        "pipe without an input template delivers the SQS record": 1
    }


def test_pipe_with_mixed_template_is_skipped(aws: None) -> None:
    source = _pipe_source(template='{"order_id": "id-<$.body.order_id>"}')
    edges = job_edges(list(source.get_workunits()))
    assert "fineGrainedLineages" not in edges[PIPE_JOB]
    assert source.get_report().column_lineage_skipped == {
        "pipe input template is not a plain field mapping": 1
    }


def test_pipe_with_enrichment_gets_table_lineage_only(aws: None) -> None:
    source = _pipe_source(enrichment=True)
    edges = job_edges(list(source.get_workunits()))
    assert "fineGrainedLineages" not in edges[PIPE_JOB]
    assert source.get_report().column_lineage_skipped == {
        "pipe has an enrichment step": 1
    }


@pytest.mark.parametrize(
    "template, expected",
    [
        ('{"a": <$.body.x>, "b": {"c": "<$.body.y.z>"}}', {"a": "x", "b.c": "y.z"}),
        ('{"a": <$.body.x>, "b": "literal"}', None),
        ('{"a": <$.messageId>}', None),
        ('{"a": [<$.body.x>]}', None),
        ("<$.body>", None),
        ("not json", None),
    ],
)
def test_template_field_map(template: str, expected: Optional[Dict[str, str]]) -> None:
    assert template_field_map(template) == expected


def test_event_path_prefix() -> None:
    assert event_path_prefix(None) == ""
    assert event_path_prefix("$") == ""
    assert event_path_prefix("$.detail") == "detail"
    assert event_path_prefix("$.detail.items[0]") is None


def _rule_source(**target: Any) -> SqsSource:
    queue_arn = create_queue("orders")
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="new-orders", EventPattern=json.dumps({"source": ["shop"]}))
    events.put_targets(
        Rule="new-orders", Targets=[{"Id": "q", "Arn": queue_arn, **target}]
    )
    graph = _graph(
        {
            BUS_URN: _schema("source", "detail.order_id", "detail.amount"),
            QUEUE_URN: _schema("order_id", "AMOUNT", "unrelated"),
        }
    )
    return SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage", graph=graph),
    )


def test_rule_input_path_maps_the_detail_subtree(aws: None) -> None:
    edges = job_edges(list(_rule_source(InputPath="$.detail").get_workunits()))
    assert _pairs(edges, RULE_JOB) == {
        "detail.order_id -> order_id",
        "detail.amount -> AMOUNT",
    }


def test_rule_without_input_path_maps_the_whole_event(aws: None) -> None:
    edges = job_edges(list(_rule_source().get_workunits()))
    # The queue has no top-level `source` field, and `order_id` is not top-level
    # in the event, so nothing matches.
    assert _pairs(edges, RULE_JOB) == set()


def test_rule_with_input_transformer_is_skipped(aws: None) -> None:
    source = _rule_source(
        InputTransformer={
            "InputPathsMap": {"id": "$.detail.order_id"},
            "InputTemplate": '{"order_id": <id>}',
        }
    )
    edges = job_edges(list(source.get_workunits()))
    assert _pairs(edges, RULE_JOB) == set()
    assert source.get_report().column_lineage_skipped == {
        "rule target uses an input transformer or constant input": 1
    }


def test_partner_event_bus_keeps_its_full_name(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # moto can't create partner buses, so the EventBridge calls are faked.
    bus_name = "aws.partner/example.com/1234/orders"
    queue_arn = create_queue("orders")
    seen_buses: List[str] = []

    def list_rules(self: EventBridgeClient, bus: str) -> Iterator[Dict[str, Any]]:
        seen_buses.append(bus)
        yield {"Name": "new-orders"}

    monkeypatch.setattr(
        EventBridgeClient,
        "list_event_bus_arns",
        lambda self: iter([f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/{bus_name}"]),
    )
    monkeypatch.setattr(EventBridgeClient, "list_rules", list_rules)
    monkeypatch.setattr(
        EventBridgeClient,
        "list_targets",
        lambda self, rule, bus: iter([RuleTarget(arn=queue_arn)]),
    )

    edges = run(
        SqsSource, lineage={"lambda_functions": False, "eventbridge_pipes": False}
    )

    assert seen_buses == [bus_name]
    bus_urn = f"urn:li:dataset:(urn:li:dataPlatform:eventbridge,{ACCOUNT}.{REGION}.{bus_name},PROD)"
    rule_job = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.{bus_name}.new-orders,PROD),route)"
    assert edges[rule_job]["inputDatasetEdges"] == {bus_urn}
