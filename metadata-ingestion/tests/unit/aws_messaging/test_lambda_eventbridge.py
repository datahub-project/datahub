import io
import json
import zipfile
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from datahub.configuration.common import GraphError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.openapi import RelationshipDirection
from datahub.ingestion.source.aws_messaging.base import AwsMessagingSourceBase
from datahub.ingestion.source.aws_messaging.clients import (
    EventBridgeClient,
    LambdaClient,
    PipesClient,
    SnsClient,
    SqsClient,
)
from datahub.ingestion.source.aws_messaging.config import SqsSourceConfig
from datahub.ingestion.source.aws_messaging.eventbridge import (
    template_field_map,
)
from datahub.ingestion.source.aws_messaging.jobs import JobEdgeTracker
from datahub.ingestion.source.aws_messaging.models import RuleTarget
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataChangeProposalClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
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
RULE_JOB = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.default/new-orders,PROD),route)"
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

    async_on = {"lambda_async_destinations": True}
    assert run(SqsSource, lineage=async_on) == {
        lambda_job("worker"): {
            "inputDatasetEdges": {QUEUE_URN},
            "outputDatasetEdges": {DLQ_URN},
        }
    }
    assert run(SnsSource, lineage=async_on) == {
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
        base_config(lineage={"lambda_functions": False}, include_column_lineage=True),
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


def test_partner_event_bus_keeps_its_full_name(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # moto can't create partner buses, so the EventBridge calls are faked.
    bus_name = "aws.partner/example.com/1234/orders"
    queue_arn = create_queue("orders")
    seen_buses: List[str] = []

    def list_rules(
        self: EventBridgeClient, bus: str, on_error: Any
    ) -> Iterator[Dict[str, Any]]:
        seen_buses.append(bus)
        yield {"Name": "new-orders"}

    monkeypatch.setattr(
        EventBridgeClient,
        "list_event_bus_arns",
        lambda self, on_error: iter(
            [f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/{bus_name}"]
        ),
    )
    monkeypatch.setattr(EventBridgeClient, "list_rules", list_rules)
    monkeypatch.setattr(
        EventBridgeClient,
        "list_targets",
        lambda self, rule, bus, on_error: iter([RuleTarget(arn=queue_arn)]),
    )

    edges = run(
        SqsSource, lineage={"lambda_functions": False, "eventbridge_pipes": False}
    )

    assert seen_buses == [bus_name]
    bus_urn = f"urn:li:dataset:(urn:li:dataPlatform:eventbridge,{ACCOUNT}.{REGION}.{bus_name},PROD)"
    rule_job = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.{bus_name}/new-orders,PROD),route)"
    assert edges[rule_job]["inputDatasetEdges"] == {bus_urn}


def test_dotted_bus_and_rule_names_get_separate_jobs(aws: None) -> None:
    # Bus "orders.eu" with rule "new" and bus "orders" with rule "eu.new" must not
    # share a job.
    queue_arn = create_queue("orders")
    events = boto3.client("events", region_name=REGION)
    for bus, rule in (("orders.eu", "new"), ("orders", "eu.new")):
        events.create_event_bus(Name=bus)
        events.put_rule(
            Name=rule,
            EventBusName=bus,
            EventPattern=json.dumps({"source": ["shop"]}),
        )
        events.put_targets(
            Rule=rule, EventBusName=bus, Targets=[{"Id": "q", "Arn": queue_arn}]
        )

    edges = run(
        SqsSource, lineage={"lambda_functions": False, "eventbridge_pipes": False}
    )

    jobs = {
        urn
        for urn in edges
        if urn.startswith("urn:li:dataJob:(urn:li:dataFlow:(eventbridge,")
    }
    assert jobs == {
        f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.orders.eu/new,PROD),route)",
        f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.orders/eu.new,PROD),route)",
    }


def test_shared_entities_get_status(aws: None) -> None:
    # Lambda jobs, rules, pipes and buses are non-primary, so the status
    # processor skips them; the sources write Status themselves.
    queue_arn = create_queue("orders")
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=create_function("worker")
    )
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="new-orders", EventPattern=json.dumps({"source": ["shop"]}))
    events.put_targets(Rule="new-orders", Targets=[{"Id": "q", "Arn": queue_arn}])
    source = SqsSource.create(base_config(), PipelineContext(run_id="lineage"))
    with_status = {
        wu.get_urn()
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, StatusClass)
    }
    lambda_flow = f"urn:li:dataFlow:(lambda,{ACCOUNT}.{REGION}.worker,PROD)"
    rule_flow = (
        f"urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.default/new-orders,PROD)"
    )
    assert {
        lambda_flow,
        lambda_job("worker"),
        rule_flow,
        RULE_JOB,
        BUS_URN,
    } <= with_status


def test_eventbridge_container_links_to_the_events_console(aws: None) -> None:
    queue_arn = create_queue("orders")
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="new-orders", EventPattern=json.dumps({"source": ["shop"]}))
    events.put_targets(Rule="new-orders", Targets=[{"Id": "q", "Arn": queue_arn}])
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage"),
    )
    urls = [
        wu.metadata.aspect.externalUrl
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, ContainerPropertiesClass)
    ]
    assert (
        f"https://{REGION}.console.aws.amazon.com/events/home?region={REGION}" in urls
    )
    assert (
        f"https://{REGION}.console.aws.amazon.com/sqs/v3/home?region={REGION}" in urls
    )


def test_lambda_listing_failure_does_not_fail_the_run() -> None:
    # Lambda lineage is optional: a mid-pagination failure there must not mark
    # the run failed, which would stop stale removal of queues and topics.
    client = boto3.client(
        "lambda",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    report = SqsSourceReport()
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_event_source_mappings",
            {"EventSourceMappings": [], "NextMarker": "page-2"},
        )
        stubber.add_client_error(
            "list_event_source_mappings", service_error_code="TooManyRequestsException"
        )
        failures: List[Exception] = []
        mappings = LambdaClient(client, report).list_event_source_mappings(
            on_error=failures.append
        )
        assert list(mappings) == []
    assert report.warnings and not report.failures
    assert len(failures) == 1


def test_scheduled_rule_has_no_event_bus_input(aws: None) -> None:
    queue_arn = create_queue("orders")
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="nightly", ScheduleExpression="rate(1 day)")
    events.put_targets(Rule="nightly", Targets=[{"Id": "q", "Arn": queue_arn}])
    nightly_job = f"urn:li:dataJob:(urn:li:dataFlow:(eventbridge,{ACCOUNT}.{REGION}.default/nightly,PROD),route)"
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage"),
    )
    workunits = list(source.get_workunits())
    assert job_edges(workunits) == {nightly_job: {"outputDatasetEdges": {QUEUE_URN}}}
    # Only a scheduled rule uses the bus here, so the bus isn't emitted either.
    assert BUS_URN not in {wu.get_urn() for wu in workunits}


def job_removals(workunits: List[Any]) -> Set[str]:
    removed: Set[str] = set()
    for workunit in workunits:
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "dataJobInputOutput"
        ):
            assert metadata.aspect is not None
            removed.update(
                f"{metadata.entityUrn} {op['path']}"
                for op in json.loads(metadata.aspect.value)
                if op["op"] == "remove"
            )
    return removed


def stale_graph(
    *edges: Tuple[str, str, str],
    io: Optional[DataJobInputOutputClass] = None,
) -> MagicMock:
    """A graph holding `(job, relationship, dataset)` edges from an earlier run."""
    graph = MagicMock()
    graph.get_related_entities.side_effect = lambda urn, types, direction: (
        [
            MagicMock(urn=job, relationship_type=kind)
            for job, kind, dataset in edges
            if dataset == urn and kind in types
        ]
        if direction == RelationshipDirection.INCOMING
        else []
    )
    graph.get_aspect.side_effect = lambda urn, aspect: (
        io if aspect is DataJobInputOutputClass else None
    )
    graph.get_schema_metadata.return_value = None
    return graph


def stale_rule_graph() -> MagicMock:
    return stale_graph((RULE_JOB, "Produces", QUEUE_URN))


def test_deleted_rule_target_edge_is_retracted(aws: None) -> None:
    create_queue("orders")
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage", graph=stale_rule_graph()),
    )
    workunits = list(source.get_workunits())
    assert job_removals(workunits) == {
        f"{RULE_JOB} /outputDatasetEdges/{QUEUE_URN.replace('/', '~1')}"
    }
    assert source.get_report().job_edges_removed == 1


def test_rule_edges_kept_when_rules_are_not_read(aws: None) -> None:
    create_queue("orders")
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False, "eventbridge_rules": False}),
        PipelineContext(run_id="lineage", graph=stale_rule_graph()),
    )
    assert job_removals(list(source.get_workunits())) == set()


def test_async_destinations_stop_after_access_denied(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_queue("orders")
    create_function("first")
    create_function("second")
    calls: List[str] = []

    def denied(self: LambdaClient, function_name: str, on_error: Any) -> Iterator[str]:
        calls.append(function_name)
        on_error(
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "x"}}, "L"
            )
        )
        yield from ()

    monkeypatch.setattr(LambdaClient, "list_async_destinations", denied)
    source = SqsSource.create(
        base_config(lineage={"lambda_async_destinations": True}),
        PipelineContext(run_id="lineage"),
    )
    list(source.get_workunits())
    assert len(calls) == 1


def test_other_platform_region_containers_are_shared(aws: None) -> None:
    queue_arn = create_queue("orders")
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="new-orders", EventPattern=json.dumps({"source": ["shop"]}))
    events.put_targets(Rule="new-orders", Targets=[{"Id": "q", "Arn": queue_arn}])
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage"),
    )
    primary_containers: Dict[str, bool] = {}
    for wu in source.get_workunits_internal():
        if wu.get_urn().startswith("urn:li:container:") and isinstance(
            wu.metadata, MetadataChangeProposalWrapper
        ):
            if isinstance(wu.metadata.aspect, ContainerPropertiesClass):
                primary_containers[
                    wu.metadata.aspect.name
                    + str(wu.metadata.aspect.customProperties.get("platform"))
                ] = wu.is_primary_source
    assert primary_containers == {
        f"{REGION}sqs": True,
        f"{REGION}eventbridge": False,
    }


def removed_edge(job: str, kind: str, dataset: str) -> str:
    return f"{job} /{kind}/{dataset.replace('/', '~1')}"


def throttled(on_error: Callable[[ClientError], None]) -> Iterator[Any]:
    on_error(ClientError({"Error": {"Code": "Throttling", "Message": "x"}}, "List"))
    yield from ()


def test_failed_run_removes_no_job_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_queue("orders")
    real = SqsClient.list_queue_urls

    def page_two_fails(self: SqsClient, *args: Any, **kwargs: Any) -> Iterator[str]:
        yield from real(self, *args, **kwargs)
        self.report.report_listing_failure(
            pages_fetched=1,
            exc=ClientError(
                {"Error": {"Code": "Throttling", "Message": "x"}}, "ListQueues"
            ),
            api_label="sqs:ListQueues",
        )

    monkeypatch.setattr(SqsClient, "list_queue_urls", page_two_fails)
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage", graph=stale_rule_graph()),
    )
    workunits = list(source.get_workunits())
    assert source.get_report().failures
    assert job_removals(workunits) == set()


def test_other_accounts_job_edges_are_kept(aws: None) -> None:
    # Another account's recipe wrote this edge from its own rule; this recipe
    # can't list that account's rules, so not seeing it says nothing.
    create_queue("orders")
    other_rule = RULE_JOB.replace(ACCOUNT, "210987654321")
    other_region_rule = RULE_JOB.replace(f".{REGION}.", ".eu-west-1.")
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(
            run_id="lineage",
            graph=stale_graph(
                (other_rule, "Produces", QUEUE_URN),
                (other_region_rule, "Produces", QUEUE_URN),
                (RULE_JOB, "Produces", QUEUE_URN),
            ),
        ),
    )
    assert job_removals(list(source.get_workunits())) == {
        removed_edge(RULE_JOB, "outputDatasetEdges", QUEUE_URN)
    }


@pytest.mark.parametrize("listing_fails", [False, True])
@pytest.mark.parametrize(
    "client, method, job",
    [
        (LambdaClient, "list_event_source_mappings", lambda_job("worker")),
        (PipesClient, "list_pipes", PIPE_JOB),
    ],
)
def test_failed_listing_keeps_consumes_edges(
    aws: None,
    monkeypatch: pytest.MonkeyPatch,
    client: Type[Any],
    method: str,
    job: str,
    listing_fails: bool,
) -> None:
    create_queue("orders")
    if listing_fails:
        monkeypatch.setattr(client, method, lambda self, on_error: throttled(on_error))
    source = SqsSource.create(
        base_config(),
        PipelineContext(
            run_id="lineage", graph=stale_graph((job, "Consumes", QUEUE_URN))
        ),
    )
    removals = job_removals(list(source.get_workunits()))
    assert removals == (
        set() if listing_fails else {removed_edge(job, "inputDatasetEdges", QUEUE_URN)}
    )


def test_failed_rule_listing_keeps_rule_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_queue("orders")
    monkeypatch.setattr(
        EventBridgeClient,
        "list_event_bus_arns",
        lambda self, on_error: throttled(on_error),
    )
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False}),
        PipelineContext(run_id="lineage", graph=stale_rule_graph()),
    )
    assert job_removals(list(source.get_workunits())) == set()


@pytest.mark.parametrize("subscriptions_fail", [False, True])
def test_unread_subscriptions_keep_lambda_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch, subscriptions_fail: bool
) -> None:
    sns_client().create_topic(Name="order-events")
    if subscriptions_fail:
        monkeypatch.setattr(
            SnsClient,
            "list_subscriptions",
            lambda self, topic_arn, on_error: throttled(on_error),
        )
    job = lambda_job("notifier")
    source = SnsSource.create(
        base_config(),
        PipelineContext(
            run_id="lineage", graph=stale_graph((job, "Consumes", TOPIC_URN))
        ),
    )
    removals = job_removals(list(source.get_workunits()))
    assert removals == (
        set()
        if subscriptions_fail
        else {removed_edge(job, "inputDatasetEdges", TOPIC_URN)}
    )


def _field(dataset: str, path: str) -> str:
    return f"urn:li:schemaField:({dataset},{path})"


def _pipe_field_edge(upstream: str, downstream: str) -> FineGrainedLineageClass:
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[_field(QUEUE_URN, upstream)],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[_field(TOPIC_URN, downstream)],
    )


def _pipe_field_graph() -> MagicMock:
    graph = stale_graph(
        (PIPE_JOB, "Produces", TOPIC_URN),
        io=DataJobInputOutputClass(
            inputDatasets=[],
            outputDatasets=[],
            fineGrainedLineages=[
                _pipe_field_edge("order_id", "ORDER_ID"),
                _pipe_field_edge("amount", "other"),
            ],
        ),
    )
    graph.get_schema_metadata.side_effect = {
        QUEUE_URN: _schema("order_id", "amount"),
        TOPIC_URN: _schema("ORDER_ID", "other"),
    }.get
    return graph


def _removed_field_edges(workunits: List[Any]) -> Set[str]:
    return {
        removal.split("/")[-1].replace("~1", "/")
        for removal in job_removals(workunits)
        if "/fineGrainedLineages/" in removal
    }


def _pipe_to_topic(template: Optional[str]) -> None:
    pipe: Dict[str, Any] = {
        "Name": "orders-to-topic",
        "Source": create_queue("orders"),
        "Target": sns_client().create_topic(Name="order-events")["TopicArn"],
        "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/pipe",
    }
    if template is not None:
        pipe["TargetParameters"] = {"InputTemplate": template}
    boto3.client("pipes", region_name=REGION).create_pipe(**pipe)


def _pipe_run() -> List[Any]:
    source = SnsSource.create(
        base_config(lineage={"lambda_functions": False}, include_column_lineage=True),
        PipelineContext(run_id="lineage", graph=_pipe_field_graph()),
    )
    return list(source.get_workunits())


def test_pipe_field_edge_no_longer_mapped_is_retracted(aws: None) -> None:
    _pipe_to_topic('{"ORDER_ID": <$.body.order_id>}')
    workunits = _pipe_run()
    assert _removed_field_edges(workunits) == {_field(QUEUE_URN, "amount")}
    assert removed_edge(PIPE_JOB, "outputDatasetEdges", TOPIC_URN) not in (
        job_removals(workunits)
    )


def test_deleted_pipe_field_edges_are_retracted(aws: None) -> None:
    create_queue("orders")
    sns_client().create_topic(Name="order-events")
    workunits = _pipe_run()
    assert removed_edge(PIPE_JOB, "outputDatasetEdges", TOPIC_URN) in job_removals(
        workunits
    )
    assert _removed_field_edges(workunits) == {
        _field(QUEUE_URN, "order_id"),
        _field(QUEUE_URN, "amount"),
    }


def test_unreadable_pipe_template_keeps_field_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _pipe_to_topic('{"ORDER_ID": <$.body.order_id>}')
    monkeypatch.setattr(PipesClient, "input_template", lambda self, name: None)
    assert job_removals(_pipe_run()) == set()


def test_malformed_pipe_field_urn_is_kept(aws: None) -> None:
    # Another writer's unparseable field urn must not abort the run.
    _pipe_to_topic('{"ORDER_ID": <$.body.order_id>}')
    graph = _pipe_field_graph()
    io = graph.get_aspect(PIPE_JOB, DataJobInputOutputClass)
    io.fineGrainedLineages.append(
        FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[_field(QUEUE_URN, "order_id")],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=["urn:li:schemaField:not-a-field-urn"],
        )
    )
    source = SnsSource.create(
        base_config(lineage={"lambda_functions": False}, include_column_lineage=True),
        PipelineContext(run_id="lineage", graph=graph),
    )
    workunits = list(source.get_workunits())
    assert _removed_field_edges(workunits) == {_field(QUEUE_URN, "amount")}
    assert "Stale field lineage not checked" in [
        w.title for w in source.get_report().warnings
    ]


def test_stale_cross_account_job_edge_is_retracted() -> None:
    # No recipe reads another account's queue in full, so the edge from this
    # recipe's rule is checked from the job's side.
    remote_queue = (
        f"urn:li:dataset:(urn:li:dataPlatform:sqs,210987654321.{REGION}.refunds,PROD)"
    )
    remote_topic = (
        f"urn:li:dataset:(urn:li:dataPlatform:sns,210987654321.{REGION}.refunds,PROD)"
    )
    graph = stale_graph(
        io=DataJobInputOutputClass(
            inputDatasets=[],
            outputDatasets=[QUEUE_URN, remote_queue, remote_topic],
        )
    )
    tracker = JobEdgeTracker(
        report=SqsSourceReport(),
        graph=graph,
        instance=ACCOUNT,
        region=REGION,
        platform="sqs",
    )
    tracker.record(RULE_JOB, outputs=[QUEUE_URN])
    removed = job_removals(list(tracker.stale_workunits([])))
    # The sns source owns the topic edge; the live queue edge stays.
    assert removed == {removed_edge(RULE_JOB, "outputDatasetEdges", remote_queue)}


def test_unreadable_pipe_field_edges_are_kept() -> None:
    # The job's lineage was read once for its edges, then the field read failed;
    # the gone table edge is still removed and no field edge is.
    remote_topic = (
        f"urn:li:dataset:(urn:li:dataPlatform:sns,210987654321.{REGION}.refunds,PROD)"
    )
    io = DataJobInputOutputClass(
        inputDatasets=[], outputDatasets=[TOPIC_URN, remote_topic]
    )
    reads = iter([io])

    def get_aspect(urn: str, aspect: Any) -> DataJobInputOutputClass:
        try:
            return next(reads)
        except StopIteration:
            raise GraphError("GMS unavailable") from None

    graph = stale_graph()
    graph.get_aspect.side_effect = get_aspect
    report = SqsSourceReport()
    tracker = JobEdgeTracker(
        report=report, graph=graph, instance=ACCOUNT, region=REGION, platform="sns"
    )
    tracker.record(PIPE_JOB, outputs=[TOPIC_URN])
    assert job_removals(list(tracker.stale_workunits([]))) == {
        removed_edge(PIPE_JOB, "outputDatasetEdges", remote_topic)
    }
    assert [w.title for w in report.warnings] == ["Stale lineage not checked"]


def test_platform_instance_datasets_are_not_treated_as_remote() -> None:
    # With a platform_instance, dataset names start with "<instance>.<region>.",
    # so an own queue this run didn't link stays out of the remote pass.
    def queue(instance: str, name: str) -> str:
        return (
            f"urn:li:dataset:(urn:li:dataPlatform:sqs,{instance}.{REGION}.{name},PROD)"
        )

    rule_job = RULE_JOB.replace(f"{ACCOUNT}.", "prod.")
    remote = queue("210987654321", "refunds")
    graph = stale_graph(
        io=DataJobInputOutputClass(
            inputDatasets=[],
            outputDatasets=[queue("prod", "orders"), queue("prod", "gone"), remote],
        )
    )
    tracker = JobEdgeTracker(
        report=SqsSourceReport(),
        graph=graph,
        instance="prod",
        region=REGION,
        platform="sqs",
    )
    tracker.record(rule_job, outputs=[queue("prod", "orders")])
    assert job_removals(list(tracker.stale_workunits([]))) == {
        removed_edge(rule_job, "outputDatasetEdges", remote)
    }


@pytest.mark.parametrize("async_destinations", [False, True])
def test_unread_lambda_outputs_keep_produces_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch, async_destinations: bool
) -> None:
    # Without async destinations, or when reading them fails, the function's
    # outputs aren't fully known, so earlier output edges stay.
    create_queue("orders")
    if async_destinations:
        monkeypatch.setattr(
            LambdaClient, "list_functions", lambda self, on_error: throttled(on_error)
        )
    job = lambda_job("worker")
    source = SqsSource.create(
        base_config(lineage={"lambda_async_destinations": async_destinations}),
        PipelineContext(
            run_id="lineage", graph=stale_graph((job, "Produces", QUEUE_URN))
        ),
    )
    assert job_removals(list(source.get_workunits())) == set()


def test_async_destinations_need_lambda_functions() -> None:
    with pytest.raises(ValueError, match="lambda_async_destinations"):
        SqsSourceConfig.model_validate(
            base_config(
                lineage={"lambda_functions": False, "lambda_async_destinations": True}
            )
        )
