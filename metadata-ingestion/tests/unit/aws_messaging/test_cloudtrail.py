import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from datahub.configuration.common import GraphError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder
from datahub.ingestion.source.aws_messaging.clients import (
    CloudTrailLakeClient,
    SnsClient,
)
from datahub.ingestion.source.aws_messaging.cloudtrail import ProducerLineage
from datahub.ingestion.source.aws_messaging.config import SqsSourceConfig
from datahub.ingestion.source.aws_messaging.constants import AwsService
from datahub.ingestion.source.aws_messaging.models import SnsSubscription
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    MetadataChangeProposalClass,
)
from tests.unit.aws_messaging.conftest import (
    ACCOUNT,
    REGION,
    base_config,
    sns_client,
    sqs_client,
)
from tests.unit.aws_messaging.test_lambda_eventbridge import (
    create_function,
    job_edges,
    job_removals,
    lambda_job,
)

STORE = f"arn:aws:cloudtrail:{REGION}:{ACCOUNT}:eventdatastore/abc-123"
QUERY_ID = "00000000-0000-0000-0000-000000000001"
QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/orders"
QUEUE_URN = f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.orders,PROD)"


def test_config_needs_event_data_store() -> None:
    with pytest.raises(ValueError):
        SqsSourceConfig.model_validate(
            base_config(cloudtrail_producers={"enabled": True})
        )


def test_run_query_waits_and_pages() -> None:
    client = boto3.client(
        "cloudtrail",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    lake = CloudTrailLakeClient(client, SqsSourceReport())
    with Stubber(client) as stubber:
        stubber.add_response("start_query", {"QueryId": QUERY_ID})
        stubber.add_response(
            "get_query_results",
            {
                "QueryStatus": "FINISHED",
                "QueryResultRows": [[{"producer": "a"}, {"resource": "x"}]],
                "NextToken": "page-2",
            },
            {"EventDataStore": STORE, "QueryId": QUERY_ID},
        )
        stubber.add_response(
            "get_query_results",
            {
                "QueryStatus": "FINISHED",
                "QueryResultRows": [[{"producer": "b"}, {"resource": "y"}]],
            },
            {"EventDataStore": STORE, "QueryId": QUERY_ID, "NextToken": "page-2"},
        )
        rows = lake.run_query("SELECT 1", STORE, 60)
    assert rows == [
        {"producer": "a", "resource": "x"},
        {"producer": "b", "resource": "y"},
    ]


def test_failed_query_is_a_warning() -> None:
    client = boto3.client(
        "cloudtrail",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    report = SqsSourceReport()
    with Stubber(client) as stubber:
        stubber.add_response("start_query", {"QueryId": QUERY_ID})
        stubber.add_response(
            "get_query_results",
            {"QueryStatus": "FAILED", "ErrorMessage": "syntax error"},
            {"EventDataStore": STORE, "QueryId": QUERY_ID},
        )
        assert (
            CloudTrailLakeClient(client, report).run_query("SELECT 1", STORE, 60)
            is None
        )
    assert [w.title for w in report.warnings] == ["CloudTrail Lake query failed"]


def test_query_is_cancelled_when_reading_results_fails() -> None:
    client = boto3.client(
        "cloudtrail",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    report = SqsSourceReport()
    with Stubber(client) as stubber:
        stubber.add_response("start_query", {"QueryId": QUERY_ID})
        stubber.add_client_error("get_query_results", "ThrottlingException")
        stubber.add_response(
            "cancel_query",
            {"QueryId": QUERY_ID, "QueryStatus": "CANCELLED"},
            {"EventDataStore": STORE, "QueryId": QUERY_ID},
        )
        assert (
            CloudTrailLakeClient(client, report).run_query("SELECT 1", STORE, 60)
            is None
        )
        stubber.assert_no_pending_responses()
    assert [w.title for w in report.warnings] == ["CloudTrail Lake query failed"]


def test_timed_out_query_is_cancelled() -> None:
    # A query left running keeps scanning, and Lake bills per GB scanned.
    client = boto3.client(
        "cloudtrail",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    report = SqsSourceReport()
    with Stubber(client) as stubber:
        stubber.add_response("start_query", {"QueryId": QUERY_ID})
        stubber.add_response(
            "get_query_results",
            {"QueryStatus": "RUNNING"},
            {"EventDataStore": STORE, "QueryId": QUERY_ID},
        )
        stubber.add_response(
            "cancel_query",
            {"QueryId": QUERY_ID, "QueryStatus": "CANCELLED"},
            {"EventDataStore": STORE, "QueryId": QUERY_ID},
        )
        assert (
            CloudTrailLakeClient(client, report).run_query("SELECT 1", STORE, -1)
            is None
        )
        stubber.assert_no_pending_responses()
    assert [w.title for w in report.warnings] == ["CloudTrail Lake query timed out"]


def upserted_properties(workunits: List[Any]) -> List[Dict[str, str]]:
    return [
        wu.metadata.aspect.customProperties
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetPropertiesClass)
    ]


def property_patches(workunits: List[Any]) -> List[Any]:
    return [
        wu
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "datasetProperties"
    ]


def _run(
    monkeypatch: pytest.MonkeyPatch,
    rows: List[Dict[str, str]],
    store: str = STORE,
    calls: Optional[List[Dict[str, Any]]] = None,
) -> List[Any]:
    statements: List[str] = []

    def fake_run_query(
        self: CloudTrailLakeClient,
        statement: str,
        event_data_store: str,
        timeout: int,
        owner_account_id: Optional[str] = None,
    ) -> Optional[List[Dict[str, str]]]:
        statements.append(statement)
        if calls is not None:
            calls.append(
                {"region": self._client.meta.region_name, "owner": owner_account_id}
            )
        return rows

    monkeypatch.setattr(CloudTrailLakeClient, "run_query", fake_run_query)
    source = SqsSource.create(
        base_config(
            cloudtrail_producers={"enabled": True, "event_data_store": store},
            lineage={"lambda_async_destinations": False},
        ),
        PipelineContext(run_id="cloudtrail-test"),
    )
    workunits = list(source.get_workunits())
    assert f"FROM {store.rsplit('/', 1)[-1]}" in statements[0]
    assert "'SendMessage', 'SendMessageBatch'" in statements[0]
    assert f"awsRegion = '{REGION}'" in statements[0]
    assert "errorCode IS NULL" in statements[0]
    return workunits


def test_lambda_producer_becomes_an_upstream_job(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqs_client().create_queue(QueueName="orders")
    create_function("checkout")
    workunits = _run(
        monkeypatch,
        [
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/checkout-role/checkout",
                "resource": QUEUE_URL,
            },
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/ecs-task/abc123",
                "resource": QUEUE_URL,
            },
            # Named like the function, but not its execution role.
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/other-role/checkout",
                "resource": QUEUE_URL,
            },
            # A malformed URL is skipped, not fatal.
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/ecs-task/abc123",
                "resource": f"https://[sqs.{REGION}.amazonaws.com/{ACCOUNT}/orders",
            },
            # Same name and account, other region: not this run's queue.
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/eu-role/x",
                "resource": f"https://sqs.eu-west-1.amazonaws.com/{ACCOUNT}/orders",
            },
            # SendMessage also takes the queue ARN as queueUrl.
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/arn-sender/y",
                "resource": f"arn:aws:sqs:{REGION}:{ACCOUNT}:orders",
            },
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/eu-arn-sender/z",
                "resource": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:orders",
            },
            # A queue this run didn't ingest is ignored.
            {
                "producer": f"arn:aws:iam::{ACCOUNT}:user/someone",
                "resource": f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/other",
            },
        ],
    )
    assert job_edges(workunits) == {
        lambda_job("checkout"): {"outputDatasetEdges": {QUEUE_URN}}
    }
    # The query runs before the queue is written, so the full write holds the
    # value and nothing is patched afterwards.
    assert upserted_properties(workunits)[0]["observed_producers"] == (
        f"arn:aws:iam::{ACCOUNT}:role/arn-sender, arn:aws:iam::{ACCOUNT}:role/ecs-task, "
        f"arn:aws:iam::{ACCOUNT}:role/other-role"
    )
    assert not property_patches(workunits)


def test_query_runs_in_the_event_data_stores_region_and_account(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqs_client().create_queue(QueueName="orders")
    calls: List[Dict[str, Any]] = []
    _run(
        monkeypatch,
        [],
        store="arn:aws:cloudtrail:eu-west-1:210987654321:eventdatastore/shared-123",
        calls=calls,
    )
    assert calls == [{"region": "eu-west-1", "owner": "210987654321"}]


def test_query_with_a_bare_store_id_uses_the_recipe_region(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqs_client().create_queue(QueueName="orders")
    calls: List[Dict[str, Any]] = []
    _run(monkeypatch, [], store="abc-123", calls=calls)
    assert calls == [{"region": REGION, "owner": None}]


def test_owner_account_is_omitted_when_this_account_is_unknown(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No queues, so the account id can't be read from a resource ARN.
    calls: List[Dict[str, Any]] = []
    _run(
        monkeypatch,
        [],
        store="arn:aws:cloudtrail:eu-west-1:210987654321:eventdatastore/shared-123",
        calls=calls,
    )
    assert calls == [{"region": "eu-west-1", "owner": None}]


def test_failed_query_keeps_previous_observed_producers(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqs_client().create_queue(QueueName="orders")
    monkeypatch.setattr(
        CloudTrailLakeClient, "run_query", lambda self, *args, **kwargs: None
    )
    previous = f"arn:aws:iam::{ACCOUNT}:role/ecs-task"
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    graph.get_aspect.side_effect = lambda urn, aspect: (
        DatasetPropertiesClass(customProperties={"observed_producers": previous})
        if urn == QUEUE_URN
        else None
    )
    source = SqsSource.create(
        base_config(
            cloudtrail_producers={"enabled": True, "event_data_store": STORE},
            lineage={"lambda_functions": False},
        ),
        PipelineContext(run_id="cloudtrail-test", graph=graph),
    )
    workunits = list(source.get_workunits())
    # Read before this run's datasetProperties are written and folded into them,
    # so nothing has to be patched back and nothing can race the upsert.
    assert upserted_properties(workunits)[0]["observed_producers"] == previous
    assert not property_patches(workunits)


def test_failed_previous_value_read_does_not_abort(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqs_client().create_queue(QueueName="orders")
    monkeypatch.setattr(
        CloudTrailLakeClient, "run_query", lambda self, *args, **kwargs: None
    )
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    graph.get_aspect.side_effect = GraphError("GMS unavailable")
    source = SqsSource.create(
        base_config(
            cloudtrail_producers={"enabled": True, "event_data_store": STORE},
            lineage={"lambda_functions": False},
        ),
        PipelineContext(run_id="cloudtrail-test", graph=graph),
    )
    workunits = list(source.get_workunits())
    assert "observed_producers" not in upserted_properties(workunits)[0]
    assert "Previous observed producers not read" in [
        w.title for w in source.get_report().warnings
    ]


@pytest.mark.parametrize(
    "rows, expected",
    [
        ([], None),
        (
            [
                {
                    "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/new-task/abc",
                    "resource": QUEUE_URL,
                }
            ],
            f"arn:aws:iam::{ACCOUNT}:role/new-task",
        ),
    ],
)
def test_successful_query_replaces_the_previous_value(
    aws: None,
    monkeypatch: pytest.MonkeyPatch,
    rows: List[Dict[str, str]],
    expected: Optional[str],
) -> None:
    # The value DataHub holds is never copied into the write after a successful
    # query, so a role that stopped sending can't survive a dropped patch.
    sqs_client().create_queue(QueueName="orders")
    monkeypatch.setattr(
        CloudTrailLakeClient, "run_query", lambda self, *args, **kwargs: rows
    )
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    graph.get_related_entities.return_value = []
    graph.get_aspect.side_effect = lambda urn, aspect: (
        DatasetPropertiesClass(
            customProperties={
                "observed_producers": f"arn:aws:iam::{ACCOUNT}:role/old-task"
            }
        )
        if urn == QUEUE_URN and aspect is DatasetPropertiesClass
        else None
    )
    source = SqsSource.create(
        base_config(
            cloudtrail_producers={"enabled": True, "event_data_store": STORE},
            lineage={"lambda_functions": False},
        ),
        PipelineContext(run_id="cloudtrail-test", graph=graph),
    )
    workunits = list(source.get_workunits())
    assert upserted_properties(workunits)[0].get("observed_producers") == expected
    assert not property_patches(workunits)


@pytest.mark.parametrize("query_succeeds", [True, False])
def test_lambda_producer_edges_are_kept_when_the_query_fails(
    aws: None, monkeypatch: pytest.MonkeyPatch, query_succeeds: bool
) -> None:
    sqs_client().create_queue(QueueName="orders")
    create_function("checkout")
    monkeypatch.setattr(
        CloudTrailLakeClient,
        "run_query",
        lambda self, *args, **kwargs: [] if query_succeeds else None,
    )
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    graph.get_aspect.return_value = None
    graph.get_related_entities.side_effect = lambda urn, types, direction: (
        [MagicMock(urn=lambda_job("checkout"), relationship_type="Produces")]
        if urn == QUEUE_URN and "Produces" in types
        else []
    )
    source = SqsSource.create(
        base_config(
            cloudtrail_producers={"enabled": True, "event_data_store": STORE},
            # Lambda output edges are only stale-removed with this on.
            lineage={"lambda_async_destinations": True},
        ),
        PipelineContext(run_id="cloudtrail-test", graph=graph),
    )
    removed = job_removals(list(source.get_workunits()))
    # A successful query that no longer sees the function sending removes the
    # edge; a failed one says nothing about it.
    assert bool(removed) == query_succeeds


@pytest.mark.parametrize(
    "store",
    [
        "abc-123",
        STORE,
        "arn:aws-cn:cloudtrail:cn-north-1:123456789012:eventdatastore/x-1",
        "arn:aws-eusc:cloudtrail:eusc-de-east-1:123456789012:eventdatastore/x-1",
    ],
)
def test_event_data_store_formats_accepted(store: str) -> None:
    SqsSourceConfig.model_validate(
        base_config(cloudtrail_producers={"enabled": True, "event_data_store": store})
    )


@pytest.mark.parametrize(
    "store",
    ["abc' OR '1'='1", "eds; DROP", f"{STORE} --", "arn:aws:s3:::bucket", "abc-123\n"],
)
def test_event_data_store_that_could_change_the_query_is_rejected(store: str) -> None:
    with pytest.raises(ValueError):
        SqsSourceConfig.model_validate(
            base_config(
                cloudtrail_producers={"enabled": True, "event_data_store": store}
            )
        )


def test_statement_rejects_a_region_that_is_not_an_aws_region() -> None:
    config = SqsSourceConfig.model_validate(
        base_config(cloudtrail_producers={"enabled": True, "event_data_store": STORE})
    )
    urns = UrnBuilder(env="PROD", account_id=ACCOUNT, platform_instance=ACCOUNT)
    producers = ProducerLineage(
        config=config.cloudtrail_producers,
        report=SqsSourceReport(),
        client=MagicMock(),
        urns=urns,
        resolver=ResourceResolver(
            urns=urns, region="us-east-1' OR '1'='1", service=AwsService.SQS
        ),
        lambdas=None,
    )
    with pytest.raises(ValueError):
        producers.statement(datetime(2026, 10, 28, tzinfo=timezone.utc))


def test_patched_topic_clears_observed_producers_after_a_successful_query(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A partial subscription listing writes topic properties as a patch; a
    # successful query that saw no role senders must still clear the old value.
    sns_client().create_topic(Name="order-events")

    def truncated(
        self: SnsClient, topic_arn: str, on_error: Callable[[ClientError], None]
    ) -> Iterator[SnsSubscription]:
        on_error(ClientError({"Error": {"Code": "Throttling", "Message": "x"}}, "List"))
        yield from ()

    monkeypatch.setattr(SnsClient, "list_subscriptions", truncated)
    monkeypatch.setattr(
        CloudTrailLakeClient, "run_query", lambda self, *args, **kwargs: []
    )
    source = SnsSource.create(
        base_config(
            cloudtrail_producers={"enabled": True, "event_data_store": STORE},
            lineage={"lambda_functions": False},
        ),
        PipelineContext(run_id="cloudtrail-test"),
    )
    operations = [
        (op["op"], op["path"])
        for wu in property_patches(list(source.get_workunits()))
        for op in json.loads(wu.metadata.aspect.value)
    ]
    assert ("remove", "/customProperties/observed_producers") in operations


def test_statement_accepts_a_sovereign_cloud_region() -> None:
    config = SqsSourceConfig.model_validate(
        base_config(cloudtrail_producers={"enabled": True, "event_data_store": STORE})
    )
    urns = UrnBuilder(env="PROD", account_id=ACCOUNT, platform_instance=ACCOUNT)
    producers = ProducerLineage(
        config=config.cloudtrail_producers,
        report=SqsSourceReport(),
        client=MagicMock(),
        urns=urns,
        resolver=ResourceResolver(
            urns=urns, region="eusc-de-east-1", service=AwsService.SQS
        ),
        lambdas=None,
    )
    statement = producers.statement(datetime(2026, 10, 28, tzinfo=timezone.utc))
    assert "awsRegion = 'eusc-de-east-1'" in statement


def test_topic_producers_from_publish_events(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    topic_arn = sns_client().create_topic(Name="order-events")["TopicArn"]
    topic_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sns,{ACCOUNT}.{REGION}.order-events,PROD)"
    )
    create_function("checkout")
    statements: List[str] = []

    def fake_run_query(
        self: CloudTrailLakeClient, statement: str, *args: Any, **kwargs: Any
    ) -> List[Dict[str, str]]:
        statements.append(statement)
        return [
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/checkout-role/checkout",
                "resource": topic_arn,
            },
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/ecs-task/abc123",
                "resource": topic_arn,
            },
            # Same name, other region.
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/eu-role/x",
                "resource": f"arn:aws:sns:eu-west-1:{ACCOUNT}:order-events",
            },
        ]

    monkeypatch.setattr(CloudTrailLakeClient, "run_query", fake_run_query)
    source = SnsSource.create(
        base_config(
            cloudtrail_producers={"enabled": True, "event_data_store": STORE},
            lineage={"lambda_async_destinations": False},
        ),
        PipelineContext(run_id="cloudtrail-test"),
    )
    workunits = list(source.get_workunits())
    assert "eventSource = 'sns.amazonaws.com'" in statements[0]
    assert "'Publish', 'PublishBatch'" in statements[0]
    assert "element_at(requestParameters, 'topicArn')" in statements[0]
    assert job_edges(workunits) == {
        lambda_job("checkout"): {"outputDatasetEdges": {topic_urn}}
    }
    assert upserted_properties(workunits)[0]["observed_producers"] == (
        f"arn:aws:iam::{ACCOUNT}:role/ecs-task"
    )
    assert source.get_report().producer_lambda_edges == 1
