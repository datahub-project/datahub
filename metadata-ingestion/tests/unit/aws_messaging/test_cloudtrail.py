import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.stub import Stubber

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder
from datahub.ingestion.source.aws_messaging.clients import CloudTrailLakeClient
from datahub.ingestion.source.aws_messaging.cloudtrail import ProducerLineage
from datahub.ingestion.source.aws_messaging.config import SqsSourceConfig
from datahub.ingestion.source.aws_messaging.constants import AwsService
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    MetadataChangeProposalClass,
)
from tests.unit.aws_messaging.conftest import ACCOUNT, REGION, base_config, sqs_client
from tests.unit.aws_messaging.test_lambda_eventbridge import (
    create_function,
    job_edges,
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
    assert report.warnings


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
            # Same name and account, other region: not this run's queue.
            {
                "producer": f"arn:aws:sts::{ACCOUNT}:assumed-role/eu-role/x",
                "resource": f"https://sqs.eu-west-1.amazonaws.com/{ACCOUNT}/orders",
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
    properties = [
        json.loads(wu.metadata.aspect.value)
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "datasetProperties"
        and wu.metadata.aspect is not None
    ]
    assert properties == [
        [
            {
                "op": "add",
                "path": "/customProperties/observed_producers",
                "value": f"arn:aws:iam::{ACCOUNT}:role/ecs-task",
            }
        ]
    ]


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
    upserted = [
        wu.metadata.aspect.customProperties
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetPropertiesClass)
    ]
    # Read before this run's datasetProperties are written and folded into them,
    # so nothing has to be patched back and nothing can race the upsert.
    assert upserted[0]["observed_producers"] == previous
    assert not [
        wu
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "datasetProperties"
    ]


def test_successful_query_with_no_producers_clears_the_previous_value(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The query succeeded and saw nobody send to the queue, so the value
    # carried into this run's datasetProperties is removed afterwards.
    sqs_client().create_queue(QueueName="orders")
    monkeypatch.setattr(
        CloudTrailLakeClient, "run_query", lambda self, *args, **kwargs: []
    )
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    graph.get_related_entities.return_value = []
    graph.get_aspect.side_effect = lambda urn, aspect: (
        DatasetPropertiesClass(
            customProperties={
                "observed_producers": f"arn:aws:iam::{ACCOUNT}:role/ecs-task"
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
    patches = [
        json.loads(wu.metadata.aspect.value)
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "datasetProperties"
        and wu.metadata.aspect is not None
    ]
    assert [(op["op"], op["path"]) for patch in patches for op in patch] == [
        ("remove", "/customProperties/observed_producers")
    ]


@pytest.mark.parametrize(
    "store",
    [
        "abc-123",
        STORE,
        "arn:aws-cn:cloudtrail:cn-north-1:123456789012:eventdatastore/x-1",
    ],
)
def test_event_data_store_formats_accepted(store: str) -> None:
    SqsSourceConfig.model_validate(
        base_config(cloudtrail_producers={"enabled": True, "event_data_store": store})
    )


@pytest.mark.parametrize(
    "store", ["abc' OR '1'='1", "eds; DROP", f"{STORE} --", "arn:aws:s3:::bucket"]
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
