import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.stub import Stubber

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.clients import CloudTrailLakeClient
from datahub.ingestion.source.aws_messaging.config import SqsSourceConfig
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport
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
    patches = [
        json.loads(wu.metadata.aspect.value)
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "datasetProperties"
        and wu.metadata.aspect is not None
    ]
    # Written back after the full datasetProperties upsert, so it survives.
    assert patches == [
        [
            {
                "op": "add",
                "path": "/customProperties/observed_producers",
                "value": previous,
            }
        ]
    ]
