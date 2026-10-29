import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Set
from unittest.mock import MagicMock

import pytest
import time_machine
from botocore.exceptions import ClientError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws_messaging.clients import SnsClient
from datahub.ingestion.source.aws_messaging.models import SnsSubscription
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)
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


def urn(platform: str, name: str, instance: str = ACCOUNT, region: str = REGION) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{instance}.{region}.{name},PROD)"


def create_queue(name: str) -> str:
    sqs = sqs_client()
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]


def patches(
    workunits: List[MetadataWorkUnit], aspect: str
) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    for workunit in workunits:
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == aspect
        ):
            assert metadata.entityUrn is not None and metadata.aspect is not None
            body = json.loads(metadata.aspect.value)
            ops = body["patch"] if isinstance(body, dict) else body
            result.setdefault(metadata.entityUrn, []).extend(op["value"] for op in ops)
    return result


def upstreams(source: SnsSource) -> Dict[str, Set[str]]:
    return {
        down: {value["dataset"] for value in values}
        for down, values in patches(
            list(source.get_workunits()), "upstreamLineage"
        ).items()
    }


def _run(**overrides: Any) -> SnsSource:
    return SnsSource.create(
        base_config(**overrides), PipelineContext(run_id="sns-test")
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_golden(aws: None, pytestconfig: pytest.Config, tmp_path: Path) -> None:
    orders = create_queue("orders")
    dlq = create_queue("orders-dlq")
    sns = sns_client()
    topic = sns.create_topic(
        Name="order-events",
        Attributes={"DisplayName": "Orders"},
        Tags=[{"Key": "team", "Value": "payments"}],
    )["TopicArn"]
    sns.create_topic(Name="alerts")
    sns.create_topic(Name="tmp-scratch")
    sns.subscribe(
        TopicArn=topic,
        Protocol="sqs",
        Endpoint=orders,
        Attributes={"RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq})},
    )
    sns.subscribe(TopicArn=topic, Protocol="email", Endpoint="someone@example.com")
    sns.subscribe(
        TopicArn=topic,
        Protocol="https",
        Endpoint="https://hooks.example.com/notify?token=secret",
    )

    source = _run(
        topic_pattern={"deny": ["tmp-.*"]}, include_subscription_attributes=True
    )
    output = tmp_path / "sns_mces.json"
    write_metadata_file(output, [wu.metadata for wu in source.get_workunits()])
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=GOLDEN_DIR / "sns_mces_golden.json",
    )
    report = source.get_report()
    assert (report.topics_scanned, report.lineage_edges_emitted) == (2, 2)
    golden = (GOLDEN_DIR / "sns_mces_golden.json").read_text()
    assert "someone@example.com" not in golden
    assert "token=secret" not in golden


def test_subscription_to_missing_local_queue_is_skipped(aws: None) -> None:
    topic = sns_client().create_topic(Name="order-events")["TopicArn"]
    sns_client().subscribe(
        TopicArn=topic,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:{REGION}:{ACCOUNT}:gone",
    )
    source = _run()
    assert upstreams(source) == {}
    assert len(source.get_report().lineage_edges_skipped) == 1


def test_cross_account_queue_uses_mapped_instance(aws: None) -> None:
    topic = sns_client().create_topic(Name="order-events")["TopicArn"]
    sns_client().subscribe(
        TopicArn=topic,
        Protocol="sqs",
        Endpoint="arn:aws:sqs:eu-west-1:210987654321:remote",
    )
    source = _run(account_platform_instance_map={"210987654321": "shared"})
    assert upstreams(source) == {
        urn("sqs", "remote", instance="shared", region="eu-west-1"): {
            urn("sns", "order-events")
        }
    }


def test_data_protection_tags_and_mapped_terms(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sns_client().create_topic(Name="order-events")
    source = _run(
        data_protection={
            "enabled": True,
            "glossary_term_map": {"EmailAddress": "urn:li:glossaryTerm:pii.email"},
        }
    )
    policy = {
        "Name": "p",
        "Version": "2021-06-01",
        "Statement": [
            {
                "DataIdentifier": [
                    "arn:aws:dataprotection::aws:data-identifier/EmailAddress",
                    "arn:aws:dataprotection::aws:data-identifier/CreditCardNumber",
                ],
                "Operation": {"Deny": {}},
                "Principal": ["*"],
                "DataDirection": "Inbound",
            }
        ],
    }
    # moto does not implement GetDataProtectionPolicy.
    monkeypatch.setattr(
        source.sns._client,
        "get_data_protection_policy",
        lambda ResourceArn: {"DataProtectionPolicy": json.dumps(policy)},
    )
    workunits = list(source.get_workunits())
    tags = patches(workunits, "globalTags")
    terms = patches(workunits, "glossaryTerms")
    topic = urn("sns", "order-events")
    assert {value["tag"] for value in tags[topic]} == {
        "urn:li:tag:aws_data_identifier:CreditCardNumber",
        "urn:li:tag:aws_data_identifier:EmailAddress",
    }
    assert [value["urn"] for value in terms[topic]] == ["urn:li:glossaryTerm:pii.email"]


def test_test_connection(aws: None) -> None:
    report = SnsSource.test_connection(base_config())
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_unreadable_topic_is_kept_out_of_stale_removal(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sns_client().create_topic(Name="flaky")
    monkeypatch.setattr(SnsClient, "get_topic_attributes", lambda self, arn: None)
    source = _run()
    aspects = [
        (wu.get_urn(), str(wu.metadata.aspectName), wu.is_primary_source)
        for wu in source.get_workunits_internal()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]
    assert [a for a in aspects if a[0] == urn("sns", "flaky")] == [
        (urn("sns", "flaky"), "status", True)
    ]


def test_placeholder_subscriptions_are_skipped(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    orders = create_queue("orders")
    topic = sns_client().create_topic(Name="order-events")["TopicArn"]

    def placeholders(
        self: SnsClient, topic_arn: str, on_error: Callable[[], None]
    ) -> Iterator[SnsSubscription]:
        for placeholder in ("PendingConfirmation", "Deleted"):
            yield SnsSubscription(
                subscription_arn=placeholder,
                topic_arn=topic_arn,
                protocol="sqs",
                endpoint=orders,
            )

    monkeypatch.setattr(SnsClient, "list_subscriptions", placeholders)
    assert topic
    assert upstreams(_run(include_subscription_attributes=False)) == {}


def test_all_unreadable_topics_still_emit_the_region_container(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sns_client().create_topic(Name="flaky")
    monkeypatch.setattr(SnsClient, "get_topic_attributes", lambda self, arn: None)
    urns = {wu.get_urn() for wu in _run().get_workunits_internal()}
    assert any(urn.startswith("urn:li:container:") for urn in urns)


def stale_graph(downstream: str, upstream: str, relationship: str) -> MagicMock:
    graph = MagicMock()
    graph.get_related_entities.side_effect = lambda urn, types, direction: (
        [MagicMock(urn=downstream)] if urn == upstream else []
    )
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream,
                type=DatasetLineageTypeClass.TRANSFORMED,
                properties={"relationship": relationship},
            )
        ]
    )
    return graph


def lineage_ops(source: SnsSource) -> Dict[str, List[Any]]:
    result: Dict[str, List[Any]] = {}
    for workunit in source.get_workunits_internal():
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "upstreamLineage"
        ):
            assert metadata.entityUrn is not None and metadata.aspect is not None
            result[metadata.entityUrn] = [
                (op["op"], op["path"]) for op in json.loads(metadata.aspect.value)
            ]
    return result


def _run_with_graph(graph: MagicMock, **overrides: Any) -> SnsSource:
    return SnsSource.create(
        base_config(**overrides), PipelineContext(run_id="sns-test", graph=graph)
    )


def test_removed_subscription_retracts_the_edge(aws: None) -> None:
    create_queue("orders")
    sns_client().create_topic(Name="order-events")
    topic, queue = urn("sns", "order-events"), urn("sqs", "orders")
    source = _run_with_graph(stale_graph(queue, topic, "subscription"))
    assert lineage_ops(source) == {queue: [("remove", f"/upstreams/{topic}")]}


def test_subscription_dead_letter_edge_kept_when_attributes_are_not_read(
    aws: None,
) -> None:
    # With include_subscription_attributes off, the run can't tell whether a
    # subscription still has its dead-letter queue.
    create_queue("orders-dlq")
    sns_client().create_topic(Name="order-events")
    topic, dlq = urn("sns", "order-events"), urn("sqs", "orders-dlq")
    graph = stale_graph(dlq, topic, "subscription_dead_letter")
    assert lineage_ops(_run_with_graph(graph)) == {}
    retracted = _run_with_graph(graph, include_subscription_attributes=True)
    assert lineage_ops(retracted) == {dlq: [("remove", f"/upstreams/{topic}")]}


def test_failed_subscription_listing_keeps_the_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_queue("orders")
    sns_client().create_topic(Name="order-events")
    topic, queue = urn("sns", "order-events"), urn("sqs", "orders")

    def throttled(
        self: SnsClient, topic_arn: str, on_error: Callable[[], None]
    ) -> Iterator[SnsSubscription]:
        on_error()
        yield from ()

    monkeypatch.setattr(SnsClient, "list_subscriptions", throttled)
    assert lineage_ops(_run_with_graph(stale_graph(queue, topic, "subscription"))) == {}


def test_failed_account_probe_without_platform_instance_aborts(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sns_client().create_topic(Name="order-events")

    def throttled(self: SnsClient) -> None:
        raise ClientError(
            {"Error": {"Code": "Throttling", "Message": "x"}}, "ListTopics"
        )

    monkeypatch.setattr(SnsClient, "first_topic_arn", throttled)
    with pytest.raises(RuntimeError):
        _run()
