import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock

import pytest
import time_machine
from botocore.exceptions import ClientError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws_messaging.clients import SqsClient
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DomainsClass,
    MetadataChangeProposalClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.testing import mce_helpers
from tests.unit.aws_messaging.conftest import ACCOUNT, REGION, base_config, sqs_client

GOLDEN_DIR = Path(__file__).parent / "golden"
FROZEN_TIME = "2026-10-28 12:00:00"


def queue_urn(name: str, instance: str = ACCOUNT, region: str = REGION) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:sqs,{instance}.{region}.{name},PROD)"


def create_queue(name: str, **kwargs: Any) -> str:
    sqs = sqs_client()
    url = sqs.create_queue(QueueName=name, **kwargs)["QueueUrl"]
    return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]


def redrive(dlq_arn: str) -> Dict[str, str]:
    return {
        "RedrivePolicy": json.dumps(
            {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}
        )
    }


def upstreams(source: Any) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    for workunit in source.get_workunits():
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "upstreamLineage"
        ):
            assert metadata.entityUrn is not None and metadata.aspect is not None
            result.setdefault(metadata.entityUrn, set()).update(
                op["value"]["dataset"] for op in json.loads(metadata.aspect.value)
            )
    return result


def _run(**overrides: Any) -> SqsSource:
    return SqsSource.create(
        base_config(**overrides), PipelineContext(run_id="sqs-test")
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_golden(aws: None, pytestconfig: pytest.Config, tmp_path: Path) -> None:
    dlq_arn = create_queue("orders-dlq")
    create_queue(
        "orders",
        Attributes={**redrive(dlq_arn), "VisibilityTimeout": "60"},
        tags={"team": "payments", "pii": ""},
    )
    create_queue(
        "events.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )
    create_queue("tmp-scratch")

    source = _run(queue_pattern={"deny": ["tmp-.*"]})
    output = tmp_path / "sqs_mces.json"
    write_metadata_file(output, [wu.metadata for wu in source.get_workunits()])
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=GOLDEN_DIR / "sqs_mces_golden.json",
    )
    report = source.get_report()
    assert report.account_id == ACCOUNT
    assert report.queues_scanned == 3
    assert list(report.filtered_queues) == ["tmp-scratch"]
    assert report.lineage_edges_emitted == 1


def test_dead_letter_edge_needs_the_dlq_ingested(aws: None) -> None:
    dlq_arn = create_queue("tmp-dlq")
    create_queue("orders", Attributes=redrive(dlq_arn))
    source = _run(queue_pattern={"deny": ["tmp-.*"]})
    assert upstreams(source) == {}
    assert len(source.get_report().lineage_edges_skipped) == 1


def test_dead_letter_edge(aws: None) -> None:
    dlq_arn = create_queue("orders-dlq")
    create_queue("orders", Attributes=redrive(dlq_arn))
    assert upstreams(_run()) == {queue_urn("orders-dlq"): {queue_urn("orders")}}


def test_dead_letter_queue_still_gets_status(aws: None) -> None:
    # The DLQ's lineage patch must not hide it from the status processor.
    dlq_arn = create_queue("orders-dlq")
    create_queue("orders", Attributes=redrive(dlq_arn))
    with_status = {
        wu.get_urn()
        for wu in _run().get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, StatusClass)
        or isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "status"
    }
    assert {queue_urn("orders"), queue_urn("orders-dlq")} <= with_status


def test_explicit_platform_instance_wins(aws: None) -> None:
    create_queue("orders")
    urns = {wu.get_urn() for wu in _run(platform_instance="prod").get_workunits()}
    assert queue_urn("orders", instance="prod") in urns


def test_failed_account_probe_without_platform_instance_aborts(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Carrying on would write every queue under a URN without the account id,
    # duplicating the catalog.
    create_queue("orders")

    def throttled(self: SqsClient) -> Optional[str]:
        raise ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "x"}}, "ListQueues"
        )

    monkeypatch.setattr(SqsClient, "first_queue_url", throttled)
    with pytest.raises(RuntimeError):
        _run()
    source = _run(platform_instance="prod")
    assert source.get_report().warnings
    urns = {wu.get_urn() for wu in source.get_workunits_internal()}
    assert queue_urn("orders", instance="prod") in urns


def _run_with_graph(graph: MagicMock, **overrides: Any) -> SqsSource:
    return SqsSource.create(
        base_config(**overrides), PipelineContext(run_id="sqs-test", graph=graph)
    )


def lineage_ops(source: SqsSource) -> Dict[str, List[Any]]:
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


def stale_graph(downstream: str, upstream: str, relationship: str) -> MagicMock:
    graph = MagicMock()
    graph.get_related_entities.side_effect = lambda urn, types, direction: (
        [MagicMock(urn=downstream)]
        if urn == upstream and "DownstreamOf" in types
        else []
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


def test_removed_redrive_policy_retracts_the_edge(aws: None) -> None:
    create_queue("orders-dlq")
    create_queue("orders")
    graph = stale_graph(queue_urn("orders-dlq"), queue_urn("orders"), "dead_letter")
    source = _run_with_graph(graph)
    assert lineage_ops(source) == {
        queue_urn("orders-dlq"): [("remove", f"/upstreams/{queue_urn('orders')}")]
    }
    assert source.get_report().lineage_edges_removed == 1


def test_edges_from_other_sources_are_not_retracted(aws: None) -> None:
    # A subscription edge belongs to the sns source, so sqs leaves it alone.
    create_queue("orders-dlq")
    create_queue("orders")
    graph = stale_graph(queue_urn("orders-dlq"), queue_urn("orders"), "subscription")
    assert lineage_ops(_run_with_graph(graph)) == {}


def test_unreadable_queue_keeps_its_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_queue("orders-dlq")
    create_queue("orders")
    monkeypatch.setattr(SqsClient, "get_attributes", lambda self, url: None)
    graph = stale_graph(queue_urn("orders-dlq"), queue_urn("orders"), "dead_letter")
    assert lineage_ops(_run_with_graph(graph)) == {}
    graph.get_aspect.assert_not_called()


def test_current_edge_is_not_retracted(aws: None) -> None:
    dlq_arn = create_queue("orders-dlq")
    create_queue("orders", Attributes=redrive(dlq_arn))
    graph = stale_graph(queue_urn("orders-dlq"), queue_urn("orders"), "dead_letter")
    ops = lineage_ops(_run_with_graph(graph))
    assert ops == {
        queue_urn("orders-dlq"): [("add", f"/upstreams/{queue_urn('orders')}")]
    }


def aspect_names(source: SqsSource) -> List[str]:
    return [
        str(wu.metadata.aspectName)
        for wu in source.get_workunits()
        if isinstance(
            wu.metadata, (MetadataChangeProposalWrapper, MetadataChangeProposalClass)
        )
    ]


def tag_patches(source: SqsSource) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    for workunit in source.get_workunits():
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "globalTags"
        ):
            assert metadata.changeType == "PATCH"
            assert metadata.entityUrn is not None and metadata.aspect is not None
            result[metadata.entityUrn] = {
                op["value"]["tag"] for op in json.loads(metadata.aspect.value)["patch"]
            }
    return result


def test_tags_off(aws: None) -> None:
    create_queue("orders", tags={"team": "payments"})
    assert "globalTags" not in aspect_names(_run(extract_tags=False))


def test_tags_are_patched_from_the_tagging_api(aws: None) -> None:
    create_queue("orders", tags={"team": "payments", "pii": ""})
    assert tag_patches(_run()) == {
        queue_urn("orders"): {"urn:li:tag:team:payments", "urn:li:tag:pii"}
    }


def test_tags_fall_back_to_per_queue_calls(aws: None) -> None:
    create_queue("orders", tags={"team": "payments"})
    source = _run()
    denied = MagicMock()
    denied.get_paginator.return_value.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "GetResources"
    )
    source.tagging._client = denied
    assert tag_patches(source) == {queue_urn("orders"): {"urn:li:tag:team:payments"}}
    assert source.get_report().tags_read_per_resource


def test_domain(aws: None) -> None:
    create_queue("orders")
    create_queue("audit")
    source = _run(domain={"urn:li:domain:payments": {"allow": ["orders"]}})
    domains = {
        wu.get_urn(): wu.metadata.aspect.domains
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DomainsClass)
    }
    assert domains == {queue_urn("orders"): ["urn:li:domain:payments"]}


def test_test_connection(aws: None) -> None:
    report = SqsSource.test_connection(base_config())
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_unreadable_queue_is_kept_out_of_stale_removal(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_queue("orders")
    create_queue("flaky")
    original = SqsClient.get_attributes

    def flaky_attributes(self: SqsClient, url: str) -> Optional[Dict[str, str]]:
        # Throttled for one queue: the client already reported a warning.
        return None if url.endswith("/flaky") else original(self, url)

    monkeypatch.setattr(SqsClient, "get_attributes", flaky_attributes)
    source = _run()
    flaky = queue_urn("flaky")
    aspects = [
        (wu.get_urn(), str(wu.metadata.aspectName), wu.is_primary_source)
        for wu in source.get_workunits_internal()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]
    # Only Status, so nothing an earlier run stored is overwritten, and primary,
    # so stale-entity removal counts the queue as still present.
    assert [a for a in aspects if a[0] == flaky] == [(flaky, "status", True)]
    assert list(source.get_report().queues_failed) == ["flaky"]


def test_account_id_comes_from_the_queue_url(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Even with GetQueueAttributes denied everywhere, the account (and so every
    # URN) stays the same as on runs where it succeeds.
    create_queue("orders")
    monkeypatch.setattr(SqsClient, "get_attributes", lambda self, url: None)
    source = _run()
    assert source.get_report().account_id == ACCOUNT
    datasets = {
        wu.get_urn()
        for wu in source.get_workunits_internal()
        if wu.get_urn().startswith("urn:li:dataset:")
    }
    assert datasets == {queue_urn("orders")}


def test_all_unreadable_queues_still_emit_the_region_container(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_queue("orders")
    monkeypatch.setattr(SqsClient, "get_attributes", lambda self, url: None)
    urns = {wu.get_urn() for wu in _run().get_workunits_internal()}
    assert any(urn.startswith("urn:li:container:") for urn in urns)


def test_edge_to_an_unreadable_dead_letter_queue_is_kept(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    dlq_arn = create_queue("orders-dlq")
    create_queue("orders", Attributes=redrive(dlq_arn))
    original = SqsClient.get_attributes

    def flaky_dlq(self: SqsClient, url: str) -> Optional[Dict[str, str]]:
        return None if url.endswith("/orders-dlq") else original(self, url)

    monkeypatch.setattr(SqsClient, "get_attributes", flaky_dlq)
    assert upstreams(_run()) == {queue_urn("orders-dlq"): {queue_urn("orders")}}
