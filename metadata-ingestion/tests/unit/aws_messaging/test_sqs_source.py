import json
from pathlib import Path
from typing import Any, Dict, Set

import pytest
import time_machine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import MetadataChangeProposalClass
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


def test_lineage_patches_are_not_registered_for_stale_removal(aws: None) -> None:
    dlq_arn = create_queue("orders-dlq")
    create_queue("orders", Attributes=redrive(dlq_arn))
    for workunit in _run().get_workunits_internal():
        if isinstance(workunit.metadata, MetadataChangeProposalClass):
            assert not workunit.is_primary_source


def test_explicit_platform_instance_wins(aws: None) -> None:
    create_queue("orders")
    urns = {wu.get_urn() for wu in _run(platform_instance="prod").get_workunits()}
    assert queue_urn("orders", instance="prod") in urns


def test_tags_off(aws: None) -> None:
    create_queue("orders", tags={"team": "payments"})
    aspects = [
        wu.metadata.aspectName
        for wu in _run(extract_tags=False).get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]
    assert "globalTags" not in aspects


def test_test_connection(aws: None) -> None:
    report = SqsSource.test_connection(base_config())
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
