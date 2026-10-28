import json
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.metadata.schema_classes import MetadataChangeProposalClass
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

    source = _run(topic_pattern={"deny": ["tmp-.*"]})
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
