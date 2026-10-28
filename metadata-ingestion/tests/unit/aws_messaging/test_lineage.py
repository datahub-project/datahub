import json
from typing import Dict, List

from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.config import AwsMessagingSourceConfig
from datahub.ingestion.source.aws_messaging.constants import LineageRelationship
from datahub.ingestion.source.aws_messaging.lineage import DatasetLineageCollector
from datahub.ingestion.source.aws_messaging.mapper import AwsRegionKey
from datahub.ingestion.source.aws_messaging.models import SnsSubscription, SnsTopic
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.aws_messaging.sns import SnsExtractor
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    MetadataChangeProposalClass,
)

OWN = "123456789012"
TOPIC_ARN = f"arn:aws:sns:us-east-1:{OWN}:events"


class FakeSnsClient:
    def __init__(self, attributes: Dict[str, Dict[str, str]]):
        self.attributes = attributes
        self.calls: List[str] = []

    def get_subscription_attributes(self, arn: str) -> Dict[str, str]:
        self.calls.append(arn)
        return self.attributes.get(arn, {})


def _extractor(client: FakeSnsClient, **config: object) -> SnsExtractor:
    report = AwsMessagingSourceReport()
    return SnsExtractor(
        config=AwsMessagingSourceConfig.model_validate(config),
        report=report,
        client=client,  # type: ignore[arg-type]
        urns=UrnBuilder(env="PROD", account_id=OWN, platform_instance=OWN),
        region_key=AwsRegionKey(region="us-east-1", platform="sns", instance=OWN),
    )


def _resolver() -> ResourceResolver:
    return ResourceResolver(
        urns=UrnBuilder(env="PROD", account_id=OWN, platform_instance=OWN),
        region="us-east-1",
    )


def _topic() -> SnsTopic:
    arn = parse_arn(TOPIC_ARN)
    assert arn is not None
    return SnsTopic(arn=arn, attributes={})


def _subscription(endpoint: str, n: int, protocol: str = "sqs") -> SnsSubscription:
    return SnsSubscription(
        subscription_arn=f"{TOPIC_ARN}:sub-{n}",
        topic_arn=TOPIC_ARN,
        protocol=protocol,
        endpoint=endpoint,
    )


def _upstreams(lineage: DatasetLineageCollector) -> Dict[str, Dict[str, str]]:
    edges: Dict[str, Dict[str, str]] = {}
    for workunit in lineage.workunits():
        mcp = workunit.metadata
        assert isinstance(mcp, MetadataChangeProposalClass)
        assert mcp.changeType == ChangeTypeClass.PATCH
        assert mcp.entityUrn is not None and mcp.aspect is not None
        for op in json.loads(mcp.aspect.value):
            edges.setdefault(mcp.entityUrn, {})[op["value"]["dataset"]] = op["value"][
                "properties"
            ]["relationship"]
    return edges


def test_cross_account_queue_edge_is_kept_and_unknown_local_queue_is_skipped() -> None:
    client = FakeSnsClient({})
    extractor = _extractor(client, include_subscription_attributes=False)
    extractor.subscriptions[TOPIC_ARN] = [
        _subscription("arn:aws:sqs:eu-west-1:210987654321:remote", 1),
        _subscription(f"arn:aws:sqs:us-east-1:{OWN}:missing", 2),
        SnsSubscription(
            subscription_arn="PendingConfirmation",
            topic_arn=TOPIC_ARN,
            protocol="sqs",
            endpoint=f"arn:aws:sqs:us-east-1:{OWN}:pending",
        ),
    ]
    lineage = DatasetLineageCollector(extractor.report)
    # No local queues were ingested.
    extractor.add_lineage(_topic(), lineage=lineage, resolver=_resolver())

    assert _upstreams(lineage) == {
        "urn:li:dataset:(urn:li:dataPlatform:sqs,210987654321.eu-west-1.remote,PROD)": {
            f"urn:li:dataset:(urn:li:dataPlatform:sns,{OWN}.us-east-1.events,PROD)": "subscription"
        }
    }
    assert len(extractor.report.lineage_edges_skipped) == 1
    assert client.calls == []


def test_subscription_dead_letter_queue_edge() -> None:
    dlq = f"arn:aws:sqs:us-east-1:{OWN}:events-dlq"
    client = FakeSnsClient(
        {f"{TOPIC_ARN}:sub-1": {"RedrivePolicy": f'{{"deadLetterTargetArn":"{dlq}"}}'}}
    )
    extractor = _extractor(client)
    extractor.subscriptions[TOPIC_ARN] = [
        _subscription("https://hooks.example.com/x", 1, protocol="https")
    ]
    lineage = DatasetLineageCollector(extractor.report)
    resolver = _resolver()
    resolver.queue_arns.add(dlq)
    extractor.add_lineage(_topic(), lineage=lineage, resolver=resolver)

    dlq_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sqs,{OWN}.us-east-1.events-dlq,PROD)"
    )
    assert list(_upstreams(lineage)[dlq_urn].values()) == ["subscription_dead_letter"]


def test_patches_add_rather_than_replace() -> None:
    report = AwsMessagingSourceReport()
    lineage = DatasetLineageCollector(report)
    queue = "urn:li:dataset:(urn:li:dataPlatform:sqs,q,PROD)"
    for upstream, relationship in [
        (
            "urn:li:dataset:(urn:li:dataPlatform:sns,a,PROD)",
            LineageRelationship.SUBSCRIPTION,
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:sqs,b,PROD)",
            LineageRelationship.DEAD_LETTER,
        ),
        (queue, LineageRelationship.DEAD_LETTER),
    ]:
        lineage.add(upstream=upstream, downstream=queue, relationship=relationship)
    assert _upstreams(lineage) == {
        queue: {
            "urn:li:dataset:(urn:li:dataPlatform:sns,a,PROD)": "subscription",
            "urn:li:dataset:(urn:li:dataPlatform:sqs,b,PROD)": "dead_letter",
        }
    }
    assert report.lineage_edges_emitted == 2
