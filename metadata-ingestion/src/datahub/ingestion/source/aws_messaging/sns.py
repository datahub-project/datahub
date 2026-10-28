from collections import defaultdict
from typing import Callable, Dict, Iterable, Iterator, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import (
    UrnBuilder,
    parse_arn,
    parse_redrive_policy,
)
from datahub.ingestion.source.aws_messaging.clients import SnsClient
from datahub.ingestion.source.aws_messaging.config import AwsMessagingSourceConfig
from datahub.ingestion.source.aws_messaging.constants import (
    PENDING_CONFIRMATION,
    SNS_PLATFORM,
    LineageRelationship,
    SubscriptionAttribute,
    SubscriptionProtocol,
)
from datahub.ingestion.source.aws_messaging.lineage import DatasetLineageCollector
from datahub.ingestion.source.aws_messaging.mapper import (
    AwsRegionKey,
    tag_urns,
    topic_console_url,
    topic_properties,
)
from datahub.ingestion.source.aws_messaging.models import (
    Arn,
    SnsSubscription,
    SnsTopic,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.sdk import Dataset


class SnsExtractor:
    def __init__(
        self,
        *,
        config: AwsMessagingSourceConfig,
        report: AwsMessagingSourceReport,
        client: SnsClient,
        urns: UrnBuilder,
        region_key: AwsRegionKey,
    ):
        self.config = config
        self.report = report
        self.client = client
        self.urns = urns
        self.region_key = region_key
        self.subscriptions: Dict[str, List[SnsSubscription]] = defaultdict(list)

    def load_subscriptions(self) -> None:
        for subscription in self.client.list_subscriptions():
            self.report.subscriptions_scanned += 1
            self.subscriptions[subscription.topic_arn].append(subscription)

    def list_topics(self) -> Iterator[SnsTopic]:
        for topic_arn in self.client.list_topic_arns():
            arn = parse_arn(topic_arn)
            if arn is None:
                self.report.topics_failed.append(topic_arn)
                continue
            if not self.config.topic_pattern.allowed(arn.resource_name):
                self.report.filtered_topics.append(arn.resource_name)
                continue
            attributes = self.client.get_topic_attributes(topic_arn)
            if attributes is None:
                self.report.topics_failed.append(arn.resource_name)
                continue
            tags = self.client.get_tags(topic_arn) if self.config.extract_tags else {}
            self.report.topics_scanned += 1
            yield SnsTopic(arn=arn, attributes=attributes, tags=tags)

    def to_workunits(self, topic: SnsTopic) -> Iterable[MetadataWorkUnit]:
        dataset = Dataset(
            platform=SNS_PLATFORM,
            name=self.urns.resource_id(topic.arn),
            display_name=topic.name,
            env=self.config.env,
            platform_instance=self.urns.instance_for(topic.arn.account),
            subtype=DatasetSubTypes.TOPIC.value,
            parent_container=self.region_key,
            external_url=topic_console_url(topic),
            custom_properties=topic_properties(
                topic, self.subscriptions.get(str(topic.arn), [])
            ),
            tags=tag_urns(topic.tags),
        )
        yield from dataset.as_workunits()

    def add_lineage(
        self,
        topic: SnsTopic,
        lineage: DatasetLineageCollector,
        queue_exists: Callable[[Arn], bool],
    ) -> None:
        topic_urn = self.urns.topic_urn(topic.arn)
        for subscription in self.subscriptions.get(str(topic.arn), []):
            # An unconfirmed subscription delivers nothing yet.
            if subscription.subscription_arn == PENDING_CONFIRMATION:
                continue
            if subscription.protocol == SubscriptionProtocol.SQS:
                self._add_queue_edge(
                    topic_urn,
                    subscription.endpoint,
                    LineageRelationship.SUBSCRIPTION,
                    lineage,
                    queue_exists,
                )
            if self.config.include_subscription_attributes:
                attributes = self.client.get_subscription_attributes(
                    subscription.subscription_arn
                )
                redrive = parse_redrive_policy(
                    attributes.get(SubscriptionAttribute.REDRIVE_POLICY)
                )
                if redrive is not None:
                    self._add_queue_edge(
                        topic_urn,
                        redrive.dead_letter_target_arn,
                        LineageRelationship.SUBSCRIPTION_DEAD_LETTER,
                        lineage,
                        queue_exists,
                    )

    def _add_queue_edge(
        self,
        topic_urn: str,
        queue_arn: str,
        relationship: LineageRelationship,
        lineage: DatasetLineageCollector,
        queue_exists: Callable[[Arn], bool],
    ) -> None:
        arn = parse_arn(queue_arn)
        if arn is None:
            self.report.report_edge_skipped(
                topic_urn, queue_arn, "unparseable queue ARN"
            )
            return
        if not queue_exists(arn):
            self.report.report_edge_skipped(
                topic_urn,
                queue_arn,
                "queue is in this account and region but was not ingested",
            )
            return
        lineage.add(
            upstream=topic_urn,
            downstream=self.urns.queue_urn(arn),
            relationship=relationship,
        )
