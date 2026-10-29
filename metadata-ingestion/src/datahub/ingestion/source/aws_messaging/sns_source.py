import functools
import itertools
from collections import defaultdict
from typing import Dict, FrozenSet, Iterable, Iterator, List, Optional, Set

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import parse_arn, parse_redrive_policy
from datahub.ingestion.source.aws_messaging.base import AwsMessagingSourceBase, probe
from datahub.ingestion.source.aws_messaging.clients import (
    SnsClient,
    SqsClient,
    TaggingClient,
)
from datahub.ingestion.source.aws_messaging.config import SnsSourceConfig
from datahub.ingestion.source.aws_messaging.constants import (
    QUEUE_URL_PATTERN,
    SNS_PLATFORM,
    AwsService,
    LineageRelationship,
    SubscriptionAttribute,
    SubscriptionProtocol,
)
from datahub.ingestion.source.aws_messaging.mapper import (
    data_identifiers,
    domain_for,
    tag_patch_workunits,
    term_patch_workunits,
    topic_console_url,
    topic_properties,
)
from datahub.ingestion.source.aws_messaging.models import Arn, SnsSubscription, SnsTopic
from datahub.ingestion.source.aws_messaging.report import SnsSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import StatusClass
from datahub.sdk import Dataset


@platform_name("Amazon SNS", id=SNS_PLATFORM)
@config_class(SnsSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.CONTAINERS, "Region containers")
@capability(
    SourceCapability.TAGS, "From AWS resource tags, added without removing UI tags"
)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Topic to subscribed queue and to subscription dead-letter queue",
)
@capability(
    SourceCapability.GLOSSARY_TERMS,
    "Opt-in, from data protection policy identifiers you map to existing terms",
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Defaults to the AWS account id")
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "SNS has no schema", supported=False)
@capability(SourceCapability.DESCRIPTIONS, "SNS has no description", supported=False)
class SnsSource(AwsMessagingSourceBase):
    """Catalogs Amazon SNS topics of one region as datasets on the `sns` platform,
    with lineage from each topic to the SQS queues subscribed to it."""

    platform = SNS_PLATFORM
    service = AwsService.SNS
    config: SnsSourceConfig
    report: SnsSourceReport

    def __init__(self, config: SnsSourceConfig, ctx: PipelineContext):
        report = SnsSourceReport()
        self.config = config
        super().__init__(config, ctx, report)
        self.report = report
        # Subscribed queues are owned by the sqs source; this tracks which local
        # ones exist so an edge never points at a queue that isn't there.
        self.queues = ResourceResolver(
            urns=self.urns, region=self.region, service=AwsService.SQS
        )
        self.topics: List[SnsTopic] = []
        self.subscriptions: Dict[str, List[SnsSubscription]] = defaultdict(list)
        # Listed but unreadable this run: still in AWS, so kept out of stale removal.
        self.unreadable_topics: List[Arn] = []
        # Topics whose subscriptions (or their attributes) couldn't all be read, so
        # their edges are unknown rather than gone.
        self.incomplete_topics: Set[str] = set()

    def create_clients(self) -> None:
        self.sns = SnsClient(
            self.config.make_client(self.session, AwsService.SNS), self.base_report
        )
        self.tagging = TaggingClient(
            self.config.make_client(self.session, AwsService.TAGGING),
            self.base_report,
        )
        self.sqs = SqsClient(
            self.config.make_client(self.session, AwsService.SQS), self.base_report
        )

    def owned_relationships(self) -> FrozenSet[LineageRelationship]:
        # Subscription dead-letter queues are only known when their attributes
        # are read, so their edges are only retracted then.
        if self.config.include_subscription_attributes:
            return frozenset(
                {
                    LineageRelationship.SUBSCRIPTION,
                    LineageRelationship.SUBSCRIPTION_DEAD_LETTER,
                }
            )
        return frozenset({LineageRelationship.SUBSCRIPTION})

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SnsSource":
        return cls(SnsSourceConfig.model_validate(config_dict), ctx)

    def first_resource_arn(self) -> Optional[str]:
        return self.sns.first_topic_arn()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self._load_local_queues()
        yield from self.with_region_container(
            itertools.chain(self._topic_workunits(), self._keep_unreadable_topics())
        )
        for topic in self.topics:
            self._add_subscription_edges(topic)
            if str(topic.arn) not in self.incomplete_topics:
                self.lineage.complete.add(self.urns.topic_urn(topic.arn))
        yield from self.lineage.workunits()

    def _load_local_queues(self) -> None:
        # Every queue that exists counts, not only those the sqs recipe's
        # queue_pattern keeps: the subscription is real and the queue URN follows
        # from its ARN, so the edge is right even if sqs doesn't ingest the queue.
        # Only used to check edges, so a failed listing must not fail the run.
        for url in self.sqs.list_queue_urls(optional=True):
            match = QUEUE_URL_PATTERN.search(url)
            if match:
                self.queues.known.add(
                    ResourceResolver.key(
                        match.group("account"), self.region, match.group("name")
                    )
                )

    def list_topics(self) -> Iterator[SnsTopic]:
        bulk_tags = (
            self.tagging.get_tags_by_arn(AwsService.SNS.value)
            if self.config.extract_tags
            else {}
        )
        for topic_arn in self.sns.list_topic_arns():
            arn = parse_arn(topic_arn)
            if arn is None:
                self.report.topics_failed.append(topic_arn)
                continue
            if not self.config.topic_pattern.allowed(arn.resource_name):
                self.report.filtered_topics.append(arn.resource_name)
                continue
            attributes = self.sns.get_topic_attributes(topic_arn)
            if attributes is None:
                self.report.topics_failed.append(arn.resource_name)
                self.unreadable_topics.append(arn)
                continue
            for subscription in self.sns.list_subscriptions(
                topic_arn,
                on_error=functools.partial(self.incomplete_topics.add, topic_arn),
            ):
                self.report.subscriptions_scanned += 1
                self.subscriptions[topic_arn].append(subscription)
            if bulk_tags is None:
                tags = self.sns.get_tags(topic_arn)
            else:
                tags = bulk_tags.get(topic_arn, {})
            self.report.topics_scanned += 1
            yield SnsTopic(arn=arn, attributes=attributes, tags=tags)

    def _topic_workunits(self) -> Iterable[MetadataWorkUnit]:
        for topic in self.list_topics():
            self.topics.append(topic)
            self.resolver.add(topic.arn)
            urn = self.urns.topic_urn(topic.arn)
            self.lineage.owned.add(urn)
            yield from Dataset(
                platform=SNS_PLATFORM,
                name=self.urns.resource_id(topic.arn),
                display_name=topic.name,
                env=self.config.env,
                platform_instance=self.urns.instance_for(topic.arn.account),
                subtype=DatasetSubTypes.TOPIC.value,
                parent_container=self.region_key(),
                external_url=topic_console_url(topic),
                custom_properties=topic_properties(
                    topic, self.subscriptions.get(str(topic.arn), [])
                ),
                domain=domain_for(topic.name, self.config.domain),
            ).as_workunits()
            tags = dict(topic.tags)
            terms: List[str] = []
            if self.config.data_protection.enabled:
                protection = self.config.data_protection
                for identifier in data_identifiers(
                    self.sns.get_data_protection_policy(str(topic.arn))
                ):
                    tags[f"{protection.tag_prefix}:{identifier}"] = ""
                    term = protection.glossary_term_map.get(identifier)
                    if term:
                        terms.append(
                            term if term.startswith("urn:li:") else make_term_urn(term)
                        )
            yield from tag_patch_workunits(urn, tags)
            yield from term_patch_workunits(urn, terms)

    def _keep_unreadable_topics(self) -> Iterable[MetadataWorkUnit]:
        # Status only: registers the topic as seen without overwriting what an
        # earlier run stored.
        for arn in self.unreadable_topics:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.urns.topic_urn(arn),
                aspect=StatusClass(removed=False),
            ).as_workunit()

    def _add_subscription_edges(self, topic: SnsTopic) -> None:
        topic_urn = self.urns.topic_urn(topic.arn)
        for subscription in self.subscriptions.get(str(topic.arn), []):
            # Unconfirmed and deleted subscriptions deliver nothing: SNS reports
            # them with a placeholder (`PendingConfirmation`, `Deleted`) instead
            # of an ARN.
            if parse_arn(subscription.subscription_arn) is None:
                continue
            if subscription.protocol == SubscriptionProtocol.SQS:
                self._add_queue_edge(
                    topic_urn, subscription.endpoint, LineageRelationship.SUBSCRIPTION
                )
            if self.config.include_subscription_attributes:
                self.report.subscription_attribute_calls += 1
                attributes = self.sns.get_subscription_attributes(
                    subscription.subscription_arn
                )
                if attributes is None:
                    self.incomplete_topics.add(str(topic.arn))
                    continue
                redrive = parse_redrive_policy(
                    attributes.get(SubscriptionAttribute.REDRIVE_POLICY)
                )
                if redrive is not None:
                    self._add_queue_edge(
                        topic_urn,
                        redrive.dead_letter_target_arn,
                        LineageRelationship.SUBSCRIPTION_DEAD_LETTER,
                    )

    def _add_queue_edge(
        self, topic_urn: str, queue_arn: str, relationship: LineageRelationship
    ) -> None:
        queue_urn = self.queues.urn(parse_arn(queue_arn))
        if queue_urn is None:
            self.report.report_edge_skipped(
                topic_urn,
                queue_arn,
                "queue ARN unparseable or not found in this region",
            )
            return
        self.lineage.add(
            upstream=topic_urn, downstream=queue_urn, relationship=relationship
        )

    def get_report(self) -> SnsSourceReport:
        return self.report

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        report = TestConnectionReport()
        try:
            config = SnsSourceConfig.model_validate(config_dict)
            session = config.aws_config.get_session()
            report.basic_connectivity = probe(
                config,
                session,
                AwsService.SNS,
                lambda c: c.list_topics(),
                "sns:ListTopics",
            )
            report.capability_report = {
                SourceCapability.LINEAGE_COARSE: probe(
                    config,
                    session,
                    AwsService.SQS,
                    lambda c: c.list_queues(MaxResults=1),
                    "sqs:ListQueues",
                )
            }
        except (
            Exception
        ) as e:  # test_connection reports every failure, config errors included
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{type(e).__name__}: {e}"
            )
        return report
