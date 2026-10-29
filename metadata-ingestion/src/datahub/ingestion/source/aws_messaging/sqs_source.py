import itertools
from typing import FrozenSet, Iterable, Iterator, List, Optional

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
from datahub.ingestion.source.aws_messaging.clients import SqsClient, TaggingClient
from datahub.ingestion.source.aws_messaging.config import SqsSourceConfig
from datahub.ingestion.source.aws_messaging.constants import (
    QUEUE_URL_PATTERN,
    SQS_PLATFORM,
    AwsService,
    LineageRelationship,
    QueueAttribute,
)
from datahub.ingestion.source.aws_messaging.mapper import (
    domain_for,
    epoch_seconds,
    queue_console_url,
    queue_properties,
    tag_patch_workunits,
)
from datahub.ingestion.source.aws_messaging.models import Arn, SqsQueue
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import StatusClass
from datahub.sdk import Dataset


@platform_name("Amazon SQS", id=SQS_PLATFORM)
@config_class(SqsSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.CONTAINERS, "Region containers")
@capability(
    SourceCapability.TAGS, "From AWS resource tags, added without removing UI tags"
)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.LINEAGE_COARSE, "Queue to dead-letter queue")
@capability(SourceCapability.PLATFORM_INSTANCE, "Defaults to the AWS account id")
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "SQS has no schema", supported=False)
@capability(SourceCapability.DESCRIPTIONS, "SQS has no description", supported=False)
class SqsSource(AwsMessagingSourceBase):
    """Catalogs Amazon SQS queues of one region as datasets on the `sqs` platform,
    with lineage from each queue to its dead-letter queue."""

    platform = SQS_PLATFORM
    service = AwsService.SQS
    config: SqsSourceConfig
    report: SqsSourceReport

    def __init__(self, config: SqsSourceConfig, ctx: PipelineContext):
        report = SqsSourceReport()
        self.config = config
        super().__init__(config, ctx, report)
        self.report = report
        self.queues: List[SqsQueue] = []
        # Listed but unreadable this run: still in AWS, so kept out of stale removal.
        self.unreadable_queue_urls: List[str] = []

    def create_clients(self) -> None:
        self.sqs = SqsClient(
            self.config.make_client(self.session, AwsService.SQS), self.base_report
        )
        self.tagging = TaggingClient(
            self.config.make_client(self.session, AwsService.TAGGING),
            self.base_report,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqsSource":
        return cls(SqsSourceConfig.model_validate(config_dict), ctx)

    def owned_relationships(self) -> FrozenSet[LineageRelationship]:
        return frozenset({LineageRelationship.DEAD_LETTER})

    def first_resource_arn(self) -> Optional[str]:
        # The queue URL carries the account id, so this needs only sqs:ListQueues.
        # Depending on GetQueueAttributes would leave the account unresolved
        # whenever that call is denied, and the URNs would change between runs.
        url = self.sqs.first_queue_url()
        match = QUEUE_URL_PATTERN.search(url) if url else None
        if match is None:
            return None
        return f"arn:aws:sqs:{self.base_config.aws_config.aws_region}:{match.group('account')}:{match.group('name')}"

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Unreadable queues go through the container too: a run where every queue
        # is unreadable must still emit the region container, or stale removal
        # would delete it.
        yield from self.with_region_container(
            itertools.chain(self._queue_workunits(), self._keep_unreadable_queues())
        )
        # Dead-letter targets are checked once every queue in the region is known.
        for queue in self.queues:
            self._add_dead_letter_edge(queue)
        yield from self.lineage.workunits()

    def list_queues(self) -> Iterator[SqsQueue]:
        bulk_tags = (
            self.tagging.get_tags_by_arn(AwsService.SQS.value)
            if self.config.extract_tags
            else {}
        )
        for url in self.sqs.list_queue_urls():
            name = url.rstrip("/").rsplit("/", 1)[-1]
            if not self.config.queue_pattern.allowed(name):
                self.report.filtered_queues.append(name)
                continue
            attributes = self.sqs.get_attributes(url)
            if attributes is None:
                self.report.queues_failed.append(name)
                self.unreadable_queue_urls.append(url)
                continue
            arn = parse_arn(attributes.get(QueueAttribute.QUEUE_ARN))
            if arn is None:
                self.report.queues_failed.append(name)
                self.unreadable_queue_urls.append(url)
                self.report.warning(
                    title="Queue has no readable ARN",
                    message="GetQueueAttributes returned no parseable QueueArn; the queue is skipped.",
                    context=f"queue={url}",
                )
                continue
            if bulk_tags is None:
                tags = self.sqs.get_tags(url)
            else:
                tags = bulk_tags.get(str(arn), {})
            self.report.queues_scanned += 1
            yield SqsQueue(url=url, arn=arn, attributes=attributes, tags=tags)

    def _queue_workunits(self) -> Iterable[MetadataWorkUnit]:
        for queue in self.list_queues():
            self.queues.append(queue)
            self.resolver.add(queue.arn)
            urn = self.urns.queue_urn(queue.arn)
            self.lineage.owned.add(urn)
            self.lineage.complete.add(urn)
            yield from Dataset(
                platform=SQS_PLATFORM,
                name=self.urns.resource_id(queue.arn),
                display_name=queue.name,
                env=self.config.env,
                platform_instance=self.urns.instance_for(queue.arn.account),
                subtype=DatasetSubTypes.SQS_QUEUE.value,
                parent_container=self.region_key(),
                external_url=queue_console_url(queue),
                custom_properties=queue_properties(queue),
                created=epoch_seconds(
                    queue.attributes.get(QueueAttribute.CREATED_TIMESTAMP)
                ),
                last_modified=epoch_seconds(
                    queue.attributes.get(QueueAttribute.LAST_MODIFIED_TIMESTAMP)
                ),
                domain=domain_for(queue.name, self.config.domain),
            ).as_workunits()
            yield from tag_patch_workunits(urn, queue.tags)

    def _keep_unreadable_queues(self) -> Iterable[MetadataWorkUnit]:
        """A queue whose attributes couldn't be read (throttling, a transient
        error) still exists. Writing only its Status registers it with stale-entity
        removal without overwriting what an earlier run stored."""
        for url in self.unreadable_queue_urls:
            match = QUEUE_URL_PATTERN.search(url)
            if match is None:
                continue
            arn = Arn(
                partition="aws",
                service=AwsService.SQS.value,
                region=self.region,
                account=match.group("account"),
                resource=match.group("name"),
            )
            urn = self.urns.queue_urn(arn)
            # It exists, so dead-letter edges to it are kept.
            self.resolver.add(arn)
            self.lineage.owned.add(urn)
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=StatusClass(removed=False)
            ).as_workunit()

    def _add_dead_letter_edge(self, queue: SqsQueue) -> None:
        redrive = parse_redrive_policy(
            queue.attributes.get(QueueAttribute.REDRIVE_POLICY)
        )
        if redrive is None:
            return
        source_urn = self.urns.queue_urn(queue.arn)
        dlq_urn = self.resolver.urn(parse_arn(redrive.dead_letter_target_arn))
        if dlq_urn is None:
            self.report.report_edge_skipped(
                source_urn,
                redrive.dead_letter_target_arn,
                "dead-letter queue was not ingested",
            )
            return
        self.lineage.add(
            upstream=source_urn,
            downstream=dlq_urn,
            relationship=LineageRelationship.DEAD_LETTER,
        )

    def get_report(self) -> SqsSourceReport:
        return self.report

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        report = TestConnectionReport()
        try:
            config = SqsSourceConfig.model_validate(config_dict)
            report.basic_connectivity = probe(
                config,
                config.aws_config.get_session(),
                AwsService.SQS,
                lambda client: client.list_queues(MaxResults=1),
                "sqs:ListQueues",
            )
        except (
            Exception
        ) as e:  # test_connection reports every failure, config errors included
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{type(e).__name__}: {e}"
            )
        return report
