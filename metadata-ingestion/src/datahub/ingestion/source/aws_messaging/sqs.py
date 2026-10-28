from typing import Callable, Iterable, Iterator

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import (
    UrnBuilder,
    parse_arn,
    parse_redrive_policy,
)
from datahub.ingestion.source.aws_messaging.clients import SqsClient
from datahub.ingestion.source.aws_messaging.config import AwsMessagingSourceConfig
from datahub.ingestion.source.aws_messaging.constants import (
    SQS_PLATFORM,
    LineageRelationship,
    QueueAttribute,
)
from datahub.ingestion.source.aws_messaging.lineage import DatasetLineageCollector
from datahub.ingestion.source.aws_messaging.mapper import (
    AwsRegionKey,
    epoch_seconds,
    queue_console_url,
    queue_properties,
    tag_urns,
)
from datahub.ingestion.source.aws_messaging.models import Arn, SqsQueue
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.sdk import Dataset


class SqsExtractor:
    def __init__(
        self,
        *,
        config: AwsMessagingSourceConfig,
        report: AwsMessagingSourceReport,
        client: SqsClient,
        urns: UrnBuilder,
        region_key: AwsRegionKey,
    ):
        self.config = config
        self.report = report
        self.client = client
        self.urns = urns
        self.region_key = region_key

    def list_queues(self) -> Iterator[SqsQueue]:
        for url in self.client.list_queue_urls():
            name = url.rstrip("/").rsplit("/", 1)[-1]
            if not self.config.queue_pattern.allowed(name):
                self.report.filtered_queues.append(name)
                continue
            attributes = self.client.get_attributes(url)
            if attributes is None:
                self.report.queues_failed.append(name)
                continue
            arn = parse_arn(attributes.get(QueueAttribute.QUEUE_ARN))
            if arn is None:
                self.report.queues_failed.append(name)
                self.report.warning(
                    title="Queue has no readable ARN",
                    message="GetQueueAttributes returned no parseable QueueArn; the queue is skipped.",
                    context=f"queue={url}",
                )
                continue
            tags = self.client.get_tags(url) if self.config.extract_tags else {}
            self.report.queues_scanned += 1
            yield SqsQueue(url=url, arn=arn, attributes=attributes, tags=tags)

    def to_workunits(self, queue: SqsQueue) -> Iterable[MetadataWorkUnit]:
        dataset = Dataset(
            platform=SQS_PLATFORM,
            name=self.urns.resource_id(queue.arn),
            display_name=queue.name,
            env=self.config.env,
            platform_instance=self.urns.instance_for(queue.arn.account),
            subtype=DatasetSubTypes.SQS_QUEUE.value,
            parent_container=self.region_key,
            external_url=queue_console_url(queue),
            custom_properties=queue_properties(queue),
            created=epoch_seconds(
                queue.attributes.get(QueueAttribute.CREATED_TIMESTAMP)
            ),
            last_modified=epoch_seconds(
                queue.attributes.get(QueueAttribute.LAST_MODIFIED_TIMESTAMP)
            ),
            tags=tag_urns(queue.tags),
        )
        yield from dataset.as_workunits()

    def add_lineage(
        self,
        queue: SqsQueue,
        lineage: DatasetLineageCollector,
        queue_exists: Callable[[Arn], bool],
    ) -> None:
        redrive = parse_redrive_policy(
            queue.attributes.get(QueueAttribute.REDRIVE_POLICY)
        )
        if redrive is None:
            return
        source_urn = self.urns.queue_urn(queue.arn)
        dlq = parse_arn(redrive.dead_letter_target_arn)
        if dlq is None or not queue_exists(dlq):
            self.report.report_edge_skipped(
                source_urn,
                redrive.dead_letter_target_arn,
                "dead-letter queue was not ingested",
            )
            return
        lineage.add(
            upstream=source_urn,
            downstream=self.urns.queue_urn(dlq),
            relationship=LineageRelationship.DEAD_LETTER,
        )
