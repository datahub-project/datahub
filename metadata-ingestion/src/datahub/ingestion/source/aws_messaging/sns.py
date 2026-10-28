from typing import Iterable, Iterator

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import SnsClient
from datahub.ingestion.source.aws_messaging.config import AwsMessagingSourceConfig
from datahub.ingestion.source.aws_messaging.constants import SNS_PLATFORM
from datahub.ingestion.source.aws_messaging.mapper import (
    AwsRegionKey,
    tag_urns,
    topic_console_url,
    topic_properties,
)
from datahub.ingestion.source.aws_messaging.models import SnsTopic
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
            custom_properties=topic_properties(topic),
            tags=tag_urns(topic.tags),
        )
        yield from dataset.as_workunits()
