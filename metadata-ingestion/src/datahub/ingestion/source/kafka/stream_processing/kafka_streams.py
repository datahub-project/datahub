import logging
from typing import Dict, List

from confluent_kafka.admin import AdminClient

from datahub.ingestion.source.kafka.consumer_group_lineage import (
    ConsumerGroupInfo,
    ConsumerGroupLineageExtractor,
)
from datahub.ingestion.source.kafka.kafka_config import (
    KafkaConsumerGroupLineageConfig,
    KafkaStreamsLineageConfig,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.constants import (
    PROP_APPLICATION_ID,
    PROP_CLIENT_IDS,
    PROP_STATE,
    STREAMS_CHANGELOG_SUFFIX,
    STREAMS_REPARTITION_SUFFIX,
    StreamProcessingEngine,
)
from datahub.ingestion.source.kafka.stream_processing.models import StreamProcessingJob

logger = logging.getLogger(__name__)


class KafkaStreamsLineageExtractor:
    def __init__(
        self,
        admin_client: AdminClient,
        config: KafkaStreamsLineageConfig,
        report: KafkaSourceReport,
        timeout_seconds: float,
    ) -> None:
        # Reuse the consumer-group discovery: a Kafka Streams application.id *is* a
        # consumer group, and the topics it reads are the group's subscribed topics.
        self._groups = ConsumerGroupLineageExtractor(
            admin_client=admin_client,
            config=KafkaConsumerGroupLineageConfig(enabled=True),
            report=report,
            timeout_seconds=timeout_seconds,
        )
        self.config = config
        self.report = report

    def extract(self) -> List[StreamProcessingJob]:
        jobs: List[StreamProcessingJob] = []
        for group in self._groups.extract():
            if not self.config.application_patterns.allowed(group.group_id):
                continue
            internal_topics = [
                topic
                for topic in group.topics
                if _is_internal_topic(group.group_id, topic)
            ]
            if not internal_topics:
                # No changelog/repartition topics named after the group => not a
                # Kafka Streams app, just a plain consumer group. Skip it here.
                continue
            input_topics = [
                topic for topic in group.topics if topic not in internal_topics
            ]

            self.report.stream_processing_jobs_scanned += 1
            jobs.append(
                StreamProcessingJob(
                    engine=StreamProcessingEngine.KAFKA_STREAMS,
                    job_id=group.group_id,
                    name=group.group_id,
                    input_topics=input_topics,
                    # ponytail: internal state topics are the only outputs the broker
                    # exposes. True downstream sink topics live in the app's
                    # TopologyDescription, which no Kafka/Confluent API serves; upgrade
                    # path is a config-provided topology or a StreamsMetadata endpoint.
                    output_topics=internal_topics,
                    low_confidence=True,
                    custom_properties=_streams_properties(group),
                )
            )
        return jobs


def _is_internal_topic(application_id: str, topic: str) -> bool:
    if not topic.startswith(f"{application_id}-"):
        return False
    return topic.endswith(STREAMS_CHANGELOG_SUFFIX) or topic.endswith(
        STREAMS_REPARTITION_SUFFIX
    )


def _streams_properties(group: ConsumerGroupInfo) -> Dict[str, str]:
    properties = {PROP_APPLICATION_ID: group.group_id}
    if group.state:
        properties[PROP_STATE] = group.state
    client_ids = sorted({m.client_id for m in group.members if m.client_id})
    if client_ids:
        properties[PROP_CLIENT_IDS] = ", ".join(client_ids)
    return properties
