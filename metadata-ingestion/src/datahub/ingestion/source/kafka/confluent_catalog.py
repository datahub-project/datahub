import logging
from collections import defaultdict
from typing import Dict, List, Optional

from pydantic import Field

from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import CatalogEntity, lookup_by_name
from datahub.ingestion.source.kafka.confluent_catalog_constants import (
    TOPIC_CATALOG_QUERY,
    TOPIC_ROOT_KEY,
)
from datahub.ingestion.source.kafka.kafka_config import KafkaConfluentCatalogConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport

logger = logging.getLogger(__name__)


class CatalogKafkaTopic(CatalogEntity):
    cluster_id: Optional[str] = Field(default=None, alias="clusterId")


class KafkaTopicCatalog:
    """
    Topic-level tags and business metadata from the Confluent Cloud Stream Catalog.

    The catalog is scoped to the Schema Registry endpoint, which covers a whole
    environment. An environment can hold more than one Kafka cluster, so topic names are
    not guaranteed unique; ambiguous names are dropped rather than risk tagging a topic
    with another cluster's metadata. Set `cluster_id` to resolve that.
    """

    def __init__(
        self,
        config: KafkaConfluentCatalogConfig,
        report: KafkaSourceReport,
        client: Optional[ConfluentStreamCatalogClient] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.client = client or ConfluentStreamCatalogClient(config, report)
        self._topics: Optional[Dict[str, CatalogKafkaTopic]] = None

    def get_topic(self, topic_name: str) -> Optional[CatalogKafkaTopic]:
        if self._topics is None:
            self._topics = self._fetch_topics()
        return lookup_by_name(self._topics, topic_name)

    def _fetch_topics(self) -> Dict[str, CatalogKafkaTopic]:
        topics = self.client.fetch_entities(
            TOPIC_CATALOG_QUERY, TOPIC_ROOT_KEY, CatalogKafkaTopic
        )
        if self.config.cluster_id:
            topics = [
                topic for topic in topics if topic.cluster_id == self.config.cluster_id
            ]
        self.report.catalog_topics_fetched = len(topics)

        by_name: Dict[str, List[CatalogKafkaTopic]] = defaultdict(list)
        for topic in topics:
            by_name[topic.name].append(topic)

        resolved: Dict[str, CatalogKafkaTopic] = {}
        for name, candidates in by_name.items():
            if len(candidates) > 1:
                self.report.warning(
                    message="Skipping Stream Catalog metadata for a topic name that exists in "
                    "more than one Kafka cluster in this environment. Set "
                    "`confluent_catalog.cluster_id` to pick the right cluster.",
                    context=f"topic={name}, clusters={sorted(str(c.cluster_id) for c in candidates)}",
                )
                continue
            resolved[name] = candidates[0]

        return resolved

    def close(self) -> None:
        self.client.close()
