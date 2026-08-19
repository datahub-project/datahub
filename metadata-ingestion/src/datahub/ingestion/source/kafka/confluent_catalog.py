from typing import Optional

from pydantic import Field

from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import (
    CatalogEntity,
    NameIndex,
    index_by_name,
)
from datahub.ingestion.source.kafka.confluent_catalog_constants import (
    TOPIC_CATALOG_QUERY,
    TOPIC_ROOT_KEY,
)
from datahub.ingestion.source.kafka.kafka_config import KafkaConfluentCatalogConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport


class CatalogKafkaTopic(CatalogEntity):
    cluster_id: Optional[str] = Field(default=None, alias="logical_cluster_id")


class KafkaTopicCatalog:
    def __init__(
        self,
        config: KafkaConfluentCatalogConfig,
        report: KafkaSourceReport,
        client: Optional[ConfluentStreamCatalogClient] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.client = client or ConfluentStreamCatalogClient(config, report)
        self._topics: Optional[NameIndex[CatalogKafkaTopic]] = None
        self._complete = True

    def get_topic(self, topic_name: str) -> Optional[CatalogKafkaTopic]:
        if self._topics is None:
            self._topics = self._fetch_topics()
        return self._topics.get(topic_name)

    def is_complete(self) -> bool:
        if self._topics is None:
            self._topics = self._fetch_topics()
        return self._complete

    def _fetch_topics(self) -> NameIndex[CatalogKafkaTopic]:
        result = self.client.fetch_entities(
            TOPIC_CATALOG_QUERY, TOPIC_ROOT_KEY, CatalogKafkaTopic
        )
        self._complete = result.complete
        topics = result.entities
        if self.config.cluster_id:
            in_cluster = [
                topic for topic in topics if topic.cluster_id == self.config.cluster_id
            ]
            if topics and not in_cluster:
                # Wrong / unset logical_cluster_id otherwise looks like "0 topics indexed".
                self.report.warning(
                    message="No Stream Catalog topic carries the configured Kafka cluster id, so no "
                    "catalog metadata will be applied. Check `confluent_catalog.cluster_id`.",
                    context=f"cluster_id={self.config.cluster_id}, topics_in_catalog={len(topics)}, "
                    f"cluster_ids_seen={sorted({str(topic.cluster_id) for topic in topics})}",
                )
            topics = in_cluster

        index = index_by_name(topics)
        index.report_issues(self.report, "topic")
        for name, candidates in index.ambiguous.items():
            self.report.warning(
                message="Skipping Stream Catalog metadata for a topic name that exists in "
                "more than one Kafka cluster in this environment. Set "
                "`confluent_catalog.cluster_id` to pick the right cluster.",
                context=f"topic={name}, clusters={sorted(str(c.cluster_id) for c in candidates)}",
            )
        self.report.catalog_topics_indexed = len(index.by_name)
        return index

    def close(self) -> None:
        self.client.close()
