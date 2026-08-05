import logging
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

logger = logging.getLogger(__name__)


class CatalogKafkaTopic(CatalogEntity):
    cluster_id: Optional[str] = Field(default=None, alias="logical_cluster_id")


class KafkaTopicCatalog:
    # Environment-scoped; ambiguous topic names are dropped unless cluster_id is set.
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

    def get_topic(self, topic_name: str) -> Optional[CatalogKafkaTopic]:
        if self._topics is None:
            self._topics = self._fetch_topics()
        return self._topics.get(topic_name)

    def _fetch_topics(self) -> NameIndex[CatalogKafkaTopic]:
        topics = self.client.fetch_entities(
            TOPIC_CATALOG_QUERY, TOPIC_ROOT_KEY, CatalogKafkaTopic
        )
        if self.config.cluster_id:
            in_cluster = [
                topic for topic in topics if topic.cluster_id == self.config.cluster_id
            ]
            if topics and not in_cluster:
                # Typo or unset logical_cluster_id otherwise looks like "0 topics fetched".
                self.report.warning(
                    message="No Stream Catalog topic carries the configured Kafka cluster id, so no "
                    "catalog metadata will be applied. Check `confluent_catalog.cluster_id`.",
                    context=f"cluster_id={self.config.cluster_id}, topics_in_catalog={len(topics)}, "
                    f"cluster_ids_seen={sorted({str(topic.cluster_id) for topic in topics})}",
                )
            topics = in_cluster

        index = index_by_name(topics)
        self._report_index_issues(index)
        self.report.catalog_topics_fetched = len(index.by_name)
        return index

    def _report_index_issues(self, index: NameIndex[CatalogKafkaTopic]) -> None:
        if index.empty_name_count:
            self.report.warning(
                message="Skipped Stream Catalog topics that had an empty name",
                context=f"count={index.empty_name_count}",
            )
        for name, candidates in index.ambiguous.items():
            self.report.warning(
                message="Skipping Stream Catalog metadata for a topic name that exists in "
                "more than one Kafka cluster in this environment. Set "
                "`confluent_catalog.cluster_id` to pick the right cluster.",
                context=f"topic={name}, clusters={sorted(str(c.cluster_id) for c in candidates)}",
            )
        for lowered, candidates in index.case_ambiguous.items():
            self.report.warning(
                message="Case-insensitive Stream Catalog topic lookup is disabled for a name "
                "that matches more than one catalog entity; exact-case lookups still work",
                context=f"name={lowered}, variants={sorted(c.name for c in candidates)}",
            )

    def close(self) -> None:
        self.client.close()
