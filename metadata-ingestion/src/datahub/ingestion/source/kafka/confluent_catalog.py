from typing import List, Optional

from pydantic import Field

from datahub.ingestion.source.confluent.catalog_index import CatalogIndex
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import CatalogEntity, CatalogModel
from datahub.ingestion.source.kafka.confluent_catalog_constants import (
    TOPIC_CATALOG_QUERY,
    TOPIC_ROOT_KEY,
)
from datahub.ingestion.source.kafka.kafka_config import KafkaConfluentCatalogConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport


class CatalogMirrorSourceTopic(CatalogModel):
    name: Optional[str] = None
    cluster_id: Optional[str] = Field(default=None, alias="logical_cluster_id")


class CatalogKafkaTopic(CatalogEntity):
    cluster_id: Optional[str] = Field(default=None, alias="logical_cluster_id")
    # Replication/cluster-link source: the topic this one mirrors from. `source_topic` is the
    # populated relationship; `externalSourceTopicName` is the scalar fallback used when the
    # upstream lives in a cluster that is not in this catalog.
    source_topic: Optional[CatalogMirrorSourceTopic] = None
    external_source_topic_name: Optional[str] = Field(
        default=None, alias="externalSourceTopicName"
    )

    def upstream_topic_name(self) -> Optional[str]:
        if self.source_topic and self.source_topic.name:
            return self.source_topic.name
        return self.external_source_topic_name or None


class KafkaTopicCatalog(CatalogIndex[CatalogKafkaTopic, KafkaSourceReport]):
    def __init__(
        self,
        config: KafkaConfluentCatalogConfig,
        report: KafkaSourceReport,
        client: Optional[ConfluentStreamCatalogClient] = None,
    ) -> None:
        super().__init__(
            config,
            report,
            query=TOPIC_CATALOG_QUERY,
            root_key=TOPIC_ROOT_KEY,
            model=CatalogKafkaTopic,
            entity_label="topic",
            client=client,
        )
        self._cluster_id = config.cluster_id

    def get_topic(self, topic_name: str) -> Optional[CatalogKafkaTopic]:
        return self._get(topic_name)

    def _filter(self, entities: List[CatalogKafkaTopic]) -> List[CatalogKafkaTopic]:
        if not self._cluster_id:
            return entities
        in_cluster = [
            topic for topic in entities if topic.cluster_id == self._cluster_id
        ]
        if entities and not in_cluster:
            # Wrong / unset logical_cluster_id otherwise looks like "0 topics indexed".
            self.report.warning(
                message="No Stream Catalog topic carries the configured Kafka cluster id, so no "
                "catalog metadata will be applied. Check `confluent_catalog.cluster_id`.",
                context=f"cluster_id={self._cluster_id}, topics_in_catalog={len(entities)}, "
                f"cluster_ids_seen={sorted({str(topic.cluster_id) for topic in entities})}",
            )
        return in_cluster

    def _warn_ambiguous(self, name: str, candidates: List[CatalogKafkaTopic]) -> None:
        self.report.warning(
            message="Skipping Stream Catalog metadata for a topic name that exists in "
            "more than one Kafka cluster in this environment. Set "
            "`confluent_catalog.cluster_id` to pick the right cluster.",
            context=f"topic={name}, clusters={sorted(str(c.cluster_id) for c in candidates)}",
        )

    def _record_indexed(self, count: int) -> None:
        self.report.catalog_topics_indexed = count
