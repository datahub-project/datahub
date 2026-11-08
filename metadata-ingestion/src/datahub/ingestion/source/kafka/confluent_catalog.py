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
    # Cluster-link source of a mirror topic. A source outside this catalog's environment
    # only appears as `externalSourceTopicName`, with no cluster id to place it, so it is
    # not read.
    source_topic: Optional[CatalogMirrorSourceTopic] = None


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
        self.topic_config = config

    def get_topic(self, topic_name: str) -> Optional[CatalogKafkaTopic]:
        return self._get(topic_name)

    def _filter(self, entities: List[CatalogKafkaTopic]) -> List[CatalogKafkaTopic]:
        cluster_id = self.topic_config.cluster_id
        if not cluster_id:
            return entities
        in_cluster = [topic for topic in entities if topic.cluster_id == cluster_id]
        if entities and not in_cluster:
            # Wrong / unset logical_cluster_id otherwise looks like "0 topics indexed".
            self.report.warning(
                message="No Stream Catalog topic carries the configured Kafka cluster id, so no "
                "catalog metadata will be applied. Check `confluent_catalog.cluster_id`.",
                context=f"cluster_id={cluster_id}, topics_in_catalog={len(entities)}, "
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
