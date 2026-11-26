import logging
from typing import List, Optional

from pydantic import Field, field_validator

from datahub.ingestion.source.confluent.catalog_index import CatalogIndex
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import CatalogEntity, CatalogModel
from datahub.ingestion.source.kafka.confluent_catalog_constants import (
    MIRROR_FIELDS_PLACEHOLDER,
    TOPIC_CATALOG_QUERY,
    TOPIC_MIRROR_FIELDS,
    TOPIC_ROOT_KEY,
)
from datahub.ingestion.source.kafka.kafka_config import KafkaConfluentCatalogConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport

logger = logging.getLogger(__name__)


def build_topic_catalog_query(include_lineage: bool) -> str:
    return TOPIC_CATALOG_QUERY.replace(
        MIRROR_FIELDS_PLACEHOLDER, TOPIC_MIRROR_FIELDS if include_lineage else ""
    )


class CatalogMirrorSourceTopic(CatalogModel):
    name: Optional[str] = None
    cluster_id: Optional[str] = Field(default=None, alias="logical_cluster_id")


class CatalogKafkaTopic(CatalogEntity):
    cluster_id: Optional[str] = Field(default=None, alias="logical_cluster_id")
    # Cluster-link source of a mirror topic. A source outside this catalog's environment
    # only appears as `externalSourceTopicName`, with no cluster id to place it, so it is
    # not read.
    source_topic: Optional[CatalogMirrorSourceTopic] = None

    @field_validator("source_topic", mode="before")
    @classmethod
    def unwrap_source_topic(cls, value: object) -> object:
        # The catalog serves other relationships (cn_connector.topics) as lists
        # and an unset relationship may come back as [] rather than null, while
        # the one-to-one shape of a set `source_topic` is unconfirmed on a tenant
        # with cluster links. Accept either shape so a mismatch cannot fail
        # validation of the whole topic and drop its tags and business metadata.
        if not isinstance(value, list):
            return value
        if len(value) > 1:
            logger.debug(
                f"source_topic carried {len(value)} entries; using the first: {value}"
            )
        if not value or not isinstance(value[0], dict):
            return None
        return value[0]


class KafkaTopicCatalog(
    CatalogIndex[CatalogKafkaTopic, KafkaSourceReport, KafkaConfluentCatalogConfig]
):
    def __init__(
        self,
        config: KafkaConfluentCatalogConfig,
        report: KafkaSourceReport,
        client: Optional[ConfluentStreamCatalogClient] = None,
    ) -> None:
        super().__init__(
            config,
            report,
            query=build_topic_catalog_query(config.include_lineage),
            root_key=TOPIC_ROOT_KEY,
            model=CatalogKafkaTopic,
            entity_label="topic",
            client=client,
        )

    def get_topic(self, topic_name: str) -> Optional[CatalogKafkaTopic]:
        return self._get(topic_name)

    def _filter(self, entities: List[CatalogKafkaTopic]) -> List[CatalogKafkaTopic]:
        cluster_id = self.config.cluster_id
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
