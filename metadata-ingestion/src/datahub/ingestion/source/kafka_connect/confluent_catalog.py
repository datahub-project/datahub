from typing import List, Optional

from pydantic import Field

from datahub.ingestion.source.confluent.catalog_index import CatalogIndex
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import (
    CatalogEntity,
    NameIndex,
    NullAsEmptyList,
)
from datahub.ingestion.source.kafka_connect.common import (
    ConfluentCatalogConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.confluent_catalog_constants import (
    CONNECTOR_CATALOG_QUERY,
    CONNECTOR_ROOT_KEY,
)


class CatalogConnector(CatalogEntity):
    topics: NullAsEmptyList[CatalogEntity] = Field(default_factory=list)

    def get_topic_names(self) -> List[str]:
        return list(dict.fromkeys(topic.name for topic in self.topics if topic.name))


class ConnectorCatalog(CatalogIndex[CatalogConnector, KafkaConnectSourceReport]):
    def __init__(
        self,
        config: ConfluentCatalogConfig,
        report: KafkaConnectSourceReport,
        client: Optional[ConfluentStreamCatalogClient] = None,
    ) -> None:
        super().__init__(
            config,
            report,
            query=CONNECTOR_CATALOG_QUERY,
            root_key=CONNECTOR_ROOT_KEY,
            model=CatalogConnector,
            entity_label="connector",
            client=client,
        )

    def get_connectors(self) -> NameIndex[CatalogConnector]:
        return self._ensure_indexed()

    def get_connector(self, connector_name: str) -> Optional[CatalogConnector]:
        return self._get(connector_name)

    def _warn_ambiguous(self, name: str, candidates: List[CatalogConnector]) -> None:
        self.report.warning(
            message="Skipping Stream Catalog metadata for a connector name that the "
            "catalog reports more than once in this environment.",
            context=f"connector={name}",
        )

    def _record_indexed(self, count: int) -> None:
        self.report.catalog_connectors_indexed = count
