from typing import List, Optional

from pydantic import Field

from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import (
    CatalogEntity,
    NameIndex,
    NullAsEmptyList,
    index_by_name,
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


class ConnectorCatalog:
    def __init__(
        self,
        config: ConfluentCatalogConfig,
        report: KafkaConnectSourceReport,
        client: Optional[ConfluentStreamCatalogClient] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.client = client or ConfluentStreamCatalogClient(config, report)
        self._connectors: Optional[NameIndex[CatalogConnector]] = None
        self._complete = True

    def is_complete(self) -> bool:
        self.get_connectors()
        return self._complete

    def get_connectors(self) -> NameIndex[CatalogConnector]:
        if self._connectors is None:
            result = self.client.fetch_entities(
                CONNECTOR_CATALOG_QUERY, CONNECTOR_ROOT_KEY, CatalogConnector
            )
            self._complete = result.complete
            connectors = result.entities
            index = index_by_name(connectors)
            index.report_issues(self.report, "connector")
            for name in index.ambiguous:
                self.report.warning(
                    message="Skipping Stream Catalog metadata for a connector name that the "
                    "catalog reports more than once in this environment.",
                    context=f"connector={name}",
                )
            self._connectors = index
            self.report.catalog_connectors_indexed = len(index.by_name)
        return self._connectors

    def get_connector(self, connector_name: str) -> Optional[CatalogConnector]:
        return self.get_connectors().get(connector_name)

    def close(self) -> None:
        self.client.close()
