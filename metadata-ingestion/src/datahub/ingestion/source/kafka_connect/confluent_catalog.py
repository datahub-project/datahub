import logging
from typing import List, Optional

from pydantic import Field, field_validator

from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import (
    CatalogEntity,
    NameIndex,
    empty_if_null,
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

logger = logging.getLogger(__name__)


class CatalogConnector(CatalogEntity):
    topics: List[CatalogEntity] = Field(default_factory=list)

    @field_validator("topics", mode="before")
    @classmethod
    def default_empty_topics(cls, value: object) -> object:
        return empty_if_null(value)

    def get_topic_names(self) -> List[str]:
        return list(dict.fromkeys(topic.name for topic in self.topics if topic.name))


class ConnectorCatalog:
    """Every connector in the environment, fetched once per ingestion run."""

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

    def get_connectors(self) -> NameIndex[CatalogConnector]:
        if self._connectors is None:
            connectors = self.client.fetch_entities(
                CONNECTOR_CATALOG_QUERY, CONNECTOR_ROOT_KEY, CatalogConnector
            )
            index = index_by_name(connectors)
            for name in index.ambiguous:
                # `cn_connector` spans the environment and is matched by name alone, so
                # inheriting the wrong connector's tags and lineage is a real risk.
                self.report.warning(
                    message="Skipping Stream Catalog metadata for a connector name that the "
                    "catalog reports more than once in this environment.",
                    context=f"connector={name}",
                )
            self._connectors = index
            self.report.catalog_connectors_fetched = len(index.by_name)
        return self._connectors

    def get_connector(self, connector_name: str) -> Optional[CatalogConnector]:
        return self.get_connectors().get(connector_name)

    def close(self) -> None:
        self.client.close()
