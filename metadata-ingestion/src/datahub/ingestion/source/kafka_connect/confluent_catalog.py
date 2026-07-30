import logging
from typing import Dict, List, Optional

from pydantic import Field, field_validator

from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.models import (
    CatalogEntity,
    index_by_name,
    lookup_by_name,
)
from datahub.ingestion.source.kafka_connect.common import (
    ConfluentCatalogConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.confluent_catalog_constants import (
    CONNECTOR_CATALOG_QUERY,
    CONNECTOR_CLASS_FIELD,
    CONNECTOR_ROOT_KEY,
)

logger = logging.getLogger(__name__)


class CatalogTopic(CatalogEntity):
    pass


class CatalogConnector(CatalogEntity):
    connector_class: Optional[str] = Field(default=None, alias=CONNECTOR_CLASS_FIELD)
    type: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None
    topics: List[CatalogTopic] = Field(default_factory=list)

    @field_validator("topics", mode="before")
    @classmethod
    def default_empty_topics(cls, value: object) -> object:
        return value or []

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
        self._connectors: Optional[Dict[str, CatalogConnector]] = None

    def get_connectors(self) -> Dict[str, CatalogConnector]:
        if self._connectors is None:
            connectors = self.client.fetch_entities(
                CONNECTOR_CATALOG_QUERY, CONNECTOR_ROOT_KEY, CatalogConnector
            )
            self._connectors = index_by_name(connectors)
            self.report.catalog_connectors_fetched = len(self._connectors)
        return self._connectors

    def get_connector(self, connector_name: str) -> Optional[CatalogConnector]:
        return lookup_by_name(self.get_connectors(), connector_name)

    def close(self) -> None:
        self.client.close()
