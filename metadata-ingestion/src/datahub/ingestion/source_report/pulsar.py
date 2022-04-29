from dataclasses import dataclass, field
from typing import List, Optional

from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
)


@dataclass
class PulsarSourceReport(StatefulIngestionReport):
    pulsar_version: Optional[str] = None
    tenants_scanned: Optional[int] = None
    namespaces_scanned: Optional[int] = None
    topics_scanned: Optional[int] = None
    tenants_filtered: List[str] = field(default_factory=list)
    namespaces_filtered: List[str] = field(default_factory=list)
    topics_filtered: List[str] = field(default_factory=list)
    soft_deleted_stale_entities: List[str] = field(default_factory=list)

    def report_pulsar_version(self, version: str) -> None:
        self.pulsar_version = version

    def report_tenants_dropped(self, tenant: str) -> None:
        self.tenants_filtered.append(tenant)

    def report_namespaces_dropped(self, namespace: str) -> None:
        self.namespaces_filtered.append(namespace)

    def report_topics_dropped(self, topic: str) -> None:
        self.topics_filtered.append(topic)

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)
