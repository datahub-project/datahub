from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class BigIDSourceReport(StaleEntityRemovalSourceReport):
    glossary_terms_emitted: int = 0
    glossary_nodes_emitted: int = 0
    tag_entities_emitted: int = 0
    datasets_enriched: int = 0
    datasets_created: int = 0
    columns_enriched: int = 0
    datasets_skipped_unstructured: int = 0
    datasets_enriched_unstructured: int = 0
    classifiers_without_glossary_id: int = 0
    classifier_terms_emitted: int = 0
    idsor_terms_emitted: int = 0
    findings_below_threshold: int = 0
    connections_without_platform: LossyList[str] = field(default_factory=LossyList)
    filtered_connections: LossyList[str] = field(default_factory=LossyList)
    objects_filtered_by_connection: int = 0
    filtered_datasets: LossyList[str] = field(default_factory=LossyList)
    objects_filtered_by_dataset_pattern: int = 0

    def report_connection_no_platform(self, conn_name: str) -> None:
        self.connections_without_platform.append(conn_name)

    def report_connection_filtered(self, conn_name: str) -> None:
        self.objects_filtered_by_connection += 1
        self.filtered_connections.append(conn_name)

    def report_dataset_filtered(self, fqn: str) -> None:
        self.objects_filtered_by_dataset_pattern += 1
        self.filtered_datasets.append(fqn)
