from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class SapMdgSourceReport(StaleEntityRemovalSourceReport):
    services_scanned: int = 0
    services_failed: int = 0
    entity_types_scanned: int = 0
    entity_sets_scanned: int = 0
    datasets_emitted: int = 0
    foreign_keys_emitted: int = 0
    foreign_keys_unresolved: int = 0
    lineage_edges_emitted: int = 0
    column_lineage_edges_emitted: int = 0
    filtered_entity_sets: LossyList[str] = field(default_factory=LossyList)
    failed_services: LossyList[str] = field(default_factory=LossyList)
    # Target business systems that have no configured/known platform mapping, so
    # their downstream dataset urn cannot be resolved.
    unresolved_target_systems: LossyList[str] = field(default_factory=LossyList)

    def report_entity_set_filtered(self, name: str) -> None:
        self.filtered_entity_sets.append(name)

    def report_service_failed(self, service: str) -> None:
        self.services_failed += 1
        self.failed_services.append(service)

    def report_target_system_unresolved(self, business_system: str) -> None:
        self.unresolved_target_systems.append(business_system)
