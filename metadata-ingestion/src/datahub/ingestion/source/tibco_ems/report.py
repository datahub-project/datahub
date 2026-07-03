from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class TibcoEmsSourceReport(StaleEntityRemovalSourceReport):
    queues_scanned: int = 0
    topics_scanned: int = 0
    datasets_emitted: int = 0
    bridges_scanned: int = 0
    lineage_edges_emitted: int = 0
    lineage_edges_unresolved: int = 0
    filtered_destinations: LossyList[str] = field(default_factory=LossyList)
    # Bridge endpoints that cannot be mapped to a concrete dataset (wildcard
    # subscriptions or endpoints with an unrecognised destination type).
    unresolved_bridge_endpoints: LossyList[str] = field(default_factory=LossyList)

    def report_destination_filtered(self, name: str) -> None:
        self.filtered_destinations.append(name)

    def report_bridge_endpoint_unresolved(self, name: str) -> None:
        self.unresolved_bridge_endpoints.append(name)
