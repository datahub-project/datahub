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
    server_groups_emitted: int = 0
    bridges_scanned: int = 0
    lineage_edges_emitted: int = 0
    lineage_edges_unresolved: int = 0
    column_lineage_edges_emitted: int = 0
    derived_schemas_emitted: int = 0
    derived_schema_fields_emitted: int = 0
    derived_fields_excluded: int = 0
    filtered_destinations: LossyList[str] = field(default_factory=LossyList)
    # Destinations a schema could not be derived for because nothing downstream of
    # them is in DataHub yet. Ingest the consuming platform first and re-run.
    destinations_without_consumers: LossyList[str] = field(default_factory=LossyList)
    # A field two consumers landed with different types. The first is kept, since
    # there is no evidence on the bus itself to break the tie.
    derived_field_type_conflicts: LossyList[str] = field(default_factory=LossyList)
    # Bridge endpoints that cannot be mapped to a concrete dataset (wildcard
    # subscriptions or endpoints with an unrecognised destination type).
    unresolved_bridge_endpoints: LossyList[str] = field(default_factory=LossyList)

    def report_destination_filtered(self, name: str) -> None:
        self.filtered_destinations.append(name)

    def report_bridge_endpoint_unresolved(self, name: str) -> None:
        self.unresolved_bridge_endpoints.append(name)

    def report_destination_without_consumers(self, name: str) -> None:
        self.destinations_without_consumers.append(name)

    def report_derived_field_type_conflict(
        self, destination: str, field_path: str, kept: str, discarded: str
    ) -> None:
        self.derived_field_type_conflicts.append(
            f"{destination}.{field_path}: kept {kept}, ignored {discarded}"
        )
