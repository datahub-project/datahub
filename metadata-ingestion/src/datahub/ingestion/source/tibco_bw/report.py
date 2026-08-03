from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class TibcoBwSourceReport(StaleEntityRemovalSourceReport):
    scopes_scanned: int = 0
    applications_scanned: int = 0
    flows_emitted: int = 0
    jobs_emitted: int = 0
    jobs_with_lineage: int = 0
    lineage_iolets_emitted: int = 0
    column_lineage_edges_emitted: int = 0
    filtered_scopes: LossyList[str] = field(default_factory=LossyList)
    filtered_applications: LossyList[str] = field(default_factory=LossyList)

    # --- Application archives (EAR) ---
    archives_read: int = 0
    processes_read: int = 0
    jms_activities_found: int = 0
    destination_schemas_emitted: int = 0
    destination_lineage_edges_emitted: int = 0
    # A destination built by string concatenation at deploy or run time. Skipped
    # rather than guessed: a wrong name attaches the schema to the wrong topic.
    unresolved_destinations: LossyList[str] = field(default_factory=LossyList)
    activities_without_destination: LossyList[str] = field(default_factory=LossyList)
    activities_without_element: LossyList[str] = field(default_factory=LossyList)
    # The activity names an element no XSD in the archive declares - usually a
    # schema imported from a module that was not packaged in this archive.
    unresolved_elements: LossyList[str] = field(default_factory=LossyList)
    elements_without_fields: LossyList[str] = field(default_factory=LossyList)
    # The same element name declared by two schemas with different shapes; the
    # activity's prefix cannot disambiguate them, so neither is used.
    duplicate_schema_elements: LossyList[str] = field(default_factory=LossyList)

    def report_scope_filtered(self, name: str) -> None:
        self.filtered_scopes.append(name)

    def report_application_filtered(self, name: str) -> None:
        self.filtered_applications.append(name)

    def report_unresolved_destination(self, context: str) -> None:
        self.unresolved_destinations.append(context)

    def report_activity_without_destination(self, context: str) -> None:
        self.activities_without_destination.append(context)

    def report_activity_without_element(self, context: str) -> None:
        self.activities_without_element.append(context)

    def report_unresolved_element(self, context: str) -> None:
        self.unresolved_elements.append(context)

    def report_element_without_fields(self, context: str) -> None:
        self.elements_without_fields.append(context)

    def report_duplicate_schema_element(self, name: str) -> None:
        self.duplicate_schema_elements.append(name)
