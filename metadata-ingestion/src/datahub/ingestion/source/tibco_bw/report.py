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
    filtered_scopes: LossyList[str] = field(default_factory=LossyList)
    filtered_applications: LossyList[str] = field(default_factory=LossyList)

    def report_scope_filtered(self, name: str) -> None:
        self.filtered_scopes.append(name)

    def report_application_filtered(self, name: str) -> None:
        self.filtered_applications.append(name)
