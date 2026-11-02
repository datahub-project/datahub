from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class BoomiSourceReport(StaleEntityRemovalSourceReport):
    api_calls: int = 0

    folders_scanned: int = 0
    folders_emitted: int = 0
    folders_filtered: LossyList[str] = field(default_factory=LossyList)

    environments_scanned: int = 0
    environments_filtered: LossyList[str] = field(default_factory=LossyList)
    deployments_scanned: int = 0
    deployments_unavailable: bool = False

    processes_scanned: int = 0
    processes_emitted: int = 0
    processes_duplicate_branch_copies: int = 0
    processes_filtered: LossyList[str] = field(default_factory=LossyList)
    processes_undeployed_skipped: LossyList[str] = field(default_factory=LossyList)
    process_xml_failures: LossyList[str] = field(default_factory=LossyList)
    invalid_records: int = 0

    executions_scanned: int = 0
    executions_emitted: int = 0
    executions_unavailable: bool = False
    executions_process_not_emitted: int = 0
    executions_over_cap: int = 0
    executions_unknown_status: int = 0

    def report_api_call(self) -> None:
        self.api_calls += 1
