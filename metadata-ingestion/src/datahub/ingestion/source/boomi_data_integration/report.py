from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class BoomiDataIntegrationSourceReport(StaleEntityRemovalSourceReport):
    api_calls: int = 0
    invalid_records: int = 0

    environments_scanned: int = 0
    environments_emitted: int = 0
    environments_filtered: LossyList[str] = field(default_factory=LossyList)
    environments_inaccessible: LossyList[str] = field(default_factory=LossyList)

    groups_emitted: int = 0
    groups_filtered: LossyList[str] = field(default_factory=LossyList)

    rivers_scanned: int = 0
    rivers_emitted: int = 0
    rivers_filtered: LossyList[str] = field(default_factory=LossyList)
    rivers_without_definition: int = 0
    rivers_without_definition_ids: LossyList[str] = field(default_factory=LossyList)
    river_definition_failures: LossyList[str] = field(default_factory=LossyList)

    logic_steps_emitted: int = 0
    logic_steps_disabled: int = 0
    logic_steps_invalid: int = 0

    target_tables_emitted: int = 0
    targets_object_storage_skipped: int = 0
    targets_unsupported: LossyList[str] = field(default_factory=LossyList)
    targets_missing_location: LossyList[str] = field(default_factory=LossyList)
    targets_missing_bigquery_project: LossyList[str] = field(default_factory=LossyList)
    targets_prefix_skipped: LossyList[str] = field(default_factory=LossyList)

    runs_scanned: int = 0
    runs_emitted: int = 0
    runs_over_cap: int = 0
    runs_river_not_emitted: int = 0
    runs_unknown_status: int = 0
    runs_unavailable_environments: LossyList[str] = field(default_factory=LossyList)

    def report_api_call(self) -> None:
        self.api_calls += 1
