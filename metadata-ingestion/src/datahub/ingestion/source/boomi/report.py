from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict


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

    lineage_edges_resolved: int = 0
    lineage_edges_unresolved: TopKDict[str, int] = field(default_factory=int_top_k_dict)
    lineage_skipped_subtypes: TopKDict[str, int] = field(default_factory=int_top_k_dict)
    lineage_listen_shapes_skipped: int = 0
    lineage_sql_parse_failures: int = 0
    lineage_components_fetched: int = 0
    lineage_component_failures: LossyList[str] = field(default_factory=LossyList)
    lineage_processes_incomplete: LossyList[str] = field(default_factory=LossyList)
    column_lineage_edges_emitted: int = 0
    column_lineage_maps_resolved: int = 0
    column_lineage_maps_skipped: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    column_lineage_fields_skipped: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    process_calls_resolved: int = 0
    process_calls_child_not_emitted: int = 0

    executions_scanned: int = 0
    executions_emitted: int = 0
    executions_unavailable: bool = False
    executions_no_runtimes: bool = False
    executions_process_not_emitted: int = 0
    executions_over_cap: int = 0
    executions_unknown_status: int = 0

    def report_api_call(self) -> None:
        self.api_calls += 1
