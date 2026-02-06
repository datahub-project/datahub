from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.utilities.stats_collections import TopKDict


@dataclass
class GoogleSheetsSourceReport(
    StaleEntityRemovalSourceReport,
    IngestionStageReport,
):
    """Report for Google Sheets source ingestion."""

    sheets_scanned: int = 0
    sheets_dropped: List[str] = dataclass_field(default_factory=list)
    formulas_processed: int = 0
    lineage_edges_found: int = 0
    fine_grained_edges_found: int = 0
    sheets_profiled: int = 0
    num_sql_queries_parsed: int = 0
    num_sql_queries_failed: int = 0

    # Timers
    scan_sheets_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    extract_schema_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    extract_lineage_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    profile_sheet_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    usage_stats_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)

    def report_sheet_scanned(self) -> None:
        self.sheets_scanned += 1

    def report_sheet_dropped(self, sheet: str) -> None:
        self.sheets_dropped.append(sheet)

    def report_formulas_processed(self, count: int) -> None:
        self.formulas_processed += count

    def report_lineage_edge(self) -> None:
        self.lineage_edges_found += 1

    def report_fine_grained_edge(self) -> None:
        self.fine_grained_edges_found += 1

    def report_sheet_profiled(self) -> None:
        self.sheets_profiled += 1
