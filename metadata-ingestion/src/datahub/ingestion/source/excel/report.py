from dataclasses import dataclass, field
from typing import List

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict


@dataclass
class ExcelSourceReport(StaleEntityRemovalSourceReport):
    files_scanned = 0
    files_processed = 0
    worksheets_scanned = 0
    worksheets_processed = 0
    datasets_profiled = 0
    local_file_get_timer: PerfTimer = field(default_factory=PerfTimer)
    s3_file_get_timer: PerfTimer = field(default_factory=PerfTimer)
    abs_file_get_timer: PerfTimer = field(default_factory=PerfTimer)
    filtered: List[str] = field(default_factory=list)
    profiling_skipped_other: TopKDict[str, int] = field(default_factory=int_top_k_dict)
    profiling_skipped_table_profile_pattern: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)

    def report_entity_profiled(self) -> None:
        self.datasets_profiled += 1

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_file_processed(self) -> None:
        self.files_processed += 1

    def report_worksheet_scanned(self) -> None:
        self.worksheets_scanned += 1

    def report_worksheet_processed(self) -> None:
        self.worksheets_processed += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)

    def report_worksheet_dropped(self, worksheet: str) -> None:
        self.filtered.append(worksheet)
