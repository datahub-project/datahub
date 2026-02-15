from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer


@dataclass
class FlinkSourceReport(StaleEntityRemovalSourceReport):
    """Report metrics for the Flink connector."""

    jobs_discovered: int = 0
    jobs_filtered_by_name: int = 0
    jobs_filtered_by_state: int = 0

    jobs_processed: int = 0
    jobs_failed: LossyList[str] = field(default_factory=LossyList)

    lineage_extracted: int = 0
    lineage_failed: int = 0
    lineage_sources_found: int = 0
    lineage_sinks_found: int = 0
    lineage_unclassified_nodes: LossyList[str] = field(default_factory=LossyList)

    dpis_emitted: int = 0

    flink_version: Optional[str] = None
    api_latency: PerfTimer = field(default_factory=PerfTimer)

    def report_job_discovered(self) -> None:
        self.jobs_discovered += 1

    def report_job_filtered_by_name(self, job_name: str) -> None:
        self.jobs_filtered_by_name += 1

    def report_job_filtered_by_state(self, job_name: str, state: str) -> None:
        self.jobs_filtered_by_state += 1

    def report_job_processed(self) -> None:
        self.jobs_processed += 1

    def report_job_failed(self, job_name: str) -> None:
        self.jobs_failed.append(job_name)

    def report_lineage(self, sources: int, sinks: int) -> None:
        self.lineage_extracted += 1
        self.lineage_sources_found += sources
        self.lineage_sinks_found += sinks
