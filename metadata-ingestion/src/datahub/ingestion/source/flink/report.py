from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class FlinkSourceReport(StaleEntityRemovalSourceReport):
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
    catalogs_scanned: int = 0
    databases_scanned: int = 0
    tables_scanned: int = 0
    flink_version: Optional[str] = None

    def report_job_scanned(self) -> None:
        self.jobs_discovered += 1

    def report_job_filtered_by_name(self) -> None:
        self.jobs_filtered_by_name += 1

    def report_job_filtered_by_state(self) -> None:
        self.jobs_filtered_by_state += 1

    def report_job_processed(self) -> None:
        self.jobs_processed += 1

    def report_job_failed(
        self, job_name: str, error: str, exc: Optional[BaseException] = None
    ) -> None:
        self.jobs_failed.append(job_name)
        self.warning(
            title="Failed to process Flink job",
            message="Job metadata extraction failed. Job will be skipped.",
            context=f"job={job_name}, error={error}",
            exc=exc,
        )

    def report_lineage_extracted(self, sources: int, sinks: int) -> None:
        self.lineage_extracted += 1
        self.lineage_sources_found += sources
        self.lineage_sinks_found += sinks

    def report_lineage_failed(
        self,
        job_name: str,
        error: str,
        exc: Optional[BaseException] = None,
    ) -> None:
        self.lineage_failed += 1
        self.warning(
            title="Failed to extract lineage",
            message="Job lineage could not be extracted from execution plan.",
            context=f"job={job_name}, error={error}",
            exc=exc,
        )

    def report_lineage_unclassified(self, description: str) -> None:
        self.lineage_unclassified_nodes.append(description)

    def report_dpi_emitted(self) -> None:
        self.dpis_emitted += 1

    def report_catalog_scanned(self) -> None:
        self.catalogs_scanned += 1

    def report_database_scanned(self) -> None:
        self.databases_scanned += 1

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1
