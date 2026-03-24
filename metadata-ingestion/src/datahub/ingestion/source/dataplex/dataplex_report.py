"""Reporting for Dataplex source."""

from dataclasses import dataclass, field

from datahub.ingestion.source.dataplex.dataplex_entries import DataplexEntriesReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.perf_timer import PerfTimer


@dataclass
class DataplexReport(StaleEntityRemovalSourceReport):
    """Report for Dataplex ingestion."""

    num_lineage_relationships_created: int = 0
    num_lineage_entries_processed: int = 0
    num_lineage_entries_scanned: int = 0
    num_lineage_entries_without_lineage: int = 0
    num_lineage_entries_skipped_unsupported_type: int = 0
    num_lineage_upstream_fqns_skipped: int = 0
    num_lineage_entries_failed: int = 0

    entries_report: DataplexEntriesReport = field(default_factory=DataplexEntriesReport)

    lineage_api_timer: PerfTimer = field(default_factory=PerfTimer)
    catalog_api_timer: PerfTimer = field(default_factory=PerfTimer)

    def report_entry_group(self, entry_group_name: str, filtered: bool) -> None:
        self.entries_report.report_entry_group(entry_group_name, filtered)

    def report_entry(
        self,
        entry_name: str,
        filtered_missing_fqn: bool,
        filtered_fqn: bool,
        filtered_name: bool,
    ) -> None:
        self.entries_report.report_entry(
            entry_name=entry_name,
            filtered_missing_fqn=filtered_missing_fqn,
            filtered_fqn=filtered_fqn,
            filtered_name=filtered_name,
        )

    def report_lineage_relationship_created(self) -> None:
        """Report that a lineage relationship was created."""
        self.num_lineage_relationships_created += 1


# Alias for consistency with other sources
DataplexSourceReport = DataplexReport
