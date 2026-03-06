"""Reporting for Dataplex source."""

from dataclasses import dataclass, field
from typing import Dict

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.perf_timer import PerfTimer


@dataclass
class DataplexReport(StaleEntityRemovalSourceReport):
    """Report for Dataplex ingestion."""

    num_entry_groups_scanned: int = 0
    num_entries_scanned: int = 0

    num_entries_filtered: int = 0

    num_entry_groups_ingested: int = 0
    num_entries_ingested: int = 0

    num_lineage_relationships_created: int = 0
    num_lineage_entries_scanned: int = 0
    num_lineage_entries_failed: int = 0

    entries_scanned: Dict[str, bool] = field(default_factory=dict)

    lineage_api_timer: PerfTimer = field(default_factory=PerfTimer)
    catalog_api_timer: PerfTimer = field(default_factory=PerfTimer)

    def report_entry_group_scanned(self) -> None:
        """Report that an entry group was scanned."""
        self.num_entry_groups_scanned += 1
        self.num_entry_groups_ingested += 1

    def report_entry_scanned(self, entry_id: str, filtered: bool = False) -> None:
        """Report that an entry was scanned."""
        self.num_entries_scanned += 1
        self.entries_scanned[entry_id] = not filtered
        if filtered:
            self.num_entries_filtered += 1
        else:
            self.num_entries_ingested += 1

    def report_lineage_relationship_created(self) -> None:
        """Report that a lineage relationship was created."""
        self.num_lineage_relationships_created += 1


# Alias for consistency with other sources
DataplexSourceReport = DataplexReport
