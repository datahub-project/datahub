"""Reporting for Dataplex source."""

import logging
from dataclasses import dataclass, field
from threading import Lock

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


@dataclass
class DataplexReport(StaleEntityRemovalSourceReport):
    """Report for Dataplex ingestion."""

    # Thread safety
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    # Entry group tracking
    num_entry_groups_scanned: int = 0
    num_entry_groups_filtered: int = 0  # Filtered by pattern
    num_entry_groups_processed: int = 0  # Actually processed

    entry_groups_filtered: LossyList[str] = field(default_factory=lambda: LossyList())
    entry_groups_processed: LossyList[str] = field(default_factory=lambda: LossyList())

    # Entry tracking
    num_entries_without_fqn: int = (
        0  # Governance objects (glossaries, aspect types, entry group metadata)
    )
    num_entries_scanned: int = 0  # Entries with FQN (data assets)
    num_entries_filtered: int = 0  # Total filtered (by pattern or FQN pattern)
    num_entries_processed: int = 0  # Actually processed as datasets

    entries_without_fqn: LossyList[str] = field(default_factory=lambda: LossyList())
    entries_filtered: LossyList[str] = field(
        default_factory=lambda: LossyList()
    )  # Includes entries filtered by both pattern and fqn_pattern
    entries_processed: LossyList[str] = field(default_factory=lambda: LossyList())

    # Lineage tracking
    num_lineage_relationships_created: int = 0
    num_lineage_entries_scanned: int = 0
    num_lineage_entries_failed: int = 0

    lineage_api_timer: PerfTimer = field(default_factory=PerfTimer)
    catalog_api_timer: PerfTimer = field(default_factory=PerfTimer)

    def report_entry_group_scanned(
        self, entry_group_name: str, filtered: bool = False
    ) -> None:
        """Report that an entry group was scanned.

        Args:
            entry_group_name: Entry group name
            filtered: True if filtered by pattern, False if processed
        """
        with self._lock:
            self.num_entry_groups_scanned += 1
            if filtered:
                self.num_entry_groups_filtered += 1
                self.entry_groups_filtered.append(entry_group_name)
                logger.debug(
                    f"Entry group {entry_group_name} filtered out by entry_groups.pattern"
                )
            else:
                self.num_entry_groups_processed += 1
                self.entry_groups_processed.append(entry_group_name)
                logger.debug(f"Entry group {entry_group_name} will be processed")

    def report_entry_without_fqn(self, entry_id: str) -> None:
        """Report an entry without FQN (governance object, not a data asset)."""
        with self._lock:
            self.num_entries_without_fqn += 1
            self.entries_without_fqn.append(entry_id)
            logger.debug(f"Entry {entry_id} has no FQN (governance object), skipping")

    def report_entry_scanned(
        self,
        entry_name: str,
        filtered_by_pattern: bool = False,
        filtered_by_fqn_pattern: bool = False,
        fqn: str | None = None,
    ) -> None:
        """Report that an entry was scanned.

        Args:
            entry_name: Entry ID
            filtered_by_pattern: True if filtered by entry name pattern
            filtered_by_fqn_pattern: True if filtered by FQN pattern
            fqn: Fully qualified name (for logging context when filtered)
        """
        with self._lock:
            self.num_entries_scanned += 1
            if filtered_by_pattern or filtered_by_fqn_pattern:
                self.num_entries_filtered += 1
                self.entries_filtered.append(entry_name)
                if filtered_by_pattern:
                    logger.debug(f"Entry {entry_name} filtered out by entries.pattern")
                if filtered_by_fqn_pattern:
                    logger.debug(
                        f"Entry {entry_name} with FQN {fqn} filtered out by entries.fqn_pattern"
                    )
            else:
                self.num_entries_processed += 1
                self.entries_processed.append(entry_name)
                logger.debug(f"Entry {entry_name} will be processed")

    def report_lineage_relationship_created(self) -> None:
        """Report that a lineage relationship was created."""
        with self._lock:
            self.num_lineage_relationships_created += 1


# Alias for consistency with other sources
DataplexSourceReport = DataplexReport
