"""
Report class for Hive Metastore source.

This module contains the HiveMetastoreSourceReport class that tracks
statistics and issues during metadata extraction.
"""

import dataclasses
from typing import List

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclasses.dataclass
class HiveMetastoreSourceReport(StaleEntityRemovalSourceReport):
    """Report for HiveMetastoreSource."""

    tables_scanned: int = 0
    views_scanned: int = 0
    filtered: List[str] = dataclasses.field(default_factory=list)

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """Report that an entity was scanned."""
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1

    def report_dropped(self, name: str) -> None:
        """Report that an entity was filtered/dropped."""
        self.filtered.append(name)
