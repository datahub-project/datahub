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

    num_lakes_scanned: int = 0
    num_zones_scanned: int = 0
    num_assets_scanned: int = 0
    num_entities_scanned: int = 0
    num_entry_groups_scanned: int = 0
    num_entries_scanned: int = 0

    num_lakes_filtered: int = 0
    num_zones_filtered: int = 0
    num_assets_filtered: int = 0
    num_entities_filtered: int = 0
    num_entries_filtered: int = 0

    num_lakes_ingested: int = 0
    num_zones_ingested: int = 0
    num_assets_ingested: int = 0
    num_entities_ingested: int = 0
    num_entry_groups_ingested: int = 0
    num_entries_ingested: int = 0

    num_lineage_relationships_created: int = 0
    num_lineage_entries_scanned: int = 0
    num_lineage_entries_failed: int = 0

    lakes_scanned: Dict[str, bool] = field(default_factory=dict)
    zones_scanned: Dict[str, bool] = field(default_factory=dict)
    assets_scanned: Dict[str, bool] = field(default_factory=dict)
    entities_scanned: Dict[str, bool] = field(default_factory=dict)
    entries_scanned: Dict[str, bool] = field(default_factory=dict)

    dataplex_api_timer: PerfTimer = field(default_factory=PerfTimer)
    lineage_api_timer: PerfTimer = field(default_factory=PerfTimer)
    bigquery_api_timer: PerfTimer = field(default_factory=PerfTimer)
    catalog_api_timer: PerfTimer = field(default_factory=PerfTimer)

    def report_lake_scanned(self, lake_id: str, filtered: bool = False) -> None:
        """Report that a lake was scanned."""
        self.num_lakes_scanned += 1
        self.lakes_scanned[lake_id] = not filtered
        if filtered:
            self.num_lakes_filtered += 1
        else:
            self.num_lakes_ingested += 1

    def report_zone_scanned(self, zone_id: str, filtered: bool = False) -> None:
        """Report that a zone was scanned."""
        self.num_zones_scanned += 1
        self.zones_scanned[zone_id] = not filtered
        if filtered:
            self.num_zones_filtered += 1
        else:
            self.num_zones_ingested += 1

    def report_asset_scanned(self, asset_id: str, filtered: bool = False) -> None:
        """Report that an asset was scanned."""
        self.num_assets_scanned += 1
        self.assets_scanned[asset_id] = not filtered
        if filtered:
            self.num_assets_filtered += 1
        else:
            self.num_assets_ingested += 1

    def report_entity_scanned(self, entity_id: str, filtered: bool = False) -> None:
        """Report that an entity was scanned."""
        self.num_entities_scanned += 1
        self.entities_scanned[entity_id] = not filtered
        if filtered:
            self.num_entities_filtered += 1
        else:
            self.num_entities_ingested += 1

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
