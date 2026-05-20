"""Source report for the Lightdash connector."""

from __future__ import annotations

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class LightdashSourceReport(StaleEntityRemovalSourceReport):
    """Extends DataHub's stale-entity report with Lightdash-specific counters.

    Numeric counters surface verbatim in ``datahub ingest`` output, so an
    operator can confirm "we emitted 13 charts and 2 dashboards" at a glance
    without grepping the structured log.
    """

    projects_scanned: int = 0
    projects_filtered: int = 0
    spaces_scanned: int = 0
    spaces_filtered: int = 0
    charts_scanned: int = 0
    charts_filtered: int = 0
    dashboards_scanned: int = 0
    dashboards_filtered: int = 0
    explores_resolved: int = 0
    explores_failed: list[str] = field(default_factory=list)
    warehouse_platform_fallbacks: list[str] = field(default_factory=list)
    # Table calculations live in the BI layer (computed from the query result
    # set) and have no warehouse-column lineage. We surface the count so
    # operators see they exist but no lineage edge was emitted for them.
    table_calculations_skipped: int = 0

    def report_explore_failed(self, key: str) -> None:
        self.explores_failed.append(key)

    def report_warehouse_platform_fallback(self, warehouse_type: str) -> None:
        if warehouse_type not in self.warehouse_platform_fallbacks:
            self.warehouse_platform_fallbacks.append(warehouse_type)

    def report_table_calculation_skipped(self, _name: str) -> None:
        self.table_calculations_skipped += 1
