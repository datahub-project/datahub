"""Ingestion report for the dlt connector."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class DltSourceReport(StaleEntityRemovalSourceReport):
    """Tracks ingestion metrics for the dlt connector."""

    # Entity counts
    pipelines_scanned: int = 0
    resources_scanned: int = 0
    run_history_loaded: int = 0

    # Filtered / dropped entities
    pipelines_filtered: LossyList[str] = dataclasses.field(default_factory=LossyList)
    resources_filtered: LossyList[str] = dataclasses.field(default_factory=LossyList)

    # Error tracking
    schema_read_errors: int = 0
    run_history_errors: int = 0

    def report_pipeline_scanned(self) -> None:
        """Increment count of pipelines successfully processed."""
        self.pipelines_scanned += 1

    def report_pipeline_filtered(self, pipeline_name: str) -> None:
        """Record a pipeline excluded by pipeline_pattern."""
        self.pipelines_filtered.append(pipeline_name)

    def report_resource_scanned(self) -> None:
        """Increment count of dlt resources/tables processed."""
        self.resources_scanned += 1

    def report_resource_filtered(self, resource_name: str) -> None:
        """Record a resource excluded (e.g. dlt system table)."""
        self.resources_filtered.append(resource_name)

    def report_run_history_loaded(self, count: int = 1) -> None:
        """Increment count of _dlt_loads rows emitted as DataProcessInstances."""
        self.run_history_loaded += count

    def report_schema_read_error(self) -> None:
        """Increment count of schema YAML / SDK read failures."""
        self.schema_read_errors += 1

    def report_run_history_error(self) -> None:
        """Increment count of _dlt_loads query failures."""
        self.run_history_errors += 1
