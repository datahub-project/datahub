from dataclasses import dataclass, field

from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class QuickSightSourceReport(StaleEntityRemovalSourceReport):
    """Per-entity counters and error buckets for a QuickSight ingestion run."""

    # The AWS account ID the run targeted (resolved via STS when not configured).
    aws_account_id: str = ""

    # Per-entity filter/processing counters.
    containers: EntityFilterReport = EntityFilterReport.field(type="container")
    data_sources: EntityFilterReport = EntityFilterReport.field(type="data_source")
    datasets: EntityFilterReport = EntityFilterReport.field(type="dataset")
    analyses: EntityFilterReport = EntityFilterReport.field(type="analysis")
    dashboards: EntityFilterReport = EntityFilterReport.field(type="dashboard")
    charts: EntityFilterReport = EntityFilterReport.field(type="chart")
    corp_users: EntityFilterReport = EntityFilterReport.field(type="corpuser")
    corp_groups: EntityFilterReport = EntityFilterReport.field(type="corpgroup")

    # Lineage counters.
    num_upstream_lineage_edges: int = 0
    num_column_lineage_edges: int = 0

    # Enrichment counters (ownership / tags).
    num_assets_with_owners: int = 0
    num_assets_with_tags: int = 0

    # Known degraded-path counters.
    num_file_datasets_summary_only: int = 0
    num_unknown_data_source_types: int = 0
    num_sqlglot_parse_failures: int = 0

    # Folders are an Enterprise-edition feature; record when they are unavailable.
    folders_unsupported: bool = field(default=False)
