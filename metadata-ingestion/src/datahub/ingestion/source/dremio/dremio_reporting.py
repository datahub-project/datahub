from dataclasses import dataclass, field
from typing import Dict, Optional

from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.sql_parsing.sql_parsing_aggregator import SqlAggregatorReport
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import (
    TopKDict,
    float_top_k_dict,
    int_top_k_dict,
)


@dataclass
class DremioSourceReport(
    SQLSourceReport,
    StaleEntityRemovalSourceReport,
    BaseTimeWindowReport,
):
    num_containers_failed: int = 0
    num_datasets_failed: int = 0
    # Set when the job-history fetch stopped early; the stateful time window
    # is then left in place so the next run re-reads it.
    query_fetch_incomplete: bool = False
    containers_scanned: int = 0
    containers_filtered: int = 0

    # Count of *lineage references* (not edges) dropped by
    # schema_pattern / dataset_pattern. Increments once per filtered
    # view-parent, and once per URN the SqlParsingAggregator rediscovers
    # during SQL parsing that fails the catalog-walk filters (via
    # _is_allowed_table). The same denied URN can be checked by the
    # aggregator multiple times (usage, lineage, operations), so a single
    # dropped dataset can contribute multiple increments; use it as a
    # "is the filter biting?" signal, not an exact edge count. Also counts
    # sys / INFORMATION_SCHEMA references, including the connector's own job
    # history reads, so it is not zero even with no patterns set.
    lineage_dropped_filtered: int = 0

    # Query-derived tables that pass the filters but that the catalog walk did
    # not find (dropped or temporary tables still in job history).
    lineage_dropped_not_in_catalog: LossyList[str] = field(default_factory=LossyList)
    # Query-derived names still carrying a `dremio.` prefix after one is
    # removed: a two-part reference the schema resolver qualified twice
    # (`dremio.dremio.space.t`). The aggregator would emit them under that URN.
    lineage_dropped_unresolved_names: LossyList[str] = field(default_factory=LossyList)
    # Datasets the fetch returned without column rows (e.g. their source is
    # offline). They are not emitted, but their query lineage is kept.
    datasets_without_columns: LossyList[str] = field(default_factory=LossyList)
    # Observed CREATE VIEW statements skipped for views whose current
    # definition the catalog registered; older DDL would add stale upstreams.
    num_view_ddl_queries_skipped: int = 0
    # The dataset fetch was cut short, so the catalog walk's names are not the
    # whole catalog; query lineage then falls back to the filters alone. Set
    # only through DremioAPIOperations._report_dataset_fetch_failure.
    dataset_fetch_incomplete: bool = False
    # One upstream URN per mapped Dremio source, to compare against the URNs the
    # source's own connector ingests.
    source_mapping_sample_upstream_urns: Dict[str, str] = field(default_factory=dict)

    api_calls_total: int = 0
    api_calls_by_method_and_path: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    api_call_secs_by_method_and_path: TopKDict[str, float] = field(
        default_factory=float_top_k_dict
    )

    sql_aggregator: Optional[SqlAggregatorReport] = None

    def report_container_scanned(self, name: str) -> None:
        self.containers_scanned += 1

    def report_container_filtered(self, container_name: str) -> None:
        self.containers_filtered += 1
        self.report_dropped(container_name)

    def report_entity_scanned(self, name: str, ent_type: str = "View") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "Table":
            self.tables_scanned += 1
        elif ent_type == "View":
            self.views_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")
