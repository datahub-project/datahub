from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.sql_parsing.sql_parsing_aggregator import SqlAggregatorReport
from datahub.utilities.stats_collections import (
    TopKDict,
    float_top_k_dict,
    int_top_k_dict,
)


@dataclass
class DremioSourceReport(
    SQLSourceReport,
    StaleEntityRemovalSourceReport,
    IngestionStageReport,
    BaseTimeWindowReport,
):
    num_containers_failed: int = 0
    num_datasets_failed: int = 0
    containers_scanned: int = 0
    containers_filtered: int = 0

    api_calls_total: int = 0
    api_calls_by_method_and_path: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    api_call_secs_by_method_and_path: TopKDict[str, float] = field(
        default_factory=float_top_k_dict
    )

    sql_aggregator: Optional[SqlAggregatorReport] = None

    def report_upstream_latency(self, start_time: datetime, end_time: datetime) -> None:
        # recording total combined latency is not very useful, keeping this method as a placeholder
        # for future implementation of min / max / percentiles etc.
        pass

    def report_container_scanned(self, name: str) -> None:
        """
        Record that a container was successfully scanned
        """
        self.containers_scanned += 1

    def report_container_filtered(self, container_name: str) -> None:
        """
        Record that a container was filtered out
        """
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
