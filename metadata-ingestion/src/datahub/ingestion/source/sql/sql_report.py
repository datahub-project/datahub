from dataclasses import dataclass, field
from typing import Dict, Optional

from datahub.ingestion.glossary.classification_mixin import ClassificationReportMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlAggregatorReport
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombinerReport
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict


@dataclass
class DetailedProfilerReportMixin:
    profiling_skipped_not_updated: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    profiling_skipped_size_limit: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    profiling_skipped_row_limit: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    profiling_skipped_table_profile_pattern: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    profiling_skipped_other: TopKDict[str, int] = field(default_factory=int_top_k_dict)

    num_tables_not_eligible_profiling: Dict[str, int] = field(
        default_factory=int_top_k_dict
    )


@dataclass
class SQLSourceReport(
    StaleEntityRemovalSourceReport,
    ClassificationReportMixin,
    DetailedProfilerReportMixin,
):
    tables_scanned: int = 0
    views_scanned: int = 0
    entities_profiled: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    query_combiner: Optional[SQLAlchemyQueryCombinerReport] = None

    num_view_definitions_parsed: int = 0
    num_view_definitions_view_urn_mismatch: int = 0
    num_view_definitions_failed_parsing: int = 0
    num_view_definitions_failed_column_parsing: int = 0
    view_definitions_parsing_failures: LossyList[str] = field(default_factory=LossyList)
    sql_aggregator: Optional[SqlAggregatorReport] = None

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_from_query_combiner(
        self, query_combiner_report: SQLAlchemyQueryCombinerReport
    ) -> None:
        self.query_combiner = query_combiner_report
