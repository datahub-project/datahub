from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, MutableSet, Optional

from datahub.ingestion.api.report import Report
from datahub.ingestion.glossary.classification_mixin import ClassificationReportMixin
from datahub.ingestion.source.snowflake.constants import SnowflakeEdition
from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.sql_parsing.sql_parsing_aggregator import SqlAggregatorReport
from datahub.utilities.perf_timer import PerfTimer


@dataclass
class SnowflakeUsageAggregationReport(Report):
    query_secs: float = -1
    query_row_count: int = -1
    result_fetch_timer: PerfTimer = field(default_factory=PerfTimer)
    result_skip_timer: PerfTimer = field(default_factory=PerfTimer)
    result_map_timer: PerfTimer = field(default_factory=PerfTimer)
    users_map_timer: PerfTimer = field(default_factory=PerfTimer)
    queries_map_timer: PerfTimer = field(default_factory=PerfTimer)
    fields_map_timer: PerfTimer = field(default_factory=PerfTimer)


@dataclass
class SnowflakeUsageReport:
    min_access_history_time: Optional[datetime] = None
    max_access_history_time: Optional[datetime] = None
    access_history_range_query_secs: float = -1
    access_history_query_secs: float = -1

    rows_processed: int = 0
    rows_missing_query_text: int = 0
    rows_zero_base_objects_accessed: int = 0
    rows_zero_direct_objects_accessed: int = 0
    rows_missing_email: int = 0
    rows_parsing_error: int = 0

    usage_start_time: Optional[datetime] = None
    usage_end_time: Optional[datetime] = None
    stateful_usage_ingestion_enabled: bool = False

    usage_aggregation: SnowflakeUsageAggregationReport = (
        SnowflakeUsageAggregationReport()
    )


@dataclass
class SnowflakeReport(ProfilingSqlReport, BaseTimeWindowReport):
    num_table_to_table_edges_scanned: int = 0
    num_table_to_view_edges_scanned: int = 0
    num_view_to_table_edges_scanned: int = 0
    num_external_table_edges_scanned: int = 0
    ignore_start_time_lineage: Optional[bool] = None
    upstream_lineage_in_report: Optional[bool] = None
    upstream_lineage: Dict[str, List[str]] = field(default_factory=dict)

    lineage_start_time: Optional[datetime] = None
    lineage_end_time: Optional[datetime] = None
    stateful_lineage_ingestion_enabled: bool = False

    cleaned_account_id: str = ""
    run_ingestion: bool = False

    # https://community.snowflake.com/s/topic/0TO0Z000000Unu5WAC/releases
    saas_version: Optional[str] = None
    default_warehouse: Optional[str] = None
    default_db: Optional[str] = None
    default_schema: Optional[str] = None
    role: str = ""

    profile_if_updated_since: Optional[datetime] = None
    profile_candidates: Dict[str, List[str]] = field(default_factory=dict)

    # lineage/usage v2
    sql_aggregator: Optional[SqlAggregatorReport] = None


@dataclass
class SnowflakeV2Report(
    SnowflakeReport,
    SnowflakeUsageReport,
    StatefulIngestionReport,
    ClassificationReportMixin,
    IngestionStageReport,
):
    account_locator: Optional[str] = None
    region: Optional[str] = None

    schemas_scanned: int = 0
    databases_scanned: int = 0
    tags_scanned: int = 0

    include_usage_stats: bool = False
    include_operational_stats: bool = False
    include_technical_schema: bool = False
    include_column_lineage: bool = False

    table_lineage_query_secs: float = -1
    external_lineage_queries_secs: float = -1
    num_tables_with_known_upstreams: int = 0
    num_upstream_lineage_edge_parsing_failed: int = 0

    # Reports how many times we reset in-memory `functools.lru_cache` caches of data,
    # which occurs when we occur a different database / schema.
    # Should not be more than the number of databases / schemas scanned.
    # Maps (function name) -> (stat_name) -> (stat_value)
    lru_cache_info: Dict[str, Dict[str, int]] = field(default_factory=dict)

    # These will be non-zero if snowflake information_schema queries fail with error -
    # "Information schema query returned too much data. Please repeat query with more selective predicates.""
    # This will result in overall increase in time complexity
    num_get_tables_for_schema_queries: int = 0
    num_get_views_for_schema_queries: int = 0
    num_get_columns_for_table_queries: int = 0

    # these will be non-zero if the user choses to enable the extract_tags = "with_lineage" option, which requires
    # individual queries per object (database, schema, table) and an extra query per table to get the tags on the columns.
    num_get_tags_for_object_queries: int = 0
    num_get_tags_on_columns_for_table_queries: int = 0

    rows_zero_objects_modified: int = 0

    _processed_tags: MutableSet[str] = field(default_factory=set)
    _scanned_tags: MutableSet[str] = field(default_factory=set)

    edition: Optional[SnowflakeEdition] = None

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table or a schema or a database
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        elif ent_type == "schema":
            self.schemas_scanned += 1
        elif ent_type == "database":
            self.databases_scanned += 1
        elif ent_type == "tag":
            # the same tag can be assigned to multiple objects, so we need
            # some extra logic account for each tag only once.
            if self._is_tag_scanned(name):
                return
            self._scanned_tags.add(name)
            self.tags_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    def is_tag_processed(self, tag_name: str) -> bool:
        return tag_name in self._processed_tags

    def _is_tag_scanned(self, tag_name: str) -> bool:
        return tag_name in self._scanned_tags

    def report_tag_processed(self, tag_name: str) -> None:
        self._processed_tags.add(tag_name)

    def set_ingestion_stage(self, database: str, stage: str) -> None:
        self.report_ingestion_stage_start(f"{database}: {stage}")
