from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from datahub.ingestion.glossary.classification_mixin import ClassificationReportMixin
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.sql_parsing.sql_parsing_aggregator import SqlAggregatorReport
from datahub.utilities.lossy_collections import LossyDict
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict


@dataclass
class RedshiftReport(
    SQLSourceReport,
    IngestionStageReport,
    BaseTimeWindowReport,
    ClassificationReportMixin,
):
    num_usage_workunits_emitted: Optional[int] = None
    num_operational_stats_workunits_emitted: Optional[int] = None
    upstream_lineage: LossyDict = field(default_factory=LossyDict)
    usage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    lineage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    table_processed: TopKDict[str, int] = field(default_factory=TopKDict)
    table_filtered: TopKDict[str, int] = field(default_factory=TopKDict)
    view_filtered: TopKDict[str, int] = field(default_factory=TopKDict)
    view_processed: TopKDict[str, int] = field(default_factory=TopKDict)
    table_cached: TopKDict[str, int] = field(default_factory=TopKDict)
    view_cached: TopKDict[str, int] = field(default_factory=TopKDict)
    metadata_extraction_sec: TopKDict[str, float] = field(default_factory=TopKDict)
    operational_metadata_extraction_sec: TopKDict[str, float] = field(
        default_factory=TopKDict
    )
    lineage_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    tables_in_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    views_in_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    num_operational_stats_filtered: int = 0
    num_repeated_operations_dropped: int = 0
    num_usage_stat_skipped: int = 0
    num_lineage_tables_dropped: int = 0
    num_lineage_dropped_query_parser: int = 0
    num_lineage_dropped_not_support_copy_path: int = 0
    num_lineage_processed_temp_tables = 0
    num_lineage_dropped_s3_path: int = 0
    num_alter_table_parse_errors: int = 0

    lineage_start_time: Optional[datetime] = None
    lineage_end_time: Optional[datetime] = None
    stateful_lineage_ingestion_enabled: bool = False

    usage_start_time: Optional[datetime] = None
    usage_end_time: Optional[datetime] = None
    stateful_usage_ingestion_enabled: bool = False
    num_unresolved_temp_columns: int = 0

    # lineage/usage v2
    sql_aggregator: Optional[SqlAggregatorReport] = None
    lineage_phases_timer: Dict[str, PerfTimer] = field(default_factory=dict)

    def report_dropped(self, key: str) -> None:
        self.filtered.append(key)
