import collections
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Counter, Dict, List, Optional

import pydantic

from datahub.ingestion.api.report import Report
from datahub.ingestion.glossary.classification_mixin import ClassificationReportMixin
from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class BigQuerySchemaApiPerfReport(Report):
    list_projects: PerfTimer = field(default_factory=PerfTimer)
    list_datasets: PerfTimer = field(default_factory=PerfTimer)
    get_columns_for_dataset: PerfTimer = field(default_factory=PerfTimer)
    get_tables_for_dataset: PerfTimer = field(default_factory=PerfTimer)
    list_tables: PerfTimer = field(default_factory=PerfTimer)
    get_views_for_dataset: PerfTimer = field(default_factory=PerfTimer)
    get_snapshots_for_dataset: PerfTimer = field(default_factory=PerfTimer)


@dataclass
class BigQueryAuditLogApiPerfReport(Report):
    get_exported_log_entries: PerfTimer = field(default_factory=PerfTimer)
    list_log_entries: PerfTimer = field(default_factory=PerfTimer)


@dataclass
class BigQueryProcessingPerfReport(Report):
    sql_parsing_sec: PerfTimer = field(default_factory=PerfTimer)
    store_usage_event_sec: PerfTimer = field(default_factory=PerfTimer)
    usage_state_size: Optional[str] = None


@dataclass
class BigQueryV2Report(
    ProfilingSqlReport,
    IngestionStageReport,
    BaseTimeWindowReport,
    ClassificationReportMixin,
):
    num_total_lineage_entries: TopKDict[str, int] = field(default_factory=TopKDict)
    num_skipped_lineage_entries_missing_data: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_skipped_lineage_entries_not_allowed: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_lineage_entries_sql_parser_failure: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_skipped_lineage_entries_other: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_lineage_total_log_entries: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_lineage_parsed_log_entries: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_lineage_log_parse_failures: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    bigquery_audit_metadata_datasets_missing: Optional[bool] = None
    lineage_failed_extraction: LossyList[str] = field(default_factory=LossyList)
    lineage_metadata_entries: TopKDict[str, int] = field(default_factory=TopKDict)
    lineage_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    lineage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    usage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    num_usage_total_log_entries: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_usage_parsed_log_entries: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    usage_error_count: Dict[str, int] = field(default_factory=int_top_k_dict)

    num_usage_resources_dropped: int = 0
    num_usage_operations_dropped: int = 0
    operation_dropped: LossyList[str] = field(default_factory=LossyList)
    usage_failed_extraction: LossyList[str] = field(default_factory=LossyList)
    num_project_datasets_to_scan: Dict[str, int] = field(default_factory=TopKDict)
    metadata_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    include_table_lineage: Optional[bool] = None
    use_date_sharded_audit_log_tables: Optional[bool] = None
    log_page_size: Optional[pydantic.PositiveInt] = None
    use_exported_bigquery_audit_metadata: Optional[bool] = None
    log_entry_start_time: Optional[datetime] = None
    log_entry_end_time: Optional[datetime] = None
    audit_start_time: Optional[datetime] = None
    audit_end_time: Optional[datetime] = None
    upstream_lineage: LossyDict = field(default_factory=LossyDict)
    partition_info: Dict[str, str] = field(default_factory=TopKDict)
    profile_table_selection_criteria: Dict[str, str] = field(default_factory=TopKDict)
    selected_profile_tables: Dict[str, List[str]] = field(default_factory=TopKDict)
    profiling_skipped_invalid_partition_ids: Dict[str, str] = field(
        default_factory=TopKDict
    )
    profiling_skipped_invalid_partition_type: Dict[str, str] = field(
        default_factory=TopKDict
    )
    profiling_skipped_partition_profiling_disabled: List[str] = field(
        default_factory=LossyList
    )
    allow_pattern: Optional[str] = None
    deny_pattern: Optional[str] = None
    num_usage_workunits_emitted: int = 0
    total_query_log_entries: int = 0
    num_read_events: int = 0
    num_query_events: int = 0
    num_view_query_events: int = 0
    num_view_query_events_failed_sql_parsing: int = 0
    num_view_query_events_failed_table_identification: int = 0
    num_filtered_read_events: int = 0
    num_filtered_query_events: int = 0
    num_usage_query_hash_collisions: int = 0
    num_operational_stats_workunits_emitted: int = 0

    snapshots_scanned: int = 0

    num_view_definitions_parsed: int = 0
    num_view_definitions_failed_parsing: int = 0
    num_view_definitions_failed_column_parsing: int = 0
    view_definitions_parsing_failures: LossyList[str] = field(default_factory=LossyList)

    read_reasons_stat: Counter[str] = field(default_factory=collections.Counter)
    operation_types_stat: Counter[str] = field(default_factory=collections.Counter)

    exclude_empty_projects: Optional[bool] = None

    schema_api_perf: BigQuerySchemaApiPerfReport = field(
        default_factory=BigQuerySchemaApiPerfReport
    )
    audit_log_api_perf: BigQueryAuditLogApiPerfReport = field(
        default_factory=BigQueryAuditLogApiPerfReport
    )
    processing_perf: BigQueryProcessingPerfReport = field(
        default_factory=BigQueryProcessingPerfReport
    )

    lineage_start_time: Optional[datetime] = None
    lineage_end_time: Optional[datetime] = None
    stateful_lineage_ingestion_enabled: bool = False

    usage_start_time: Optional[datetime] = None
    usage_end_time: Optional[datetime] = None
    stateful_usage_ingestion_enabled: bool = False

    def set_ingestion_stage(self, project_id: str, stage: str) -> None:
        self.report_ingestion_stage_start(f"{project_id}: {stage}")
