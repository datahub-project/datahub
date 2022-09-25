import collections
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import Counter, Dict, List, Optional

import pydantic

from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class BigQueryV2Report(SQLSourceReport):
    num_total_lineage_entries: Optional[int] = None
    num_skipped_lineage_entries_missing_data: Optional[int] = None
    num_skipped_lineage_entries_not_allowed: Optional[int] = None
    num_skipped_lineage_entries_sql_parser_failure: Optional[int] = None
    num_skipped_lineage_entries_other: Optional[int] = None
    num_total_log_entries: Optional[int] = None
    num_parsed_log_entires: Optional[int] = None
    num_total_audit_entries: Optional[int] = None
    num_parsed_audit_entires: Optional[int] = None
    bigquery_audit_metadata_datasets_missing: Optional[bool] = None
    lineage_failed_extraction: LossyList[str] = field(default_factory=LossyList)
    lineage_metadata_entries: Optional[int] = None
    lineage_mem_size: Optional[str] = None
    lineage_extraction_sec: Dict[str, float] = field(default_factory=dict)
    usage_extraction_sec: Dict[str, float] = field(default_factory=dict)
    usage_failed_extraction: LossyList[str] = field(default_factory=LossyList)
    metadata_extraction_sec: Dict[str, float] = field(default_factory=dict)
    include_table_lineage: Optional[bool] = None
    use_date_sharded_audit_log_tables: Optional[bool] = None
    log_page_size: Optional[pydantic.PositiveInt] = None
    use_v2_audit_metadata: Optional[bool] = None
    use_exported_bigquery_audit_metadata: Optional[bool] = None
    end_time: Optional[datetime] = None
    log_entry_start_time: Optional[str] = None
    log_entry_end_time: Optional[str] = None
    audit_start_time: Optional[str] = None
    audit_end_time: Optional[str] = None
    upstream_lineage: LossyDict = field(default_factory=LossyDict)
    partition_info: Dict[str, str] = field(default_factory=dict)
    profile_table_selection_criteria: Dict[str, str] = field(default_factory=dict)
    selected_profile_tables: Dict[str, List[str]] = field(default_factory=dict)
    invalid_partition_ids: Dict[str, str] = field(default_factory=dict)
    allow_pattern: Optional[str] = None
    deny_pattern: Optional[str] = None
    num_usage_workunits_emitted: Optional[int] = None
    query_log_delay: Optional[int] = None
    total_query_log_entries: Optional[int] = None
    num_read_events: Optional[int] = None
    num_query_events: Optional[int] = None
    num_filtered_read_events: Optional[int] = None
    num_filtered_query_events: Optional[int] = None
    num_operational_stats_workunits_emitted: Optional[int] = None
    read_reasons_stat: Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    operation_types_stat: Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
