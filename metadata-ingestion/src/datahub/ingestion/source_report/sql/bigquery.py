from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pydantic

from datahub.ingestion.source.sql.sql_common import SQLSourceReport


@dataclass
class BigQueryReport(SQLSourceReport):
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
    lineage_metadata_entries: Optional[int] = None
    include_table_lineage: Optional[bool] = None
    use_date_sharded_audit_log_tables: Optional[bool] = None
    log_page_size: Optional[pydantic.PositiveInt] = None
    use_v2_audit_metadata: Optional[bool] = None
    use_exported_bigquery_audit_metadata: Optional[bool] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    log_entry_start_time: Optional[str] = None
    log_entry_end_time: Optional[str] = None
    audit_start_time: Optional[str] = None
    audit_end_time: Optional[str] = None
    upstream_lineage: Dict = field(default_factory=dict)
    partition_info: Dict[str, str] = field(default_factory=dict)
    table_metadata: Dict[str, List[str]] = field(default_factory=dict)
    profile_table_selection_criteria: Dict[str, str] = field(default_factory=dict)
    selected_profile_tables: Dict[str, List[str]] = field(default_factory=dict)
