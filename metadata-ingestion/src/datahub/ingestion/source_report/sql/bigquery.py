from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from datahub.ingestion.source.sql.sql_common import SQLSourceReport


@dataclass
class BigQueryReport(SQLSourceReport):
    num_total_log_entries: Optional[int] = None
    num_parsed_log_entires: Optional[int] = None
    num_total_audit_entries: Optional[int] = None
    num_parsed_audit_entires: Optional[int] = None
    lineage_metadata_entries: Optional[int] = None
    use_v2_audit_metadata: Optional[bool] = None
    use_exported_bigquery_audit_metadata: Optional[bool] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    log_entry_start_time: Optional[str] = None
    log_entry_end_time: Optional[str] = None
    audit_start_time: Optional[str] = None
    audit_end_time: Optional[str] = None
