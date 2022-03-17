from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from datahub.ingestion.source.sql.sql_common import SQLSourceReport


@dataclass
class BigQueryReport(SQLSourceReport):
    num_total_log_entries: int = 0
    num_parsed_log_entires: int = 0
    num_total_audit_entries: int = 0
    num_parsed_audit_entires: int = 0
    lineage_metadata_entries: int = 0
    use_v2_audit_metadata: bool = False
    use_exported_bigquery_audit_metadata: bool = False

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    log_entry_start_time: str = ""
    log_entry_end_time: str = ""

    audit_start_time: str = ""
    audit_end_time: str = ""
