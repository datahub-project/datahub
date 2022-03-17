import collections
import dataclasses
from datetime import datetime
from typing import Counter, Optional

from datahub.ingestion.api.source import SourceReport


@dataclasses.dataclass
class BigQueryUsageSourceReport(SourceReport):
    dropped_table: Counter[str] = dataclasses.field(default_factory=collections.Counter)
    total_log_entries: Optional[int] = None
    num_read_events: Optional[int] = None
    num_query_events: Optional[int] = None
    use_v2_audit_metadata: Optional[bool] = None
    log_page_size: Optional[int] = None
    query_log_delay: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    allow_pattern: Optional[str] = None
    deny_pattern: Optional[str] = None
    log_entry_start_time: Optional[str] = None
    log_entry_end_time: Optional[str] = None

    def report_dropped(self, key: str) -> None:
        self.dropped_table[key] += 1
