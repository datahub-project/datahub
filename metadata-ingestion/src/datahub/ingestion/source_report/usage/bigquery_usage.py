import collections
import dataclasses
from datetime import datetime
from typing import Counter, Optional

from datahub.ingestion.api.source import SourceReport


@dataclasses.dataclass
class BigQueryUsageSourceReport(SourceReport):
    dropped_table: Counter[str] = dataclasses.field(default_factory=collections.Counter)
    total_log_entries: int = 0
    num_read_events: int = 0
    num_query_events: int = 0

    use_v2_audit_metadata: bool = False
    log_page_size: int = 1000
    query_log_delay: int = 0

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    allow_pattern: str = ""
    deny_pattern: str = ""

    log_entry_start_time: str = ""
    log_entry_end_time: str = ""

    def report_dropped(self, key: str) -> None:
        self.dropped_table[key] += 1
