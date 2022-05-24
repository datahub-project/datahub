from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
)
from datahub.ingestion.source_report.sql.snowflake import BaseSnowflakeReport


@dataclass
class SnowflakeUsageReport(BaseSnowflakeReport, StatefulIngestionReport):
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
