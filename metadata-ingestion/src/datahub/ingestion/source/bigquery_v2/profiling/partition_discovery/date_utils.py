import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    DATE_LIKE_COLUMN_NAMES,
    DATE_TIME_TYPES,
)

logger = logging.getLogger(__name__)


class DateUtils:
    @staticmethod
    def is_date_like_column(col_name: str) -> bool:
        return col_name.lower() in DATE_LIKE_COLUMN_NAMES

    @staticmethod
    def is_date_type_column(data_type: str) -> bool:
        if not data_type:
            return False

        return data_type.upper() in DATE_TIME_TYPES

    @staticmethod
    def get_column_ordering_strategy(col_name: str, data_type: str = "") -> str:
        if (
            DateUtils.is_date_type_column(data_type)
            or col_name.lower() in ["year", "month", "day"]
            or DateUtils.is_date_like_column(col_name)
        ):
            return f"`{col_name}` DESC"
        else:
            return "record_count DESC"

    @staticmethod
    def get_strategic_candidate_dates() -> List[Tuple[datetime, str]]:
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = now - timedelta(days=1)
        return [(today, "today"), (yesterday, "yesterday")]
