"""Date column detection and strategic date generation utilities."""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    DATE_LIKE_COLUMN_NAMES,
    DATE_TIME_TYPES,
)

logger = logging.getLogger(__name__)


class DateUtils:
    """Utilities for date column detection and strategic date generation."""

    @staticmethod
    def is_date_like_column(col_name: str) -> bool:
        """Check if a column name suggests it contains date/time data."""
        return col_name.lower() in DATE_LIKE_COLUMN_NAMES

    @staticmethod
    def is_date_type_column(data_type: str) -> bool:
        """Check if a column's data type is a BigQuery date/time type."""
        if not data_type:
            return False

        return data_type.upper() in DATE_TIME_TYPES

    @staticmethod
    def get_column_ordering_strategy(col_name: str, data_type: str = "") -> str:
        """Get the appropriate ORDER BY strategy for a column based on its data type and name."""
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
        """Get today and yesterday as candidate dates for partition discovery."""
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = now - timedelta(days=1)
        return [(today, "today"), (yesterday, "yesterday")]
