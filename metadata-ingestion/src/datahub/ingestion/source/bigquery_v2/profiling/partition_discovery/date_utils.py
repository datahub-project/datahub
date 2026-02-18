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
        """
        Check if a column name suggests it contains date/time data.

        This is used as a fallback when column type information is unavailable or when
        date columns are stored as STRING/INT64 in external tables.

        See DATE_LIKE_COLUMN_NAMES in constants.py for the full list of patterns.
        """
        return col_name.lower() in DATE_LIKE_COLUMN_NAMES

    @staticmethod
    def is_date_type_column(data_type: str) -> bool:
        """
        Check if a column's data type is a date/time type in BigQuery.

        See DATE_TIME_TYPES in constants.py for the full list of recognized types.
        """
        if not data_type:
            return False

        return data_type.upper() in DATE_TIME_TYPES

    @staticmethod
    def get_column_ordering_strategy(col_name: str, data_type: str = "") -> str:
        """
        Get the appropriate ORDER BY strategy for a column based on its data type and name.
        Prioritizes data type over column name for reliable detection.
        """
        # Primary check: Use data type (most reliable)
        if DateUtils.is_date_type_column(data_type):
            return f"`{col_name}` DESC"  # Most recent first for date/timestamp types
        # Secondary check: Date component columns
        elif col_name.lower() in ["year", "month", "day"]:
            return f"`{col_name}` DESC"  # Most recent first for date components
        # Tertiary check: Column name patterns (fallback for date columns with non-date types like STRING)
        elif DateUtils.is_date_like_column(col_name):
            return f"`{col_name}` DESC"  # Most recent first for date-like names
        else:
            return "record_count DESC"  # Most populated first for other columns

    @staticmethod
    def get_strategic_candidate_dates() -> List[Tuple[datetime, str]]:
        """
        Get strategic candidate dates that are most likely to contain data in real-world BigQuery tables.
        Optimized to only check today and yesterday for cost efficiency.

        Returns:
            List of (datetime, description) tuples in priority order
        """
        now = datetime.now(timezone.utc)
        candidates = []

        # 1. Today (current date - most likely to have data)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        candidates.append((today, "today (current date)"))

        # 2. Yesterday (very common case)
        yesterday = now - timedelta(days=1)
        candidates.append((yesterday, "yesterday"))

        logger.debug(
            f"Generated {len(candidates)} strategic date candidates for partition discovery (optimized for cost)"
        )
        return candidates
