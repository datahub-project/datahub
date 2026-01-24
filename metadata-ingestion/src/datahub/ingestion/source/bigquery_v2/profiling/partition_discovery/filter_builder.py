"""Filter creation and validation utilities for partition queries."""

import logging
import re
from typing import Dict, List, Optional, Union

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BIGQUERY_NUMERIC_TYPES,
    DATE_TIME_TYPES,
    PARTITION_ID_YYYYMMDD_LENGTH,
    PARTITION_ID_YYYYMMDDHH_LENGTH,
    VALID_COLUMN_NAME_PATTERN,
)

logger = logging.getLogger(__name__)


class FilterBuilder:
    """Utilities for creating safe partition filters."""

    @staticmethod
    def create_safe_filter(
        col_name: str, val: Union[str, int, float], col_type: Optional[str] = None
    ) -> str:
        """
        Create safe partition filter with appropriate quoting based on column type.

        Handles:
        - Numeric types (INT64, FLOAT64, etc.) -> unquoted
        - Date/Time types (DATE, TIMESTAMP, DATETIME) -> formatted and quoted
        - String types -> quoted
        - Special date formats (YYYYMMDD, YYYYMM) -> converted to proper date format
        """
        if not VALID_COLUMN_NAME_PATTERN.match(col_name):
            raise ValueError(f"Invalid column name for filter: {col_name}")

        str_val = str(val)

        # Security check
        if any(pattern in str_val for pattern in [";", "--", "/*", "\\"]):
            raise ValueError(f"Invalid value for filter: {val}")

        # Handle numeric types (no quoting)
        if col_type and col_type.upper() in BIGQUERY_NUMERIC_TYPES:
            try:
                if "." in str_val:
                    float(str_val)
                else:
                    int(str_val)
                return f"`{col_name}` = {str_val}"
            except ValueError:
                logger.warning(
                    f"Non-numeric value '{str_val}' for numeric column {col_name} ({col_type}), using string comparison"
                )

        # Handle DATE/TIMESTAMP/DATETIME types
        if col_type and col_type.upper() in DATE_TIME_TYPES:
            formatted_val = FilterBuilder._format_date_value(str_val, col_type.upper())
            if formatted_val:
                # For TIMESTAMP columns, use TIMESTAMP() cast if value doesn't contain time
                if col_type.upper() == "TIMESTAMP" and " " not in formatted_val:
                    return f"`{col_name}` = TIMESTAMP('{formatted_val}')"
                return f"`{col_name}` = '{formatted_val}'"
            else:
                logger.warning(
                    f"Could not format date value '{str_val}' for {col_type} column {col_name}"
                )

        # Handle string types (with escaping)
        if "'" in str_val:
            escaped_val = str_val.replace("'", "''")
            return f"`{col_name}` = '{escaped_val}'"
        else:
            return f"`{col_name}` = '{str_val}'"

    @staticmethod
    def _format_date_value(val: str, col_type: str) -> Optional[str]:
        """
        Format a date value for BigQuery based on column type.

        Converts common partition formats to proper date/timestamp formats:
        - YYYYMMDD (8 digits) -> YYYY-MM-DD
        - YYYYMM (6 digits) -> YYYY-MM-01 (first day of month)
        - YYYYMMDDHH (10 digits) -> YYYY-MM-DD HH:00:00
        - YYYY-MM-DD -> kept as is
        - YYYY-MM-DD HH:MM:SS -> kept as is

        Returns None if format cannot be determined.
        """
        # Already in proper format
        if re.match(r"^\d{4}-\d{2}-\d{2}$", val):  # YYYY-MM-DD
            return val
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", val):  # Full timestamp
            return val

        # Convert YYYYMMDD (8 digits) to YYYY-MM-DD
        if re.match(r"^\d{8}$", val):
            return f"{val[:4]}-{val[4:6]}-{val[6:8]}"

        # Convert YYYYMM (6 digits) to YYYY-MM-01
        if re.match(r"^\d{6}$", val):
            return f"{val[:4]}-{val[4:6]}-01"

        # Convert YYYYMMDDHH (10 digits) to YYYY-MM-DD HH:00:00
        if re.match(r"^\d{10}$", val):
            date_part = f"{val[:4]}-{val[4:6]}-{val[6:8]}"
            hour_part = val[8:10]
            if col_type == "TIMESTAMP" or col_type == "DATETIME":
                return f"{date_part} {hour_part}:00:00"
            else:  # DATE type, just return date part
                return date_part

        # Can't determine format
        return None

    @staticmethod
    def convert_partition_id_to_filters(
        partition_id: str,
        required_columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
    ) -> Optional[List[str]]:
        """
        Convert partition_id from INFORMATION_SCHEMA.PARTITIONS to filter expressions.

        Handles: YYYYMMDD dates, YYYYMMDDHH timestamps, col1=val1$col2=val2 multi-column.
        """
        try:
            filters = []
            column_types = column_types or {}

            if "$" in partition_id:
                parts = partition_id.split("$")
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in required_columns:
                            col_type = column_types.get(col)
                            filters.append(
                                FilterBuilder.create_safe_filter(col, val, col_type)
                            )

            else:
                if len(required_columns) == 1:
                    col_name = required_columns[0]
                    col_type = column_types.get(col_name)

                    if (
                        len(partition_id) == PARTITION_ID_YYYYMMDD_LENGTH
                        and partition_id.isdigit()
                    ) or (
                        len(partition_id) == PARTITION_ID_YYYYMMDDHH_LENGTH
                        and partition_id.isdigit()
                    ):
                        date_str = f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                        filters.append(
                            FilterBuilder.create_safe_filter(
                                col_name, date_str, col_type
                            )
                        )
                    else:
                        # For other formats (including YYYYMM = 6 digits), pass type to enable conversion
                        filters.append(
                            FilterBuilder.create_safe_filter(
                                col_name, partition_id, col_type
                            )
                        )
                else:
                    logger.debug(
                        f"Complex partition mapping for {partition_id} with {len(required_columns)} columns"
                    )
                    return None

            return filters if filters else None

        except Exception as e:
            logger.warning(
                f"Error converting partition_id {partition_id} to filters: {e}"
            )
            return None
