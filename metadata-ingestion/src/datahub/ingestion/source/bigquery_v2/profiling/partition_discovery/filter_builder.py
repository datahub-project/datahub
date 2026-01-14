"""Filter creation and validation utilities for partition queries."""

import logging
from typing import List, Optional, Union

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    BIGQUERY_NUMERIC_TYPES,
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
        """Create safe partition filter with appropriate quoting based on column type."""
        if not VALID_COLUMN_NAME_PATTERN.match(col_name):
            raise ValueError(f"Invalid column name for filter: {col_name}")

        str_val = str(val)

        if any(pattern in str_val for pattern in [";", "--", "/*", "\\"]):
            raise ValueError(f"Invalid value for filter: {val}")

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

        if "'" in str_val:
            escaped_val = str_val.replace("'", "''")
            return f"`{col_name}` = '{escaped_val}'"
        else:
            return f"`{col_name}` = '{str_val}'"

    @staticmethod
    def convert_partition_id_to_filters(
        partition_id: str, required_columns: List[str]
    ) -> Optional[List[str]]:
        """
        Convert partition_id from INFORMATION_SCHEMA.PARTITIONS to filter expressions.

        Handles: YYYYMMDD dates, YYYYMMDDHH timestamps, col1=val1$col2=val2 multi-column.
        """
        try:
            filters = []

            if "$" in partition_id:
                parts = partition_id.split("$")
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in required_columns:
                            filters.append(FilterBuilder.create_safe_filter(col, val))

            else:
                if len(required_columns) == 1:
                    col_name = required_columns[0]

                    if (
                        len(partition_id) == PARTITION_ID_YYYYMMDD_LENGTH
                        and partition_id.isdigit()
                    ) or (
                        len(partition_id) == PARTITION_ID_YYYYMMDDHH_LENGTH
                        and partition_id.isdigit()
                    ):
                        date_str = f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                        filters.append(
                            FilterBuilder.create_safe_filter(col_name, date_str)
                        )
                    else:
                        filters.append(
                            FilterBuilder.create_safe_filter(col_name, partition_id)
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
