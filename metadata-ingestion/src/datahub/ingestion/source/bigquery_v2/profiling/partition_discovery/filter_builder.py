"""Filter creation and validation utilities for partition queries."""

import logging
from typing import List, Optional, Union

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
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
        Create a safe partition filter with upstream validation of inputs.

        This ensures we only create filters with safe, validated inputs before
        they reach the downstream validation. Always uses string quoting for all values
        to rely on BigQuery's implicit type casting, which handles most type
        conversions automatically and avoids type mismatch errors between
        schema-declared types and actual stored values.
        """
        # Validate column name
        if not VALID_COLUMN_NAME_PATTERN.match(col_name):
            raise ValueError(f"Invalid column name for filter: {col_name}")

        # Convert value to string for consistent handling
        str_val = str(val)

        # Check for SQL injection patterns
        if any(pattern in str_val for pattern in [";", "--", "/*", "\\"]):
            raise ValueError(f"Invalid value for filter: {val}")

        # Always quote values to avoid type mismatch issues
        # BigQuery's implicit casting handles STRING -> INT64, STRING -> DATE, etc.
        # This is safer than trying to guess the correct format based on schema types
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
        Convert a partition_id from INFORMATION_SCHEMA.PARTITIONS to filter expressions.

        Handles various BigQuery partition formats:
        - Single column: "20231225" -> date_col = "2023-12-25"
        - Multi-column: "col1=val1$col2=val2" -> col1 = "val1" AND col2 = "val2"
        - Date formats: YYYYMMDD, YYYYMMDDHH, etc.
        """
        try:
            filters = []

            if "$" in partition_id:
                # Multi-column partitioning: col1=val1$col2=val2$col3=val3
                parts = partition_id.split("$")
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in required_columns:
                            filters.append(FilterBuilder.create_safe_filter(col, val))

            else:
                # Single column partitioning - need to map to the right column
                if len(required_columns) == 1:
                    col_name = required_columns[0]

                    # Handle date partition formats
                    if (
                        len(partition_id) == PARTITION_ID_YYYYMMDD_LENGTH
                        and partition_id.isdigit()
                    ):
                        # YYYYMMDD format
                        date_str = f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                        filters.append(
                            FilterBuilder.create_safe_filter(col_name, date_str)
                        )
                    elif (
                        len(partition_id) == PARTITION_ID_YYYYMMDDHH_LENGTH
                        and partition_id.isdigit()
                    ):
                        # YYYYMMDDHH format
                        date_str = f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                        filters.append(
                            FilterBuilder.create_safe_filter(col_name, date_str)
                        )
                    else:
                        # Use partition_id as-is
                        filters.append(
                            FilterBuilder.create_safe_filter(col_name, partition_id)
                        )
                else:
                    # Multiple columns but single partition_id - this is complex
                    # Try to parse based on common patterns or fall back
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
