"""INFORMATION_SCHEMA query utilities for partition discovery."""

import logging
from typing import Callable, Dict, List, Optional, Union

from google.cloud.bigquery import QueryJobConfig, Row, ScalarQueryParameter

from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    DEFAULT_INFO_SCHEMA_PARTITIONS_LIMIT,
    PARTITION_ID_YYYYMMDD_LENGTH,
    PARTITION_ID_YYYYMMDDHH_LENGTH,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.filter_builder import (
    FilterBuilder,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.types import (
    PartitionResult,
)
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_column_names,
)

logger = logging.getLogger(__name__)


class InfoSchemaQueries:
    """Utilities for querying INFORMATION_SCHEMA for partition information."""

    @staticmethod
    def get_partition_columns_from_info_schema(
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        """
        Get partition columns from INFORMATION_SCHEMA using parameterized queries.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            execute_query_func: Function to execute queries safely

        Returns:
            Dictionary mapping column names to data types
        """
        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = f"""SELECT column_name, data_type
FROM {safe_info_schema_ref}
WHERE table_name = @table_name AND is_partitioning_column = 'YES'"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )

            partition_column_rows = execute_query_func(
                query, job_config, "partition columns from info schema"
            )

            partition_columns = [row.column_name for row in partition_column_rows]

            # Get data types for the partition columns
            if partition_columns:
                return InfoSchemaQueries.get_partition_column_types(
                    table, project, schema, partition_columns, execute_query_func
                )
            else:
                return {}
        except Exception as e:
            logger.warning(
                f"Error getting partition columns from INFORMATION_SCHEMA: {e}"
            )
            return {}

    @staticmethod
    def get_partition_column_types(
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        """Get data types for partition columns using parameterized queries."""
        if not partition_columns:
            return {}

        try:
            # Use utility for validation
            safe_columns = validate_column_names(
                partition_columns, "column type lookup"
            )

            if not safe_columns:
                logger.warning(f"No valid column names provided for table {table.name}")
                return {}

            # Build safe table reference
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            # Use parameterized query building
            column_conditions = []
            parameters = [ScalarQueryParameter("table_name", "STRING", table.name)]

            for i, col_name in enumerate(safe_columns):
                param_name = f"col_{i}"
                column_conditions.append(f"column_name = @{param_name}")
                parameters.append(ScalarQueryParameter(param_name, "STRING", col_name))

            column_filter_clause = " OR ".join(column_conditions)

            query = f"""SELECT column_name, data_type
FROM {safe_info_schema_ref}
WHERE table_name = @table_name 
AND ({column_filter_clause})"""

            job_config = QueryJobConfig(query_parameters=parameters)

            # Use utility for execution
            query_results = execute_query_func(
                query, job_config, "partition column types"
            )
            return {row.column_name: row.data_type for row in query_results}
        except Exception as e:
            logger.warning(f"Error getting partition column types: {e}")
            return {}

    @staticmethod
    def get_partition_info_from_information_schema(
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        max_results: int = DEFAULT_INFO_SCHEMA_PARTITIONS_LIMIT,
    ) -> PartitionResult:
        """Get partition information from INFORMATION_SCHEMA.PARTITIONS."""
        if not partition_columns:
            return PartitionResult(partition_values={}, row_count=None)

        try:
            safe_max_results = max(1, min(int(max_results), 1000))

            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.PARTITIONS"
            )

            query = f"""SELECT partition_id, total_rows
FROM {safe_info_schema_ref}
WHERE table_name = @table_name 
AND partition_id != '__NULL__'
AND partition_id != '__UNPARTITIONED__'
AND total_rows > 0
ORDER BY total_rows DESC
LIMIT @max_results"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name),
                    ScalarQueryParameter("max_results", "INT64", safe_max_results),
                ]
            )

            partition_info_results = execute_query_func(
                query, job_config, "partition info from information schema"
            )

            if not partition_info_results:
                logger.warning(
                    f"No partitions found in INFORMATION_SCHEMA for table {table.name}"
                )
                return PartitionResult(partition_values={}, row_count=None)

            # Take the partition with the most rows
            best_partition = partition_info_results[0]
            partition_id = best_partition.partition_id

            logger.debug(
                f"Found best partition {partition_id} with {best_partition.total_rows} rows"
            )

            partition_values: Dict[str, Union[str, int, float]] = {}
            row_count = (
                best_partition.total_rows
                if hasattr(best_partition, "total_rows")
                else None
            )

            if "$" in partition_id:
                # Multi-column partitioning with format: col1=val1$col2=val2$col3=val3
                parts = partition_id.split("$")
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in partition_columns:
                            if val.isdigit():
                                partition_values[col] = int(val)
                            else:
                                partition_values[col] = val
            else:
                # Single column partitioning
                if len(partition_columns) == 1:
                    col_name = partition_columns[0]
                    partition_values[col_name] = partition_id
                else:
                    InfoSchemaQueries._parse_single_partition_id_for_multiple_columns(
                        partition_id, partition_columns, partition_values
                    )

            if partition_values:
                logger.info(
                    f"Successfully obtained partition values from INFORMATION_SCHEMA for table {table.name}: {dict(partition_values)}"
                )

            return PartitionResult(
                partition_values=partition_values, row_count=row_count
            )

        except Exception as e:
            logger.warning(f"Error getting partition info from INFORMATION_SCHEMA: {e}")
            return PartitionResult(partition_values={}, row_count=None)

    @staticmethod
    def get_partition_filters_from_information_schema(
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        verify_partition_has_data: Callable,
        column_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """
        Get comprehensive partition filters using INFORMATION_SCHEMA.PARTITIONS.

        This is the optimal approach for internal BigQuery tables as it:
        - Uses pure metadata queries (no data scanning)
        - Gets comprehensive partition coverage
        - Applies date windowing efficiently
        - Returns multiple recent partitions for better profiling coverage

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            required_columns: Partition columns that need values
            execute_query_func: Function to execute queries safely
            verify_partition_has_data: Function to verify partition has data

        Returns:
            List of partition filter strings, or None if method fails
        """
        if not required_columns:
            return []

        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.PARTITIONS"
            )

            # Build comprehensive query with date windowing
            query_parts = [
                "SELECT partition_id, last_modified_time, total_rows",
                f"FROM {safe_info_schema_ref}",
                "WHERE table_name = @table_name",
                "AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__', '__STREAMING_UNPARTITIONED__')",
                "AND total_rows > 0",
            ]

            # For partition discovery, we want to find the actual latest partitions
            # Don't apply restrictive windowing here - the profiling stage will apply partition_datetime_window_days
            # This ensures we find tables with latest data even if it's outside the configured window
            logger.debug(
                f"Discovering partitions for {table.name} without restrictive windowing to find actual latest data. "
                f"Date windowing (partition_datetime_window_days) will be applied during profiling."
            )

            # Order by recency and limit results
            query_parts.extend(
                ["ORDER BY last_modified_time DESC", "LIMIT @max_partitions"]
            )

            query = " ".join(query_parts)

            # Build query parameters
            parameters = [
                ScalarQueryParameter("table_name", "STRING", table.name),
                ScalarQueryParameter(
                    "max_partitions", "INT64", 10
                ),  # Get multiple partitions
            ]

            # No windowing parameters needed - we want to discover actual latest partitions

            job_config = QueryJobConfig(query_parameters=parameters)

            logger.debug(
                "Querying INFORMATION_SCHEMA.PARTITIONS for comprehensive partition discovery"
            )
            partition_rows = execute_query_func(
                query,
                job_config,
                "comprehensive partition discovery from information schema",
            )

            if not partition_rows:
                logger.debug(
                    f"No partitions found in INFORMATION_SCHEMA for table {table.name}"
                )
                return None

            # Convert partition IDs to filters
            partition_filters = []

            for partition_row in partition_rows:
                partition_id = partition_row.partition_id

                try:
                    filters_for_partition = (
                        FilterBuilder.convert_partition_id_to_filters(
                            partition_id, required_columns, column_types
                        )
                    )

                    if filters_for_partition:
                        # Verify this partition has data (quick check)
                        if verify_partition_has_data(
                            table,
                            project,
                            schema,
                            filters_for_partition,
                            execute_query_func,
                        ):
                            partition_filters.extend(filters_for_partition)
                            logger.debug(
                                f"Added partition {partition_id} with {partition_row.total_rows} rows"
                            )
                            break  # Found one working partition, that's enough
                        else:
                            logger.debug(
                                f"Partition {partition_id} verification failed, trying next"
                            )

                except Exception as e:
                    logger.warning(f"Error processing partition {partition_id}: {e}")
                    continue

            if partition_filters:
                logger.info(
                    f"Successfully discovered {len(partition_filters)} partition filters from INFORMATION_SCHEMA for {table.name}"
                )
                return partition_filters
            else:
                logger.debug(
                    "No valid partition filters could be generated from INFORMATION_SCHEMA"
                )
                return None

        except Exception as e:
            logger.warning(f"Error in INFORMATION_SCHEMA partition discovery: {e}")
            return None

    @staticmethod
    def _parse_single_partition_id_for_multiple_columns(
        partition_id: str,
        partition_columns: List[str],
        result_values: Dict[str, Union[str, int, float]],
    ) -> None:
        """
        Parse a single partition_id when there are multiple partition columns.
        Common patterns: YYYYMMDD for year/month/day, YYYYMMDDHH for year/month/day/hour
        """
        if partition_id.isdigit():
            if len(partition_id) == PARTITION_ID_YYYYMMDD_LENGTH:  # YYYYMMDD
                year = partition_id[:4]
                month = partition_id[4:6]
                day = partition_id[6:8]

                for col in partition_columns:
                    col_lower = col.lower()
                    if col_lower in ["year", "yr"]:
                        result_values[col] = year
                    elif col_lower in ["month", "mo"]:
                        result_values[col] = month
                    elif col_lower in ["day", "dy"]:
                        result_values[col] = day

            elif len(partition_id) == PARTITION_ID_YYYYMMDDHH_LENGTH:  # YYYYMMDDHH
                year = partition_id[:4]
                month = partition_id[4:6]
                day = partition_id[6:8]
                hour = partition_id[8:10]

                for col in partition_columns:
                    col_lower = col.lower()
                    if col_lower in ["year", "yr"]:
                        result_values[col] = year
                    elif col_lower in ["month", "mo"]:
                        result_values[col] = month
                    elif col_lower in ["day", "dy"]:
                        result_values[col] = day
                    elif col_lower in ["hour", "hr"]:
                        result_values[col] = hour
