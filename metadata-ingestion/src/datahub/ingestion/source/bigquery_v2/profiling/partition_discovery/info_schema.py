import logging
from typing import Callable, Dict, List, Optional

from google.cloud.bigquery import QueryJobConfig, Row, ScalarQueryParameter

from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
    BigqueryTable,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_NULL_PARTITION_ID,
    BQ_STREAMING_UNPARTITIONED_PARTITION_ID,
    BQ_UNPARTITIONED_PARTITION_ID,
)
from datahub.ingestion.source.bigquery_v2.profiling import queries
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    PARTITIONING_COLUMN_FLAG,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.filter_builder import (
    FilterBuilder,
)
from datahub.ingestion.source.bigquery_v2.profiling.reporting import warn
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_column_names,
)

logger = logging.getLogger(__name__)


class InfoSchemaQueries:
    def __init__(self, report: Optional[BigQueryV2Report] = None) -> None:
        self.report = report

    def get_partition_columns_from_info_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = queries.PARTITION_COLUMN_TYPES.format(
                info_schema_ref=safe_info_schema_ref, flag=PARTITIONING_COLUMN_FLAG
            )

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )

            partition_column_rows = execute_query_func(
                query, job_config, "partition columns from info schema"
            )

            # PARTITION_COLUMN_TYPES already selects data_type, so build the
            # {column -> type} map directly rather than issuing a second
            # INFORMATION_SCHEMA.COLUMNS round trip (and extra failure point).
            return {row.column_name: row.data_type for row in partition_column_rows}
        except Exception as e:
            warn(
                self.report,
                logger,
                title="Partition column discovery failed",
                message="Failed to read partition columns from INFORMATION_SCHEMA; "
                "the table will be treated as unpartitioned and may be full-scanned or skipped",
                context=f"{table.name}: {e}",
            )
            return {}

    def get_partition_column_types(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        if not partition_columns:
            return {}

        try:
            safe_columns = validate_column_names(
                partition_columns, "column type lookup"
            )

            if not safe_columns:
                logger.warning(f"No valid column names provided for table {table.name}")
                return {}

            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            column_conditions = []
            parameters = [ScalarQueryParameter("table_name", "STRING", table.name)]

            for i, col_name in enumerate(safe_columns):
                param_name = f"col_{i}"
                column_conditions.append(f"column_name = @{param_name}")
                parameters.append(ScalarQueryParameter(param_name, "STRING", col_name))

            column_filter_clause = " OR ".join(column_conditions)

            query = queries.PARTITION_COLUMN_TYPES_FILTERED.format(
                info_schema_ref=safe_info_schema_ref,
                column_filter_clause=column_filter_clause,
            )

            job_config = QueryJobConfig(query_parameters=parameters)

            query_results = execute_query_func(
                query, job_config, "partition column types"
            )
            return {row.column_name: row.data_type for row in query_results}
        except Exception as e:
            warn(
                self.report,
                logger,
                title="Partition column type lookup failed",
                message="Failed to read partition column data types from INFORMATION_SCHEMA; "
                "partition filters may be built with incorrect quoting",
                context=f"{table.name}: {e}",
            )
            return {}

    def get_partition_filters_from_information_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        verify_partition_has_data: Callable,
        column_types: Dict[str, str],
    ) -> Optional[List[str]]:
        if not required_columns:
            return []

        # A RANGE (integer) partition_id is the max bucket's inclusive floor, so it must
        # be scanned with `>=` rather than an equality that only matches the floor row.
        # Temporal (DATE/DATETIME/TIMESTAMP) granularity is inferred from the
        # partition_id shape inside convert_partition_id_to_filters.
        is_range_partition = (
            table.partition_info is not None
            and table.partition_info.type == RANGE_PARTITION_NAME
        )

        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.PARTITIONS"
            )

            query = queries.PARTITIONS_BY_MODIFIED.format(
                info_schema_ref=safe_info_schema_ref,
                null_id=BQ_NULL_PARTITION_ID,
                unpartitioned_id=BQ_UNPARTITIONED_PARTITION_ID,
                streaming_id=BQ_STREAMING_UNPARTITIONED_PARTITION_ID,
            )

            parameters = [
                ScalarQueryParameter("table_name", "STRING", table.name),
                ScalarQueryParameter("max_partitions", "INT64", 10),
            ]

            job_config = QueryJobConfig(query_parameters=parameters)

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

            if is_range_partition:
                # A RANGE bucket id is only its inclusive integer floor; the bucket width
                # is not exposed in INFORMATION_SCHEMA.PARTITIONS, so `col >= floor` cannot
                # be bounded to a single bucket by width. Selecting the *maximum* populated
                # bucket makes the lower-bound scan exact: nothing exists above the top
                # bucket, so `col >= max_floor` reads that bucket alone and cannot spill
                # into higher buckets (the bug that picking a mid-range, most-recently
                # modified bucket would cause).
                range_filters = self._range_partition_lower_bound_filters(
                    table,
                    project,
                    schema,
                    partition_rows,
                    required_columns,
                    column_types,
                    execute_query_func,
                    verify_partition_has_data,
                )
                if range_filters:
                    return range_filters
                return None

            partition_filters = []
            convert_failures = 0

            for partition_row in partition_rows:
                partition_id = partition_row.partition_id

                try:
                    filters_for_partition = (
                        FilterBuilder.convert_partition_id_to_filters(
                            partition_id,
                            required_columns,
                            column_types,
                            is_range_partition=is_range_partition,
                        )
                    )

                    if filters_for_partition:
                        if verify_partition_has_data(
                            table,
                            project,
                            schema,
                            filters_for_partition,
                            execute_query_func,
                        ):
                            partition_filters.extend(filters_for_partition)
                            break
                        else:
                            logger.debug(
                                f"Partition {partition_id} verification failed, trying next"
                            )

                except Exception as e:
                    convert_failures += 1
                    logger.debug(f"Error processing partition {partition_id}: {e}")
                    continue

            if partition_filters:
                return partition_filters

            # One summarizing warning on whole-set failure, not one per partition row.
            if convert_failures:
                warn(
                    self.report,
                    logger,
                    title="Partition filter discovery failed",
                    message="No partition id from INFORMATION_SCHEMA.PARTITIONS could be "
                    "converted to a filter; the table may be full-scanned or skipped.",
                    context=f"{table.name}: {convert_failures} partition id(s) failed to convert",
                )
            logger.debug(
                f"No valid partition filters from INFORMATION_SCHEMA for {table.name}"
            )
            return None

        except Exception as e:
            warn(
                self.report,
                logger,
                title="Partition filter discovery failed",
                message="Failed to derive partition filters from INFORMATION_SCHEMA.PARTITIONS",
                context=f"{table.name}: {e}",
            )
            return None

    def _range_partition_lower_bound_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_rows: List[Row],
        required_columns: List[str],
        column_types: Dict[str, str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        verify_partition_has_data: Callable,
    ) -> Optional[List[str]]:
        # Resolve the *global* maximum bucket floor so the `col >= floor` scan reads exactly
        # the top bucket and cannot spill into higher buckets. The fetched partition_rows are
        # ordered by last-modified (see PARTITIONS_BY_MODIFIED), so their max can be a
        # mid-range floor when the true top bucket is rarely modified; query the numeric max
        # directly and only fall back to the fetched rows if that query yields nothing.
        max_floor = self._query_max_range_bucket(
            table, project, schema, execute_query_func
        )
        if max_floor is None:
            for partition_row in partition_rows:
                partition_id = partition_row.partition_id
                if partition_id is None or not str(partition_id).lstrip("-").isdigit():
                    continue
                value = int(partition_id)
                if max_floor is None or value > max_floor:
                    max_floor = value

        if max_floor is None:
            return None

        try:
            filters_for_partition = FilterBuilder.convert_partition_id_to_filters(
                str(max_floor),
                required_columns,
                column_types,
                is_range_partition=True,
            )
        except Exception as e:
            logger.debug(f"Error building range partition filter for {table.name}: {e}")
            return None

        if filters_for_partition and verify_partition_has_data(
            table,
            project,
            schema,
            filters_for_partition,
            execute_query_func,
        ):
            return filters_for_partition
        return None

    def _query_max_range_bucket(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[int]:
        # Fetch the true numeric max RANGE bucket directly, independent of last-modified
        # ordering. Best-effort: any failure returns None so the caller falls back to the
        # already-fetched partition rows.
        try:
            info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.PARTITIONS"
            )
            query = queries.MAX_RANGE_PARTITION_ID.format(
                info_schema_ref=info_schema_ref,
                null_id=BQ_NULL_PARTITION_ID,
                unpartitioned_id=BQ_UNPARTITIONED_PARTITION_ID,
                streaming_id=BQ_STREAMING_UNPARTITIONED_PARTITION_ID,
            )
            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )
            rows = execute_query_func(query, job_config, "max range partition bucket")
            if rows and rows[0].partition_id is not None:
                return int(rows[0].partition_id)
        except Exception as e:
            logger.debug(f"Could not query max range bucket for {table.name}: {e}")
        return None
