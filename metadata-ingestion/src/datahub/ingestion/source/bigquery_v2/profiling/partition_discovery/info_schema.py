import logging
from typing import Callable, Dict, List, Optional

from google.cloud.bigquery import QueryJobConfig, Row, ScalarQueryParameter

from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
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

            partition_columns = [row.column_name for row in partition_column_rows]

            if partition_columns:
                return self.get_partition_column_types(
                    table, project, schema, partition_columns, execute_query_func
                )
            else:
                return {}
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

            partition_filters = []
            convert_failures = 0

            for partition_row in partition_rows:
                partition_id = partition_row.partition_id

                try:
                    filters_for_partition = (
                        FilterBuilder.convert_partition_id_to_filters(
                            partition_id, required_columns, column_types
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

            # Only a whole-set failure is operator-relevant: one summarizing warning
            # instead of one per partition row.
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
