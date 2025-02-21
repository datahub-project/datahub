import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
)
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


class BigqueryProfiler(GenericProfiler):
    config: BigQueryV2Config
    report: BigQueryV2Report

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        super().__init__(config, report, "bigquery", state_handler)
        self.config = config
        self.report = report

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime]
    ) -> Tuple[datetime, datetime]:
        partition_range_map: Dict[int, Tuple[relativedelta, str]] = {
            4: (relativedelta(years=1), "%Y"),
            6: (relativedelta(months=1), "%Y%m"),
            8: (relativedelta(days=1), "%Y%m%d"),
            10: (relativedelta(hours=1), "%Y%m%d%H"),
        }

        duration: relativedelta
        if partition_range_map.get(len(partition_id)):
            (delta, format) = partition_range_map[len(partition_id)]
            duration = delta
            if not partition_datetime:
                partition_datetime = datetime.strptime(partition_id, format)
            else:
                partition_datetime = datetime.strptime(
                    partition_datetime.strftime(format), format
                )
        else:
            raise ValueError(
                f"check your partition_id {partition_id}. It must be yearly/monthly/daily/hourly."
            )
        upper_bound_partition_datetime = partition_datetime + duration
        return partition_datetime, upper_bound_partition_datetime

    def _get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """Get partition filters for all required partition columns."""
        current_time = datetime.now(timezone.utc)
        partition_filters = []

        # Get required partition columns from table info
        required_partition_columns = set()

        # Get from explicit partition_info if available
        if table.partition_info:
            if isinstance(table.partition_info.fields, list):
                required_partition_columns.update(table.partition_info.fields)

            if table.partition_info.columns:
                required_partition_columns.update(
                    col.name for col in table.partition_info.columns if col
                )

        # If no partition columns found
        if not required_partition_columns:
            logger.debug(f"No partition columns found for table {table.name}")
            if table.external:
                # For external tables, check if it has external partitioning
                try:
                    query = f"""
                    SELECT column_name
                    FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
                    WHERE table_name = '{table.name}'
                    AND is_partitioning_column = 'YES'
                    """
                    query_job = self.config.get_bigquery_client().query(query)
                    results = list(query_job)

                    if results:
                        required_partition_columns.update(
                            row.column_name for row in results
                        )
                        logger.debug(
                            f"Found external partition columns: {required_partition_columns}"
                        )
                    else:
                        # No partitions found at all - this is valid for external tables
                        return []
                except Exception as e:
                    logger.error(f"Error checking external table partitioning: {e}")
                    return None
            else:
                # Internal table with no partitions - this is unexpected
                return None

        logger.debug(f"Required partition columns: {required_partition_columns}")

        # Add filters for each partition column
        for col_name in required_partition_columns:
            # Handle standard time-based columns first
            col_name_lower = col_name.lower()
            if col_name_lower == "year":
                partition_filters.append(f"`{col_name}` = {current_time.year}")
            elif col_name_lower == "month":
                partition_filters.append(f"`{col_name}` = {current_time.month}")
            elif col_name_lower == "day":
                partition_filters.append(f"`{col_name}` = {current_time.day}")
            else:
                try:
                    # Query for a valid partition value
                    query = f"""
                    SELECT DISTINCT {col_name} as val
                    FROM `{project}.{schema}.{table.name}`
                    WHERE {col_name} IS NOT NULL
                    ORDER BY {col_name} DESC  -- Get most recent partition by default
                    LIMIT 1
                    """
                    query_job = self.config.get_bigquery_client().query(query)
                    results = list(query_job)

                    if not results or results[0].val is None:
                        logger.error(
                            f"No values found for required partition column {col_name}"
                        )
                        return None

                    val = results[0].val

                    if isinstance(val, (int, float)):
                        partition_filters.append(f"`{col_name}` = {val}")
                    else:
                        partition_filters.append(f"`{col_name}` = '{val}'")

                except Exception as e:
                    logger.error(f"Error getting partition value for {col_name}: {e}")
                    return None

        return partition_filters

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """Handle partition-aware querying for all operations including COUNT."""
        bq_table = cast(BigqueryTable, table)
        base_kwargs = {
            "schema": db_name,  # <project>
            "table": f"{schema_name}.{table.name}",  # <dataset>.<table>
            "project": db_name,
            "dataset": schema_name,
            "table_name": bq_table.name,
        }

        # For external tables, add specific handling
        if bq_table.external:
            base_kwargs["is_external"] = "true"
            # Add any specific external table options needed

        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        if partition_filters is None:
            logger.warning(
                f"Could not construct partition filters for {bq_table.name}. "
                "This may cause partition elimination errors."
            )
            return base_kwargs

        # If no partition filters needed (e.g. some external tables), return base kwargs
        if not partition_filters:
            return base_kwargs

        # Construct query with partition filters
        partition_where = " AND ".join(partition_filters)
        logger.debug(f"Using partition filters: {partition_where}")

        custom_sql = f"""SELECT * 
FROM `{db_name}.{schema_name}.{table.name}`
WHERE {partition_where}"""

        base_kwargs.update({"custom_sql": custom_sql, "partition_handling": "true"})

        return base_kwargs

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        """Get profile request with appropriate partition handling."""
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)

        # Skip external tables if configured to do so
        if bq_table.external and not self.config.profiling.profile_external_tables:
            self.report.report_warning(
                title="Profiling skipped for external table",
                message="profiling.profile_external_tables is disabled",
                context=profile_request.pretty_name,
            )
            return None

        # Get partition filters
        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        # If we got None back, that means there was an error getting partition filters
        if partition_filters is None:
            self.report.report_warning(
                title="Profile skipped for partitioned table",
                message="Could not construct partition filters - required for partition elimination",
                context=profile_request.pretty_name,
            )
            return None

        if not self.config.profiling.partition_profiling_enabled:
            logger.debug(
                f"{profile_request.pretty_name} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        # Only add partition handling if we actually have partition filters
        if partition_filters:
            partition_where = " AND ".join(partition_filters)
            custom_sql = f"""SELECT * 
    FROM `{db_name}.{schema_name}.{bq_table.name}`
    WHERE {partition_where}"""

            logger.debug(f"Using partition filters: {partition_where}")
            profile_request.batch_kwargs.update(
                dict(custom_sql=custom_sql, partition_handling="true")
            )

        return profile_request

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """Get profile workunits handling both internal and external tables."""
        profile_requests: List[TableProfilerRequest] = []

        for dataset in tables:
            for table in tables[dataset]:
                normalized_table_name = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                ).get_table_name()

                if table.external and not self.config.profiling.profile_external_tables:
                    self.report.profiling_skipped_other[f"{project_id}.{dataset}"] += 1
                    logger.info(
                        f"Skipping profiling of external table {project_id}.{dataset}.{table.name}"
                    )
                    continue

                # Emit profile work unit
                logger.debug(
                    f"Creating profile request for table {normalized_table_name}"
                )
                profile_request = self.get_profile_request(table, dataset, project_id)
                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                else:
                    logger.debug(
                        f"Table {normalized_table_name} was not eligible for profiling."
                    )

        if len(profile_requests) == 0:
            return

        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """Get dataset name in BigQuery format."""
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()
