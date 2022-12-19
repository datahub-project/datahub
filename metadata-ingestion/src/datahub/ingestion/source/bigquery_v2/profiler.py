import dataclasses
import datetime
import logging
from typing import Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp_builder import wrap_aspect_as_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryTable,
)
from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BigqueryProfilerRequest(GEProfilerRequest):
    table: BigqueryTable
    profile_table_level_only: bool = False


class BigqueryProfiler(GenericProfiler):
    config: BigQueryV2Config
    report: BigQueryV2Report

    def __init__(self, config: BigQueryV2Config, report: BigQueryV2Report) -> None:
        super().__init__(config, report, "bigquery")
        self.config = config
        self.report = report

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime.datetime]
    ) -> Tuple[datetime.datetime, datetime.datetime]:
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
                partition_datetime = datetime.datetime.strptime(partition_id, format)
        else:
            raise ValueError(
                f"check your partition_id {partition_id}. It must be yearly/monthly/daily/hourly."
            )
        upper_bound_partition_datetime = partition_datetime + duration
        return partition_datetime, upper_bound_partition_datetime

    def generate_partition_profiler_query(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        partition_datetime: Optional[datetime.datetime],
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Method returns partition id if table is partitioned or sharded and generate custom partition query for
        partitioned table.
        See more about partitioned tables at https://cloud.google.com/bigquery/docs/partitioned-tables
        """
        logger.debug(
            f"generate partition profiler query for project: {project} schema: {schema} and table {table.name}, partition_datetime: {partition_datetime}"
        )
        partition = table.max_partition_id
        if partition:
            partition_where_clause: str

            if not table.time_partitioning:
                partition_column: Optional[BigqueryColumn] = None
                for column in table.columns:
                    if column.is_partition_column:
                        partition_column = column
                        break
                if partition_column:
                    partition_where_clause = f"{partition_column.name} >= {partition}"
                else:
                    logger.warning(
                        f"Partitioned table {table.name} without partiton column"
                    )
                    return None, None
            else:
                logger.debug(
                    f"{table.name} is partitioned and partition column is {partition}"
                )
                try:
                    (
                        partition_datetime,
                        upper_bound_partition_datetime,
                    ) = self.get_partition_range_from_partition_id(
                        partition, partition_datetime
                    )
                except ValueError as e:
                    logger.error(
                        f"Unable to get partition range for partition id: {partition} it failed with exception {e}"
                    )
                    self.report.invalid_partition_ids[
                        f"{schema}.{table.name}"
                    ] = partition
                    return None, None

                partition_column_type: str = "DATE"
                for c in table.columns:
                    if c.is_partition_column:
                        partition_column_type = c.data_type

                if table.time_partitioning.type_ in ("DAY", "MONTH", "YEAR"):
                    partition_where_clause = f"`{table.time_partitioning.field}` BETWEEN {partition_column_type}('{partition_datetime}') AND {partition_column_type}('{upper_bound_partition_datetime}')"
                elif table.time_partitioning.type_ in ("HOUR"):
                    partition_where_clause = f"`{table.time_partitioning.field}` BETWEEN '{partition_datetime}' AND '{upper_bound_partition_datetime}'"
                else:
                    logger.warning(
                        f"Not supported partition type {table.time_partitioning.type_}"
                    )
                    return None, None
            custom_sql = """
SELECT
    *
FROM
    `{table_catalog}.{table_schema}.{table_name}`
WHERE
    {partition_where_clause}
            """.format(
                table_catalog=project,
                table_schema=schema,
                table_name=table.name,
                partition_where_clause=partition_where_clause,
            )

            return (partition, custom_sql)
        if table.max_shard_id:
            # For sharded table we want to get the partition id but not needed to generate custom query
            return table.max_shard_id, None

        return None, None

    def get_workunits(
        self, tables: Dict[str, Dict[str, List[BigqueryTable]]]
    ) -> Iterable[MetadataWorkUnit]:
        # Otherwise, if column level profiling is enabled, use  GE profiler.
        for project in tables.keys():
            if not self.config.project_id_pattern.allowed(project):
                continue
            profile_requests = []

            for dataset in tables[project]:
                if not self.config.schema_pattern.allowed(dataset):
                    continue

                for table in tables[project][dataset]:
                    for column in table.columns:
                        # Profiler has issues with complex types (array, struct, geography, json), so we deny those types from profiling
                        # We also filter columns without data type as it means that column is part of a complex type.
                        if not column.data_type or any(
                            word in column.data_type.lower()
                            for word in ["array", "struct", "geography", "json"]
                        ):
                            normalized_table_name = BigqueryTableIdentifier(
                                project_id=project, dataset=dataset, table=table.name
                            ).get_table_name()

                            self.config.profile_pattern.deny.append(
                                f"^{normalized_table_name}.{column.field_path}$"
                            )

                    # Emit the profile work unit
                    profile_request = self.get_bigquery_profile_request(
                        project=project, dataset=dataset, table=table
                    )
                    if profile_request is not None:
                        profile_requests.append(profile_request)

            if len(profile_requests) == 0:
                continue
            table_profile_requests = cast(List[TableProfilerRequest], profile_requests)
            for request, profile in self.generate_profiles(
                table_profile_requests,
                self.config.profiling.max_workers,
                platform=self.platform,
                profiler_args=self.get_profile_args(),
            ):
                if request is None or profile is None:
                    continue

                request = cast(BigqueryProfilerRequest, request)
                profile.sizeInBytes = request.table.size_in_bytes
                # If table is partitioned we profile only one partition (if nothing set then the last one)
                # but for table level we can use the rows_count from the table metadata
                # This way even though column statistics only reflects one partition data but the rows count
                # shows the proper count.
                if profile.partitionSpec and profile.partitionSpec.partition:
                    profile.rowCount = request.table.rows_count

                dataset_name = request.pretty_name
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )
                wu = wrap_aspect_as_workunit(
                    "dataset",
                    dataset_urn,
                    "datasetProfile",
                    profile,
                )
                self.report.report_workunit(wu)
                yield wu

    def get_bigquery_profile_request(
        self, project: str, dataset: str, table: BigqueryTable
    ) -> Optional[BigqueryProfilerRequest]:
        skip_profiling = False
        profile_table_level_only = self.config.profiling.profile_table_level_only
        dataset_name = BigqueryTableIdentifier(
            project_id=project, dataset=dataset, table=table.name
        ).get_table_name()
        if not self.is_dataset_eligible_for_profiling(
            dataset_name, table.last_altered, table.size_in_bytes, table.rows_count
        ):
            profile_table_level_only = True
            self.report.num_tables_not_eligible_profiling[f"{project}.{dataset}"] = (
                self.report.num_tables_not_eligible_profiling.get(
                    f"{project}.{dataset}", 0
                )
                + 1
            )

        if not table.columns:
            skip_profiling = True

        if skip_profiling:
            if self.config.profiling.report_dropped_profiles:
                self.report.report_dropped(f"profile of {dataset_name}")
            return None
        (partition, custom_sql) = self.generate_partition_profiler_query(
            project, dataset, table, self.config.profiling.partition_datetime
        )

        if partition is None and table.time_partitioning:
            self.report.report_warning(
                "profile skipped as partitioned table is empty or partition id was invalid",
                dataset_name,
            )
            return None

        if (
            partition is not None
            and not self.config.profiling.partition_profiling_enabled
        ):
            logger.debug(
                f"{dataset_name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            return None

        self.report.report_entity_profiled(dataset_name)
        logger.debug(f"Preparing profiling request for {dataset_name}")
        profile_request = BigqueryProfilerRequest(
            pretty_name=dataset_name,
            batch_kwargs=dict(
                schema=project,
                table=f"{dataset}.{table.name}",
                custom_sql=custom_sql,
                partition=partition,
            ),
            table=table,
            profile_table_level_only=profile_table_level_only,
        )
        return profile_request
