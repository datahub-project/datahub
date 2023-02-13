import dataclasses
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
    BigqueryTable,
)
from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BigqueryProfilerRequest(GEProfilerRequest):
    table: BigqueryTable
    profile_table_level_only: bool = False


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

    def generate_partition_profiler_query(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        partition_datetime: Optional[datetime] = None,
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
        if table.partition_info and partition:
            partition_where_clause: str

            if table.partition_info.type == RANGE_PARTITION_NAME:
                if table.partition_info and table.partition_info.column:
                    partition_where_clause = (
                        f"{table.partition_info.column.name} >= {partition}"
                    )
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

                partition_data_type: str = "TIMESTAMP"
                # Ingestion time partitioned tables has a pseudo column called _PARTITIONTIME
                # See more about this at
                # https://cloud.google.com/bigquery/docs/partitioned-tables#ingestion_time
                partition_column_name = "_PARTITIONTIME"
                if table.partition_info.column:
                    partition_column_name = table.partition_info.column.name
                    partition_data_type = table.partition_info.column.data_type
                if table.partition_info.type in ("HOUR", "DAY", "MONTH", "YEAR"):
                    partition_where_clause = f"{partition_data_type}(`{partition_column_name}`) BETWEEN {partition_data_type}('{partition_datetime}') AND {partition_data_type}('{upper_bound_partition_datetime}')"
                else:
                    logger.warning(
                        f"Not supported partition type {table.partition_info.type}"
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
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        # Otherwise, if column level profiling is enabled, use  GE profiler.
        if not self.config.project_id_pattern.allowed(project_id):
            return
        profile_requests = []

        for dataset in tables:
            if not self.config.schema_pattern.allowed(dataset):
                continue

            for table in tables[dataset]:
                normalized_table_name = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                ).get_table_name()
                for column in table.columns_ignore_from_profiling:
                    # Profiler has issues with complex types (array, struct, geography, json), so we deny those types from profiling
                    # We also filter columns without data type as it means that column is part of a complex type.
                    self.config.profile_pattern.deny.append(
                        f"^{normalized_table_name}.{column}$"
                    )

                # Emit the profile work unit
                profile_request = self.get_bigquery_profile_request(
                    project=project_id, dataset=dataset, table=table
                )
                if profile_request is not None:
                    profile_requests.append(profile_request)

        if len(profile_requests) == 0:
            return
        yield from self.generate_wu_from_profile_requests(profile_requests)

    def generate_wu_from_profile_requests(
        self, profile_requests: List[BigqueryProfilerRequest]
    ) -> Iterable[MetadataWorkUnit]:
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
            # We don't add to the profiler state if we only do table level profiling as it always happens
            if self.state_handler and not request.profile_table_level_only:
                self.state_handler.add_to_state(
                    dataset_urn, int(datetime.now().timestamp() * 1000)
                )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=profile
            ).as_workunit()

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

        if not table.column_count:
            skip_profiling = True

        if skip_profiling:
            if self.config.profiling.report_dropped_profiles:
                self.report.report_dropped(f"profile of {dataset_name}")
            return None
        (partition, custom_sql) = self.generate_partition_profiler_query(
            project, dataset, table, self.config.profiling.partition_datetime
        )

        if partition is None and table.partition_info:
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
