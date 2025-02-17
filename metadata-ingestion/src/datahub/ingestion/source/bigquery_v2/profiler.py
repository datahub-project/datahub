import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
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
                if table.partition_info.column:
                    partition_where_clause = (
                        f"{table.partition_info.column.name} >= {partition}"
                    )
                else:
                    logger.warning(
                        f"Partitioned table {table.name} without partition column"
                    )
                    self.report.profiling_skipped_invalid_partition_ids[
                        f"{project}.{schema}.{table.name}"
                    ] = partition
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
                    self.report.profiling_skipped_invalid_partition_ids[
                        f"{project}.{schema}.{table.name}"
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
                    partition_where_clause = f"`{partition_column_name}` BETWEEN {partition_data_type}('{partition_datetime}') AND {partition_data_type}('{upper_bound_partition_datetime}')"
                else:
                    logger.warning(
                        f"Not supported partition type {table.partition_info.type}"
                    )
                    self.report.profiling_skipped_invalid_partition_type[
                        f"{project}.{schema}.{table.name}"
                    ] = table.partition_info.type
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
        elif table.max_shard_id:
            # For sharded table we want to get the partition id but not needed to generate custom query
            return table.max_shard_id, None

        return None, None

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
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

                # Emit the profile work unit
                logger.debug(
                    f"Creating profile request for table {normalized_table_name}"
                )
                profile_request = self.get_profile_request(table, dataset, project_id)
                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                else:
                    logger.debug(
                        f"Table {normalized_table_name} was not eliagible for profiling."
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
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        return dict(
            schema=db_name,  # <project>
            table=f"{schema_name}.{table.name}",  # <dataset>.<table>
        )

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        # Below code handles profiling changes required for partitioned or sharded tables
        # 1. Skip profile if partition profiling is disabled.
        # 2. Else update `profile_request.batch_kwargs` with partition and custom_sql

        bq_table = cast(BigqueryTable, table)
        (partition, custom_sql) = self.generate_partition_profiler_query(
            db_name, schema_name, bq_table, self.config.profiling.partition_datetime
        )

        if partition is None and bq_table.partition_info:
            self.report.report_warning(
                title="Profile skipped for partitioned table",
                message="profile skipped as partitioned table is empty or partition id or type was invalid",
                context=profile_request.pretty_name,
            )
            return None
        if (
            partition is not None
            and not self.config.profiling.partition_profiling_enabled
        ):
            logger.debug(
                f"{profile_request.pretty_name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        if partition:
            logger.debug("Updating profiling request for partitioned/sharded tables")
            profile_request.batch_kwargs.update(
                dict(
                    custom_sql=custom_sql,
                    partition=partition,
                )
            )

        return profile_request
