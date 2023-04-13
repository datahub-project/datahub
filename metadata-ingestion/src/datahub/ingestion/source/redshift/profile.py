import dataclasses
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Union, cast

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp_builder import wrap_aspect_as_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RedshiftProfilerRequest(GEProfilerRequest):
    table: Union[RedshiftTable, RedshiftView]
    profile_table_level_only: bool = False


class RedshiftProfiler(GenericProfiler):
    config: RedshiftConfig
    report: RedshiftReport

    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        state_handler: Optional[ProfilingHandler],
    ) -> None:
        super().__init__(config, report, "redshift", state_handler)
        self.config = config
        self.report = report

    def get_workunits(
        self,
        tables: Union[
            Dict[str, Dict[str, List[RedshiftTable]]],
            Dict[str, Dict[str, List[RedshiftView]]],
        ],
    ) -> Iterable[MetadataWorkUnit]:

        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if self.config.profiling.enabled:
            self.config.options.setdefault(
                "max_overflow", self.config.profiling.max_workers
            )

        for db in tables.keys():
            profile_requests = []
            for schema in tables.get(db, {}).keys():
                if not self.config.schema_pattern.allowed(schema):
                    continue
                for table in tables[db].get(schema, {}):

                    # Emit the profile work unit
                    profile_request = self.get_redshift_profile_request(
                        table, schema, db
                    )
                    if profile_request is not None:
                        profile_requests.append(profile_request)

            if len(profile_requests) == 0:
                continue
            table_profile_requests = cast(List[TableProfilerRequest], profile_requests)
            for request, profile in self.generate_profiles(
                table_profile_requests,
                self.config.profiling.max_workers,
                db,
                platform=self.platform,
                profiler_args=self.get_profile_args(),
            ):
                if profile is None:
                    continue
                request = cast(RedshiftProfilerRequest, request)

                profile.sizeInBytes = request.table.size_in_bytes
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

                yield wrap_aspect_as_workunit(
                    "dataset",
                    dataset_urn,
                    "datasetProfile",
                    profile,
                )

    def get_redshift_profile_request(
        self,
        table: Union[RedshiftTable, RedshiftView],
        schema_name: str,
        db_name: str,
    ) -> Optional[RedshiftProfilerRequest]:
        skip_profiling = False
        profile_table_level_only = self.config.profiling.profile_table_level_only
        dataset_name = f"{db_name}.{schema_name}.{table.name}".lower()
        if not self.is_dataset_eligible_for_profiling(
            dataset_name, table.last_altered, table.size_in_bytes, table.rows_count
        ):
            # Profile only table level if dataset is filtered from profiling
            # due to size limits alone
            if self.is_dataset_eligible_for_profiling(
                dataset_name, table.last_altered, 0, 0
            ):
                profile_table_level_only = True
            else:
                skip_profiling = True

        if len(table.columns) == 0:
            skip_profiling = True

        if skip_profiling:
            if self.config.profiling.report_dropped_profiles:
                self.report.report_dropped(f"profile of {dataset_name}")
            return None

        self.report.report_entity_profiled(dataset_name)
        logger.debug(f"Preparing profiling request for {dataset_name}")
        profile_request = RedshiftProfilerRequest(
            pretty_name=dataset_name,
            batch_kwargs=dict(schema=schema_name, table=table.name),
            table=table,
            profile_table_level_only=profile_table_level_only,
        )
        return profile_request
