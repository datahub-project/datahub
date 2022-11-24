import dataclasses
import datetime
import logging
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union, cast

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp_builder import wrap_aspect_as_workunit
from datahub.ingestion.api.common import WorkUnit
from datahub.ingestion.source.ge_data_profiler import (
    DatahubGEProfiler,
    GEProfilerRequest,
)
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProfile
from datahub.metadata.schema_classes import DatasetProfileClass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RedshiftProfilerRequest(GEProfilerRequest):
    table: RedshiftTable
    profile_table_level_only: bool = False


class RedshiftProfiler:
    def __init__(self, config: RedshiftConfig, report: RedshiftReport) -> None:
        self.config = config
        self.report = report
        self.logger = logger
        self.platform = "redshift"

    def get_workunits(
        self,
        tables: Dict[
            str, Dict[str, List[Union[RedshiftTable, RedshiftView]]]
        ] = {},
    ) -> Iterable[WorkUnit]:

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
            for request, profile in self.generate_profiles(
                db,
                profile_requests,
                self.config.profiling.max_workers,
                platform=self.platform,
                profiler_args=self.get_profile_args(),
            ):
                if profile is None:
                    continue
                profile.sizeInBytes = cast(
                    RedshiftProfilerRequest, request
                ).table.size_in_bytes
                dataset_name = request.pretty_name
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )
                yield wrap_aspect_as_workunit(
                    "dataset",
                    dataset_urn,
                    "datasetProfile",
                    profile,
                )

    def get_redshift_profile_request(
        self,
        table: RedshiftTable,
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

    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        last_altered: datetime.datetime,
        size_in_bytes: int,
        rows_count: Optional[int],
    ) -> bool:
        threshold_time: Optional[datetime.datetime] = None
        if self.config.profiling.profile_if_updated_since_days is not None:
            threshold_time = datetime.datetime.now(
                datetime.timezone.utc
            ) - datetime.timedelta(self.config.profiling.profile_if_updated_since_days)

        return (
            (
                self.config.table_pattern.allowed(dataset_name)
                and self.config.profile_pattern.allowed(dataset_name)
            )
            and (
                threshold_time is None
                or last_altered is None
                or last_altered >= threshold_time
            )
            and (
                self.config.profiling.profile_table_size_limit is None
                or (
                    size_in_bytes is not None
                    and size_in_bytes / (2**30)
                    <= self.config.profiling.profile_table_size_limit
                )
                # Note: Profiling is not allowed is size_in_bytes is not available
                # and self.config.profiling.profile_table_size_limit is set
            )
            and (
                self.config.profiling.profile_table_row_limit is None
                or (
                    rows_count is not None
                    and rows_count <= self.config.profiling.profile_table_row_limit
                )
                # Note: Profiling is not allowed is rows_count is not available
                # and self.config.profiling.profile_table_row_limit is set
            )
        )

    def get_profiler_instance(self, db_name: str) -> "DatahubGEProfiler":
        logger.debug("Getting profiler instance from bigquery")
        url = self.config.get_sql_alchemy_url()

        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)

        return DatahubGEProfiler(
            conn=inspector.bind,
            report=self.report,
            config=self.config.profiling,
            platform=self.platform,
        )

    def get_profile_args(self) -> Dict:
        """Passed down to GE profiler"""
        return {}

    def callable_for_db_connection(self, db_name: str) -> Callable:
        def get_db_connection():
            conn = self.config.get_connection()
            return conn

        return get_db_connection

    def generate_profiles(
        self,
        db_name: str,
        requests: List[RedshiftProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Iterable[Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]]:

        ge_profile_requests: List[GEProfilerRequest] = [
            cast(GEProfilerRequest, request)
            for request in requests
            if not request.profile_table_level_only
        ]
        table_level_profile_requests: List[RedshiftProfilerRequest] = [
            request for request in requests if request.profile_table_level_only
        ]
        for request in table_level_profile_requests:
            profile = DatasetProfile(
                timestampMillis=round(datetime.datetime.now().timestamp() * 1000),
                columnCount=len(request.table.columns),
                rowCount=request.table.rows_count,
                sizeInBytes=request.table.size_in_bytes,
            )
            yield (request, profile)

        if len(ge_profile_requests) == 0:
            return

        # Otherwise, if column level profiling is enabled, use  GE profiler.
        ge_profiler = self.get_profiler_instance(db_name)
        yield from ge_profiler.generate_profiles(
            ge_profile_requests, max_workers, platform, profiler_args
        )
