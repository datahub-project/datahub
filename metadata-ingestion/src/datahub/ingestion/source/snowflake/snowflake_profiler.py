import dataclasses
import datetime
import logging
from typing import Callable, Dict, Iterable, List, Optional, Tuple, cast

from snowflake.sqlalchemy import snowdialect
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import sqltypes

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import WorkUnit
from datahub.ingestion.source.ge_data_profiler import (
    DatahubGEProfiler,
    GEProfilerRequest,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeCommonMixin
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProfile
from datahub.metadata.schema_classes import DatasetProfileClass

snowdialect.ischema_names["GEOGRAPHY"] = sqltypes.NullType

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SnowflakeProfilerRequest(GEProfilerRequest):
    table: SnowflakeTable
    profile_table_level_only: bool = False


class SnowflakeProfiler(SnowflakeCommonMixin):
    def __init__(self, config: SnowflakeV2Config, report: SnowflakeV2Report) -> None:
        self.config = config
        self.report = report
        self.logger = logger

    def get_workunits(self, databases: List[SnowflakeDatabase]) -> Iterable[WorkUnit]:

        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if self.config.profiling.enabled:
            self.config.options.setdefault(
                "max_overflow", self.config.profiling.max_workers
            )

        for db in databases:
            if not self.config.database_pattern.allowed(db.name):
                continue
            profile_requests = []
            for schema in db.schemas:
                if not self.config.schema_pattern.allowed(schema.name):
                    continue
                for table in schema.tables:

                    # Emit the profile work unit
                    profile_request = self.get_snowflake_profile_request(
                        table, schema.name, db.name
                    )
                    if profile_request is not None:
                        profile_requests.append(profile_request)

            if len(profile_requests) == 0:
                continue
            for request, profile in self.generate_profiles(
                db.name,
                profile_requests,
                self.config.profiling.max_workers,
                platform=self.platform,
                profiler_args=self.get_profile_args(),
            ):
                if profile is None:
                    continue
                profile.sizeInBytes = cast(
                    SnowflakeProfilerRequest, request
                ).table.size_in_bytes
                dataset_name = request.pretty_name
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )
                yield self.wrap_aspect_as_workunit(
                    "dataset",
                    dataset_urn,
                    "datasetProfile",
                    profile,
                )

    def get_snowflake_profile_request(
        self,
        table: SnowflakeTable,
        schema_name: str,
        db_name: str,
    ) -> Optional[SnowflakeProfilerRequest]:
        skip_profiling = False
        profile_table_level_only = self.config.profiling.profile_table_level_only
        dataset_name = self.get_dataset_identifier(table.name, schema_name, db_name)
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
        profile_request = SnowflakeProfilerRequest(
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
            and (threshold_time is None or last_altered >= threshold_time)
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
        url = self.config.get_sql_alchemy_url(
            database=db_name,
            username=self.config.username,
            password=self.config.password,
            role=self.config.role,
        )

        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(
            url,
            creator=self.callable_for_db_connection(db_name),
            **self.config.get_options(),
        )
        conn = engine.connect()
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
            conn.cursor().execute(SnowflakeQuery.use_database(db_name))
            return conn

        return get_db_connection

    def generate_profiles(
        self,
        db_name: str,
        requests: List[SnowflakeProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Iterable[Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]]:

        ge_profile_requests: List[GEProfilerRequest] = [
            cast(GEProfilerRequest, request)
            for request in requests
            if not request.profile_table_level_only
        ]
        table_level_profile_requests: List[SnowflakeProfilerRequest] = [
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
