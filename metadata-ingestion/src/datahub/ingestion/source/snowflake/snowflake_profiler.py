import dataclasses
import logging
from datetime import datetime
from typing import Callable, Iterable, List, Optional, cast

from snowflake.sqlalchemy import snowdialect
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import sqltypes

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
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
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

snowdialect.ischema_names["GEOGRAPHY"] = sqltypes.NullType

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SnowflakeProfilerRequest(GEProfilerRequest):
    table: SnowflakeTable
    profile_table_level_only: bool = False


class SnowflakeProfiler(GenericProfiler, SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        super().__init__(config, report, self.platform, state_handler)
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeV2Report = report
        self.logger = logger

    def get_workunits(
        self, databases: List[SnowflakeDatabase]
    ) -> Iterable[MetadataWorkUnit]:
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
                if not is_schema_allowed(
                    self.config.schema_pattern,
                    schema.name,
                    db.name,
                    self.config.match_fully_qualified_names,
                ):
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

            table_profile_requests = cast(List[TableProfilerRequest], profile_requests)

            for request, profile in self.generate_profiles(
                table_profile_requests,
                self.config.profiling.max_workers,
                db.name,
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

                # We don't add to the profiler state if we only do table level profiling as it always happens
                if self.state_handler:
                    self.state_handler.add_to_state(
                        dataset_urn, int(datetime.now().timestamp() * 1000)
                    )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=profile
                ).as_workunit()

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

    def get_profiler_instance(
        self, db_name: Optional[str] = None
    ) -> "DatahubGEProfiler":
        assert db_name

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

    def callable_for_db_connection(self, db_name: str) -> Callable:
        def get_db_connection():
            conn = self.config.get_connection()
            conn.cursor().execute(SnowflakeQuery.use_database(db_name))
            return conn

        return get_db_connection
