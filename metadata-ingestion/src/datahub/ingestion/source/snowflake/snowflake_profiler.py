import logging
from typing import Callable, Dict, Iterable, List, Optional

from snowflake.sqlalchemy import snowdialect
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import sqltypes

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeCommonMixin
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import GenericProfiler
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

snowdialect.ischema_names["GEOGRAPHY"] = sqltypes.NullType
snowdialect.ischema_names["GEOMETRY"] = sqltypes.NullType

logger = logging.getLogger(__name__)

PUBLIC_SCHEMA = "PUBLIC"


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
        self.database_default_schema: Dict[str, str] = dict()

    def get_workunits(
        self, database: SnowflakeDatabase, db_tables: Dict[str, List[SnowflakeTable]]
    ) -> Iterable[MetadataWorkUnit]:
        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if self.config.is_profiling_enabled():
            self.config.options.setdefault(
                "max_overflow", self.config.profiling.max_workers
            )

        if PUBLIC_SCHEMA not in db_tables:
            # If PUBLIC schema is absent, we use any one of schemas as default schema
            self.database_default_schema[database.name] = list(db_tables.keys())[0]

        profile_requests = []
        for schema in database.schemas:
            for table in db_tables[schema.name]:
                if (
                    not self.config.profiling.profile_external_tables
                    and table.type == "EXTERNAL TABLE"
                ):
                    logger.info(
                        f"Skipping profiling of external table {database.name}.{schema.name}.{table.name}"
                    )
                    self.report.profiling_skipped_other[schema.name] += 1
                    continue

                profile_request = self.get_profile_request(
                    table, schema.name, database.name
                )
                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)

        if len(profile_requests) == 0:
            return

        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            db_name=database.name,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return self.get_dataset_identifier(table_name, schema_name, db_name)

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        custom_sql = None
        if (
            not self.config.profiling.limit
            and self.config.profiling.use_sampling
            and table.rows_count
            and table.rows_count > self.config.profiling.sample_size
        ):
            # GX creates a temporary table from query if query is passed as batch kwargs.
            # We are using fraction-based sampling here, instead of fixed-size sampling because
            # Fixed-size sampling can be slower than equivalent fraction-based sampling
            # as per https://docs.snowflake.com/en/sql-reference/constructs/sample#performance-considerations
            estimated_block_row_count = 500_000
            block_profiling_min_rows = 100 * estimated_block_row_count

            tablename = f'"{db_name}"."{schema_name}"."{table.name}"'
            sample_pc = self.config.profiling.sample_size / table.rows_count

            overgeneration_factor = 1000
            if (
                table.rows_count > block_profiling_min_rows
                and table.rows_count
                > self.config.profiling.sample_size * overgeneration_factor
            ):
                # If the table is significantly larger than the sample size, do a first pass
                # using block sampling to improve performance. We generate a table 1000 times
                # larger than the target sample size, and then use normal sampling for the
                # final size reduction.
                tablename = f"(SELECT * FROM {tablename} TABLESAMPLE BLOCK ({100 * overgeneration_factor * sample_pc:.8f}))"
                sample_pc = 1 / overgeneration_factor

            custom_sql = f"select * from {tablename} TABLESAMPLE BERNOULLI ({100 * sample_pc:.8f})"
        return {
            **super().get_batch_kwargs(table, schema_name, db_name),
            # Lowercase/Mixedcase table names in Snowflake do not work by default.
            # We need to pass `use_quoted_name=True` for such tables as mentioned here -
            # https://github.com/great-expectations/great_expectations/pull/2023
            "use_quoted_name": (table.name != table.name.upper()),
            "custom_sql": custom_sql,
        }

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
        schema_name = self.database_default_schema.get(db_name)

        def get_db_connection():
            conn = self.config.get_connection()
            conn.cursor().execute(SnowflakeQuery.use_database(db_name))

            # As mentioned here - https://docs.snowflake.com/en/sql-reference/sql/use-database#usage-notes
            # no schema is selected if PUBLIC schema is absent. We need to explicitly call `USE SCHEMA <schema>`
            if schema_name:
                conn.cursor().execute(SnowflakeQuery.use_schema(schema_name))
            return conn

        return get_db_connection
