import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional

from snowflake.sqlalchemy import snowdialect

if TYPE_CHECKING:
    from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
        SQLAlchemyProfiler,
    )
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import sqltypes

from datahub.ingestion.api.workunit import MetadataWorkUnit
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
from datahub.metadata.schema_classes import DatasetProfileClass

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
        column_name_maps: Dict[str, Dict[str, str]] = {}
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
                    column_name_maps[
                        self.dataset_urn_builder(profile_request.pretty_name)
                    ] = self._build_column_name_map(table)

        if len(profile_requests) == 0:
            return

        yield from self._restore_column_case(
            self.generate_profile_workunits(
                profile_requests,
                max_workers=self.config.profiling.max_workers,
                db_name=database.name,
                platform=self.platform,
                profiler_args=self.get_profile_args(),
            ),
            column_name_maps,
        )

    def _build_column_name_map(self, table: SnowflakeTable) -> Dict[str, str]:
        # snowflake-sqlalchemy sets requires_name_normalize, so reflected column names
        # arrive folded (`CUSTOMER_ID` -> `customer_id`) while field paths are built
        # from INFORMATION_SCHEMA. Map both spellings a profile can carry — the folded
        # name, and the as-stored name the profiler uses for case-colliding columns —
        # back to the column's true name.
        folded: Dict[str, List[str]] = defaultdict(list)
        for col in table.columns:
            folded[col.name.lower()].append(col.name)

        resolved: Dict[str, str] = {col.name: col.name for col in table.columns}
        for folded_name, original_names in folded.items():
            # An ambiguous folded name is already covered by the exact entries above.
            if len(original_names) == 1:
                resolved.setdefault(folded_name, original_names[0])
        return resolved

    def _restore_column_case(
        self,
        workunits: Iterable[MetadataWorkUnit],
        column_name_maps: Dict[str, Dict[str, str]],
    ) -> Iterable[MetadataWorkUnit]:
        for wu in workunits:
            profile = wu.get_aspect_of_type(DatasetProfileClass)
            name_map = column_name_maps.get(wu.get_urn()) if profile else None
            if profile and name_map is not None and profile.fieldProfiles:
                unresolved = []
                kept = []
                seen_field_paths = set()
                for field_profile in profile.fieldProfiles:
                    original_name = name_map.get(field_profile.fieldPath)
                    if original_name is None:
                        original_name = name_map.get(field_profile.fieldPath.lower())
                    if original_name is None:
                        unresolved.append(field_profile.fieldPath)
                        kept.append(field_profile)
                        continue
                    field_profile.fieldPath = (
                        self.identifiers.snowflake_identifier(original_name)
                    )
                    # Columns differing only by case fold to one field path unless
                    # preserve_column_case is set. Keeping the first mirrors the
                    # schema, where the duplicate field is dropped; emitting both
                    # would put two profiles on one field.
                    if field_profile.fieldPath in seen_field_paths:
                        continue
                    seen_field_paths.add(field_profile.fieldPath)
                    kept.append(field_profile)
                profile.fieldProfiles = kept
                if unresolved:
                    self.report.warning(
                        title="Profile field path does not match any column",
                        message="These profile statistics keep the profiler's own "
                        "field path and may not attach to the schema.",
                        context=f"{wu.get_urn()}: {sorted(unresolved)}",
                    )
            yield wu

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return self.identifiers.get_dataset_identifier(table_name, schema_name, db_name)

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        return {
            **super().get_batch_kwargs(table, schema_name, db_name),
            # Lowercase/Mixedcase table names in Snowflake do not work by default.
            # We need to pass `use_quoted_name=True` for such tables as mentioned here -
            # https://github.com/great-expectations/great_expectations/pull/2023
            "use_quoted_name": (table.name != table.name.upper()),
            "custom_sql": None,
            "row_count": table.rows_count,
        }

    def get_profiler_instance(
        self, db_name: Optional[str] = None
    ) -> "SQLAlchemyProfiler":
        from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
            SQLAlchemyProfiler,
        )

        assert db_name

        url = self.config.get_sql_alchemy_url(database=db_name)

        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(
            url,
            creator=self.callable_for_db_connection(db_name),
            **self.config.get_options(),
        )
        conn = engine.connect()
        inspector = inspect(conn)

        logger.info(
            f"Using SQLAlchemyProfiler for profiling (platform: {self.platform})"
        )
        return SQLAlchemyProfiler(
            conn=inspector.bind,
            report=self.report,
            config=self.config.profiling,
            platform=self.platform,
            env=self.config.env,
        )

    def callable_for_db_connection(self, db_name: str) -> Callable:
        schema_name = self.database_default_schema.get(db_name)

        def get_db_connection():
            conn = self.config.get_native_connection()
            conn.cursor().execute(SnowflakeQuery.use_database(db_name))

            # As mentioned here - https://docs.snowflake.com/en/sql-reference/sql/use-database#usage-notes
            # no schema is selected if PUBLIC schema is absent. We need to explicitly call `USE SCHEMA <schema>`
            if schema_name:
                conn.cursor().execute(SnowflakeQuery.use_schema(schema_name))
            return conn

        return get_db_connection
