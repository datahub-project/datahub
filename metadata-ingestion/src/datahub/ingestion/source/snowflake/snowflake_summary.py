import dataclasses
import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

import pydantic
from snowflake.connector import SnowflakeConnection

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import LowerCaseDatasetUrnConfigMixin
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import SupportStatus, config_class, support_status
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeDataDictionary,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeConnectionMixin,
    SnowflakeQueryMixin,
)
from datahub.ingestion.source_config.sql.snowflake import BaseSnowflakeConfig
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.utilities.lossy_collections import LossyList


class SnowflakeSummaryConfig(
    BaseSnowflakeConfig, BaseTimeWindowConfig, LowerCaseDatasetUrnConfigMixin
):

    # Copied from SnowflakeConfig.
    database_pattern: AllowDenyPattern = AllowDenyPattern(
        deny=[r"^UTIL_DB$", r"^SNOWFLAKE$", r"^SNOWFLAKE_SAMPLE_DATA$"]
    )
    schema_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )
    table_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    view_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    match_fully_qualified_names: bool = pydantic.Field(
        default=True,
        description="Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`.",
    )


@dataclasses.dataclass
class SnowflakeSummaryReport(SourceReport, BaseTimeWindowReport):
    filtered: LossyList[str] = dataclasses.field(default_factory=LossyList)

    num_get_tables_for_schema_queries: int = 0
    num_get_views_for_schema_queries: int = 0

    schema_counters: Dict[str, int] = dataclasses.field(default_factory=dict)
    object_counters: Dict[str, Dict[str, int]] = dataclasses.field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )

    num_snowflake_queries: Optional[int] = None
    num_snowflake_mutations: Optional[int] = None

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        pass


@config_class(SnowflakeSummaryConfig)
@support_status(SupportStatus.INCUBATING)
class SnowflakeSummarySource(
    SnowflakeQueryMixin,
    SnowflakeConnectionMixin,
    SnowflakeCommonMixin,
    Source,
):
    def __init__(self, ctx: PipelineContext, config: SnowflakeSummaryConfig):
        super().__init__(ctx)
        self.config: SnowflakeSummaryConfig = config
        self.report: SnowflakeSummaryReport = SnowflakeSummaryReport()

        self.data_dictionary = SnowflakeDataDictionary()
        self.connection: Optional[SnowflakeConnection] = None
        self.logger = logging.getLogger(__name__)

    def create_connection(self) -> Optional[SnowflakeConnection]:
        # TODO: Eventually we'll want to use the implementation from SnowflakeConnectionMixin,
        # since it has better error reporting.
        # return super().create_connection()
        return self.config.get_connection()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.connection = self.create_connection()
        if self.connection is None:
            return

        self.data_dictionary.set_connection(self.connection)

        # Databases.
        databases: List[SnowflakeDatabase] = []
        for database in self.get_databases() or []:  # type: ignore
            # TODO: Support database_patterns.
            if not self.config.database_pattern.allowed(database.name):
                self.report.report_dropped(f"{database.name}.*")
            else:
                databases.append(database)

        # Schemas.
        for database in databases:
            self.fetch_schemas_for_database(database, database.name)  # type: ignore

            self.report.schema_counters[database.name] = len(database.schemas)

            for schema in database.schemas:
                # Tables/views.
                tables = self.fetch_tables_for_schema(  # type: ignore
                    schema, database.name, schema.name
                )
                views = self.fetch_views_for_schema(  # type: ignore
                    schema, database.name, schema.name
                )

                self.report.object_counters[database.name][schema.name] = len(
                    tables
                ) + len(views)

        # Queries for usage.
        start_time_millis = self.config.start_time.timestamp() * 1000
        end_time_millis = self.config.end_time.timestamp() * 1000
        for row in self.query(
            f"""\
SELECT COUNT(*) AS CNT
FROM snowflake.account_usage.query_history
WHERE query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
  AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3)
"""
        ):
            self.report.num_snowflake_queries = row["CNT"]

        # Queries for lineage/operations.
        for row in self.query(
            f"""\
SELECT COUNT(*) AS CNT
FROM
    snowflake.account_usage.access_history access_history
WHERE query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
  AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
  AND access_history.objects_modified is not null
  AND ARRAY_SIZE(access_history.objects_modified) > 0
"""
        ):
            self.report.num_snowflake_mutations = row["CNT"]

        # This source doesn't produce any metadata itself. All important information goes into the report.
        yield from []

    # This is a bit of a hack, but lets us reuse the code from the main ingestion source.
    # Mypy doesn't really know how to deal with it though, which is why we have all these
    # type ignore comments.
    get_databases = SnowflakeSchemaGenerator.get_databases
    get_databases_from_ischema = SnowflakeSchemaGenerator.get_databases_from_ischema
    fetch_schemas_for_database = SnowflakeSchemaGenerator.fetch_schemas_for_database
    fetch_tables_for_schema = SnowflakeSchemaGenerator.fetch_tables_for_schema
    fetch_views_for_schema = SnowflakeSchemaGenerator.fetch_views_for_schema
    get_tables_for_schema = SnowflakeSchemaGenerator.get_tables_for_schema
    get_views_for_schema = SnowflakeSchemaGenerator.get_views_for_schema

    def get_report(self) -> SnowflakeSummaryReport:
        return self.report
