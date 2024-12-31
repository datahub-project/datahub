import dataclasses
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from datahub.configuration.source_common import LowerCaseDatasetUrnConfigMixin
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import SupportStatus, config_class, support_status
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeFilterConfig,
    SnowflakeIdentifierConfig,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeDatabase
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.utilities.lossy_collections import LossyList


class SnowflakeSummaryConfig(
    SnowflakeFilterConfig,
    SnowflakeConnectionConfig,
    BaseTimeWindowConfig,
    LowerCaseDatasetUrnConfigMixin,
):
    pass


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
class SnowflakeSummarySource(Source):
    def __init__(self, ctx: PipelineContext, config: SnowflakeSummaryConfig):
        super().__init__(ctx)
        self.config: SnowflakeSummaryConfig = config
        self.report: SnowflakeSummaryReport = SnowflakeSummaryReport()

        self.connection = self.config.get_connection()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        schema_generator = SnowflakeSchemaGenerator(
            # This is a hack, but we just hope that the config / report have all the fields we need.
            config=self.config,  # type: ignore
            report=self.report,  # type: ignore
            connection=self.connection,
            identifiers=SnowflakeIdentifierBuilder(
                identifier_config=SnowflakeIdentifierConfig(),
                structured_reporter=self.report,
            ),
            domain_registry=None,
            profiler=None,
            aggregator=None,
            snowsight_url_builder=None,
        )

        # Databases.
        databases: List[SnowflakeDatabase] = []
        for database in schema_generator.get_databases() or []:
            # TODO: Support database_patterns.
            if not self.config.database_pattern.allowed(database.name):
                self.report.report_dropped(f"{database.name}.*")
            else:
                databases.append(database)

        # Schemas.
        for database in databases:
            schema_generator.fetch_schemas_for_database(database, database.name)

            self.report.schema_counters[database.name] = len(database.schemas)

            for schema in database.schemas:
                # Tables/views.
                tables = schema_generator.fetch_tables_for_schema(
                    schema, database.name, schema.name
                )
                views = schema_generator.fetch_views_for_schema(
                    schema, database.name, schema.name
                )

                self.report.object_counters[database.name][schema.name] = len(
                    tables
                ) + len(views)

        # Queries for usage.
        start_time_millis = self.config.start_time.timestamp() * 1000
        end_time_millis = self.config.end_time.timestamp() * 1000
        for row in self.connection.query(
            f"""\
SELECT COUNT(*) AS CNT
FROM snowflake.account_usage.query_history
WHERE query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
  AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3)
"""
        ):
            self.report.num_snowflake_queries = row["CNT"]

        # Queries for lineage/operations.
        for row in self.connection.query(
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

    def get_report(self) -> SnowflakeSummaryReport:
        return self.report
