import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional, Union

# This import verifies that the dependencies are available.
import teradatasqlalchemy  # noqa: F401
import teradatasqlalchemy.types as custom_types
from pydantic.fields import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, register_custom_type
from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    TimeTypeClass,
)
from datahub.utilities.sqlglot_lineage import SchemaResolver, sqlglot_lineage

logger: logging.Logger = logging.getLogger(__name__)

register_custom_type(custom_types.JSON, BytesTypeClass)
register_custom_type(custom_types.INTERVAL_DAY, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MONTH, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR_TO_MONTH, TimeTypeClass)
register_custom_type(custom_types.MBB, BytesTypeClass)
register_custom_type(custom_types.MBR, BytesTypeClass)
register_custom_type(custom_types.GEOMETRY, BytesTypeClass)
register_custom_type(custom_types.TDUDT, BytesTypeClass)
register_custom_type(custom_types.XML, BytesTypeClass)


@dataclass
class TeradataReport(ProfilingSqlReport, IngestionStageReport, BaseTimeWindowReport):
    num_queries_parsed: int = 0
    num_view_ddl_parsed: int = 0
    num_table_parse_failures: int = 0


class BaseTeradataConfig(TwoTierSQLAlchemyConfig):
    scheme = Field(default="teradatasql", description="database scheme")


class TeradataConfig(BaseTeradataConfig, BaseTimeWindowConfig):
    database_pattern = Field(
        default=AllowDenyPattern(deny=["dbc"]),
        description="Regex patterns for databases to filter in ingestion.",
    )
    include_table_lineage = Field(
        default=False,
        description="Whether to include table lineage in the ingestion. "
        "This requires to have the table lineage feature enabled.",
    )

    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    default_db: Optional[str] = Field(
        default=None,
        description="The default database to use for unqualified table names",
    )

    include_usage_statistics: bool = Field(
        default=False,
        description="Generate usage statistic.",
    )


@platform_name("Teradata")
@config_class(TeradataConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Optionally enabled via configuration")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_FINE, "Optionally enabled via configuration")
@capability(SourceCapability.USAGE_STATS, "Optionally enabled via configuration")
class TeradataSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    """

    config: TeradataConfig

    LINEAGE_QUERY: str = """SELECT ProcID, UserName as "user", StartTime AT TIME ZONE 'GMT' as "timestamp", DefaultDatabase as default_database, QueryText as query
     FROM "DBC".DBQLogTbl
     where ErrorCode = 0
     and QueryText like 'create table demo_user.test_lineage%'
     and "timestamp" >= TIMESTAMP '{start_time}'
     and "timestamp" < TIMESTAMP '{end_time}'
     """

    def __init__(self, config: TeradataConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "teradata")

        self.report: TeradataReport = TeradataReport()
        self.graph: Optional[DataHubGraph] = ctx.graph

        self.builder: SqlParsingBuilder = SqlParsingBuilder(
            usage_config=self.config.usage
            if self.config.include_usage_statistics
            else None,
            generate_lineage=True,
            generate_usage_statistics=self.config.include_usage_statistics,
            generate_operations=self.config.usage.include_operational_stats,
        )

        self.schema_resolver = SchemaResolver(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            graph=None,
            env=self.config.env,
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = TeradataConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_audit_log_mcps(self) -> Iterable[MetadataWorkUnit]:
        engine = self.get_metadata_engine()
        for entry in engine.execute(
            self.LINEAGE_QUERY.format(
                start_time=self.config.start_time, end_time=self.config.end_time
            )
        ):
            self.report.num_queries_parsed += 1
            if self.report.num_queries_parsed % 1000 == 0:
                logger.info(f"Parsed {self.report.num_queries_parsed} queries")

            yield from self.gen_lineage_from_query(
                query=entry.query,
                default_database=entry.default_database,
                timestamp=entry.timestamp,
                user=entry.user,
                is_view_ddl=False,
            )

    def gen_lineage_from_query(
        self,
        query: str,
        default_database: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        user: Optional[str] = None,
        is_view_ddl: bool = False,
    ) -> Iterable[MetadataWorkUnit]:
        result = sqlglot_lineage(
            sql=query,
            schema_resolver=self.schema_resolver,
            default_db=None,
            default_schema=default_database
            if default_database
            else self.config.default_db,
        )
        if result.debug_info.table_error:
            logger.debug(
                f"Error parsing table lineage, {result.debug_info.table_error}"
            )
            self.report.num_table_parse_failures += 1
        else:
            yield from self.builder.process_sql_parsing_result(
                result,
                query=query,
                is_view_ddl=is_view_ddl,
                query_timestamp=timestamp,
                user=f"urn:li:corpuser:{user}",
                include_urns=self.schema_resolver.get_urns(),
            )

    def get_metadata_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        return create_engine(url, **self.config.options)

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # Add all schemas to the schema resolver
        yield from super().get_workunits_internal()

        if self.config.include_table_lineage or self.config.include_usage_statistics:
            self.report.report_ingestion_stage_start("audit log extraction")
            yield from self.get_audit_log_mcps()

        yield from self.builder.gen_workunits()
