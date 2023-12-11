import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Iterable, List, Optional, Set

from pydantic import Field

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    Source,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.utilities.sqlglot_lineage import SchemaResolver, sqlglot_lineage

logger = logging.getLogger(__name__)


class SqlQueriesSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    query_file: str = Field(description="Path to file to ingest")

    platform: str = Field(
        description="The platform for which to generate data, e.g. snowflake"
    )

    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    use_schema_resolver: bool = Field(
        description="Read SchemaMetadata aspects from DataHub to aid in SQL parsing. Turn off only for testing.",
        default=True,
        hidden_from_docs=True,
    )
    default_db: Optional[str] = Field(
        description="The default database to use for unqualified table names",
        default=None,
    )
    default_schema: Optional[str] = Field(
        description="The default schema to use for unqualified table names",
        default=None,
    )


class SqlQueriesSourceReport(SourceReport):
    num_queries_parsed: int = 0
    num_table_parse_failures: int = 0
    num_column_parse_failures: int = 0

    def compute_stats(self) -> None:
        super().compute_stats()
        self.table_failure_rate = (
            f"{self.num_table_parse_failures / self.num_queries_parsed:.4f}"
            if self.num_queries_parsed
            else "0"
        )
        self.column_failure_rate = (
            f"{self.num_column_parse_failures / self.num_queries_parsed:.4f}"
            if self.num_queries_parsed
            else "0"
        )


@platform_name("SQL Queries")
@config_class(SqlQueriesSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.LINEAGE_COARSE, "Parsed from SQL queries")
@capability(SourceCapability.LINEAGE_FINE, "Parsed from SQL queries")
class SqlQueriesSource(Source):
    """
    This source reads a newline-delimited JSON file containing SQL queries and parses them to generate lineage.

    ### Query File Format
    This file should contain one JSON object per line, with the following fields:
    - query: string - The SQL query to parse.
    - timestamp (optional): number - The timestamp of the query, in seconds since the epoch.
    - user (optional): string - The user who ran the query.
    This user value will be directly converted into a DataHub user urn.
    - operation_type (optional): string - Platform-specific operation type, used if the operation type can't be parsed.
    - downstream_tables (optional): string[] - Fallback list of tables that the query writes to,
     used if the query can't be parsed.
    - upstream_tables (optional): string[] - Fallback list of tables the query reads from,
     used if the query can't be parsed.
    """

    urns: Optional[Set[str]]
    schema_resolver: SchemaResolver
    builder: SqlParsingBuilder

    def __init__(self, ctx: PipelineContext, config: SqlQueriesSourceConfig):
        if not ctx.graph:
            raise ValueError(
                "SqlQueriesSource needs a datahub_api from which to pull schema metadata"
            )

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = SqlQueriesSourceReport()

        self.builder = SqlParsingBuilder(usage_config=self.config.usage)

        if self.config.use_schema_resolver:
            self.schema_resolver = self.graph.initialize_schema_resolver_from_datahub(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            self.urns = self.schema_resolver.get_urns()
        else:
            self.schema_resolver = self.graph._make_schema_resolver(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            self.urns = None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqlQueriesSource":
        config = SqlQueriesSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SqlQueriesSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [partial(auto_workunit_reporter, self.get_report())]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Parsing queries from {os.path.basename(self.config.query_file)}")
        with open(self.config.query_file) as f:
            for line in f:
                try:
                    query_dict = json.loads(line, strict=False)
                    entry = QueryEntry.create(query_dict, config=self.config)
                    yield from self._process_query(entry)
                except Exception as e:
                    logger.warning("Error processing query", exc_info=True)
                    self.report.report_warning("process-query", str(e))

        logger.info("Generating workunits")
        yield from self.builder.gen_workunits()

    def _process_query(self, entry: "QueryEntry") -> Iterable[MetadataWorkUnit]:
        self.report.num_queries_parsed += 1
        if self.report.num_queries_parsed % 1000 == 0:
            logger.info(f"Parsed {self.report.num_queries_parsed} queries")

        result = sqlglot_lineage(
            sql=entry.query,
            schema_resolver=self.schema_resolver,
            default_db=self.config.default_db,
            default_schema=self.config.default_schema,
        )
        if result.debug_info.table_error:
            logger.info(f"Error parsing table lineage, {result.debug_info.table_error}")
            self.report.num_table_parse_failures += 1
            for downstream_urn in set(entry.downstream_tables):
                self.builder.add_lineage(
                    downstream_urn=downstream_urn,
                    upstream_urns=entry.upstream_tables,
                    timestamp=entry.timestamp,
                    user=entry.user,
                )
            return
        elif result.debug_info.column_error:
            logger.debug(
                f"Error parsing column lineage, {result.debug_info.column_error}"
            )
            self.report.num_column_parse_failures += 1

        yield from self.builder.process_sql_parsing_result(
            result,
            query=entry.query,
            query_timestamp=entry.timestamp,
            user=entry.user,
            custom_operation_type=entry.operation_type,
            include_urns=self.urns,
        )


@dataclass
class QueryEntry:
    query: str
    timestamp: Optional[datetime]
    user: Optional[str]
    operation_type: Optional[str]
    downstream_tables: List[str]
    upstream_tables: List[str]

    @classmethod
    def create(
        cls, entry_dict: dict, *, config: SqlQueriesSourceConfig
    ) -> "QueryEntry":
        return cls(
            query=entry_dict["query"],
            timestamp=datetime.fromtimestamp(entry_dict["timestamp"], tz=timezone.utc)
            if "timestamp" in entry_dict
            else None,
            user=make_user_urn(entry_dict["user"]) if "user" in entry_dict else None,
            operation_type=entry_dict.get("operation_type"),
            downstream_tables=[
                make_dataset_urn_with_platform_instance(
                    name=table,
                    platform=config.platform,
                    platform_instance=config.platform_instance,
                    env=config.env,
                )
                for table in entry_dict.get("downstream_tables", [])
            ],
            upstream_tables=[
                make_dataset_urn_with_platform_instance(
                    name=table,
                    platform=config.platform,
                    platform_instance=config.platform_instance,
                    env=config.env,
                )
                for table in entry_dict.get("upstream_tables", [])
            ],
        )
