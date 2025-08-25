import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import ClassVar, Iterable, List, Optional, Union

from pydantic import BaseModel, Field, validator

from datahub.configuration.datetimes import parse_user_datetime
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
    auto_incremental_lineage,
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
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.metadata.urns import CorpUserUrn, DatasetUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlAggregatorReport,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


class SqlQueriesSourceConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, IncrementalLineageConfigMixin
):
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
    override_dialect: Optional[str] = Field(
        description="The SQL dialect to use when parsing queries. Overrides automatic dialect detection.",
        default=None,
    )


@dataclass
class SqlQueriesSourceReport(SourceReport, IngestionStageReport):
    num_entries_processed: int = 0
    num_entries_failed: int = 0
    num_queries_aggregator_failures: int = 0

    sql_aggregator: Optional[SqlAggregatorReport] = None


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
    - session_id (optional): string - Session identifier for temporary table resolution across queries.
    - downstream_tables (optional): string[] - Fallback list of tables that the query writes to,
     used if the query can't be parsed.
    - upstream_tables (optional): string[] - Fallback list of tables the query reads from,
     used if the query can't be parsed.

    ### Incremental Lineage
    When `incremental_lineage` is enabled, this source will emit lineage as patches rather than full overwrites.
    This allows you to add lineage edges without removing existing ones, which is useful for:
    - Gradually building up lineage from multiple sources
    - Preserving manually curated lineage
    - Avoiding conflicts when multiple ingestion processes target the same datasets

    Note: Incremental lineage only applies to UpstreamLineage aspects. Other aspects like queries and usage
    statistics will still be emitted normally.
    """

    schema_resolver: Optional[SchemaResolver]
    aggregator: SqlParsingAggregator

    def __init__(self, ctx: PipelineContext, config: SqlQueriesSourceConfig):
        if not ctx.graph:
            raise ValueError(
                "SqlQueriesSource needs a datahub_api from which to pull schema metadata"
            )

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = SqlQueriesSourceReport()

        if self.config.use_schema_resolver:
            # TODO: `initialize_schema_resolver_from_datahub` does a  bulk initialization by fetching all schemas
            # for the given platform, platform instance, and env. Instead this should be configurable:
            # bulk initialization vs lazy on-demand schema fetching.
            self.schema_resolver = self.graph.initialize_schema_resolver_from_datahub(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
        else:
            self.schema_resolver = None

        self.aggregator = SqlParsingAggregator(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=self.schema_resolver,
            eager_graph_load=False,
            generate_lineage=True,  # TODO: make this configurable
            generate_queries=True,  # TODO: make this configurable
            generate_query_subject_fields=True,  # TODO: make this configurable
            generate_query_usage_statistics=True,  # This enables publishing SELECT query entities, otherwise only mutation queries are published
            generate_usage_statistics=True,
            generate_operations=True,  # TODO: make this configurable
            usage_config=self.config.usage,
            is_temp_table=None,
            is_allowed_table=None,
            format_queries=False,
        )
        self.report.sql_aggregator = self.aggregator.report

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqlQueriesSource":
        config = SqlQueriesSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SqlQueriesSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            partial(auto_workunit_reporter, self.get_report()),
            partial(
                auto_incremental_lineage,
                self.config.incremental_lineage,
            ),
        ]

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        logger.info(f"Parsing queries from {os.path.basename(self.config.query_file)}")

        with self.report.new_stage("Collecting queries from file"):
            queries = list(self._parse_query_file())
            logger.info(f"Collected {len(queries)} queries for processing")

        with self.report.new_stage("Processing queries through SQL parsing aggregator"):
            for query_entry in queries:
                self._add_query_to_aggregator(query_entry)

        with self.report.new_stage("Generating metadata work units"):
            logger.info("Generating workunits from SQL parsing aggregator")
            yield from self.aggregator.gen_metadata()

    def _parse_query_file(self) -> Iterable["QueryEntry"]:
        """Parse the query file and yield QueryEntry objects."""
        with open(self.config.query_file) as f:
            for line in f:
                try:
                    query_dict = json.loads(line, strict=False)
                    entry = QueryEntry.create(query_dict, config=self.config)
                    self.report.num_entries_processed += 1
                    if self.report.num_entries_processed % 1000 == 0:
                        logger.info(
                            f"Processed {self.report.num_entries_processed} query entries"
                        )
                    yield entry
                except Exception as e:
                    self.report.num_entries_failed += 1
                    self.report.warning(
                        title="Error processing query",
                        message="Query skipped due to parsing error",
                        context=line.strip(),
                        exc=e,
                    )

    def _add_query_to_aggregator(self, query_entry: "QueryEntry") -> None:
        """Add a query to the SQL parsing aggregator."""
        try:
            # If we have both upstream and downstream tables, use explicit lineage
            if query_entry.upstream_tables and query_entry.downstream_tables:
                logger.debug("Using explicit lineage from query file")
                for downstream_table in query_entry.downstream_tables:
                    known_lineage = KnownQueryLineageInfo(
                        query_text=query_entry.query,
                        downstream=str(downstream_table),
                        upstreams=[str(urn) for urn in query_entry.upstream_tables],
                        timestamp=query_entry.timestamp,
                        session_id=query_entry.session_id,
                    )
                    self.aggregator.add_known_query_lineage(known_lineage)
            else:
                # Warn if only partial lineage information is provided
                # XOR: true if exactly one of upstream_tables or downstream_tables is provided
                if bool(query_entry.upstream_tables) ^ bool(
                    query_entry.downstream_tables
                ):
                    query_preview = (
                        query_entry.query[:150] + "..."
                        if len(query_entry.query) > 150
                        else query_entry.query
                    )
                    missing_upstream = (
                        "Missing upstream. " if not query_entry.upstream_tables else ""
                    )
                    missing_downstream = (
                        "Missing downstream. "
                        if not query_entry.downstream_tables
                        else ""
                    )
                    logger.info(
                        f"Only partial lineage information provided, falling back to SQL parsing for complete lineage detection. {missing_upstream}{missing_downstream}Query: {query_preview}"
                    )
                # No explicit lineage, rely on parsing
                observed_query = ObservedQuery(
                    query=query_entry.query,
                    timestamp=query_entry.timestamp,
                    user=query_entry.user,
                    session_id=query_entry.session_id,
                    default_db=self.config.default_db,
                    default_schema=self.config.default_schema,
                    override_dialect=self.config.override_dialect,
                )
                self.aggregator.add_observed_query(observed_query)

        except Exception as e:
            self.report.num_queries_aggregator_failures += 1
            self.report.warning(
                title="Error adding query to aggregator",
                message="Query skipped due to failure when adding query to SQL parsing aggregator",
                context=query_entry.query,
                exc=e,
            )


class QueryEntry(BaseModel):
    query: str
    timestamp: Optional[datetime] = None
    user: Optional[CorpUserUrn] = None
    operation_type: Optional[str] = None
    downstream_tables: List[DatasetUrn] = Field(default_factory=list)
    upstream_tables: List[DatasetUrn] = Field(default_factory=list)
    session_id: Optional[str] = None

    # Validation context for URN creation
    _validation_context: ClassVar[Optional[SqlQueriesSourceConfig]] = None

    class Config:
        arbitrary_types_allowed = True

    @validator("timestamp", pre=True)
    def parse_timestamp(cls, v):
        return None if v is None else parse_user_datetime(str(v))

    @validator("user", pre=True)
    def parse_user(cls, v):
        if v is None:
            return None

        return v if isinstance(v, CorpUserUrn) else CorpUserUrn(v)

    @validator("downstream_tables", "upstream_tables", pre=True)
    def parse_tables(cls, v):
        if not v:
            return []

        result = []
        for item in v:
            if isinstance(item, DatasetUrn):
                result.append(item)
            elif isinstance(item, str):
                # Skip empty/whitespace-only strings
                if item and item.strip():
                    # Convert to URN using validation context
                    assert cls._validation_context, (
                        "Validation context must be set for URN creation"
                    )
                    urn_string = make_dataset_urn_with_platform_instance(
                        name=item,
                        platform=cls._validation_context.platform,
                        platform_instance=cls._validation_context.platform_instance,
                        env=cls._validation_context.env,
                    )
                    result.append(DatasetUrn.from_string(urn_string))

        return result

    @classmethod
    def create(
        cls, entry_dict: dict, *, config: SqlQueriesSourceConfig
    ) -> "QueryEntry":
        """Create QueryEntry from dict with config context."""
        # Set validation context for URN creation
        cls._validation_context = config
        try:
            return cls.parse_obj(entry_dict)
        finally:
            cls._validation_context = None
