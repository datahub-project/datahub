import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import Any, ClassVar, Iterable, List, Optional, Union, cast

import smart_open
from pydantic import BaseModel, ConfigDict, Field, field_validator

from datahub.configuration.common import HiddenFromDocs
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
from datahub.ingestion.api.source_helpers import auto_workunit, auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import CorpUserUrn, DatasetUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver, SchemaResolverReport
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

    use_schema_resolver: HiddenFromDocs[bool] = Field(
        True,
        description="Read SchemaMetadata aspects from DataHub to aid in SQL parsing. Turn off only for testing.",
    )
    default_db: Optional[str] = Field(
        None,
        description="The default database to use for unqualified table names",
    )
    default_schema: Optional[str] = Field(
        None,
        description="The default schema to use for unqualified table names",
    )
    override_dialect: Optional[str] = Field(
        None,
        description="The SQL dialect to use when parsing queries. Overrides automatic dialect detection.",
    )
    temp_table_patterns: List[str] = Field(
        description="Regex patterns for temporary tables to filter in lineage ingestion. "
        "Specify regex to match the entire table name. This is useful for platforms like Athena "
        "that don't have native temp tables but use naming patterns for fake temp tables.",
        default=[],
    )

    enable_lazy_schema_loading: bool = Field(
        default=True,
        description="Enable lazy schema loading for better performance. When enabled, schemas are fetched on-demand "
        "instead of bulk loading all schemas upfront, reducing startup time and memory usage.",
    )

    # AWS/S3 configuration
    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="AWS configuration for S3 access. Required when query_file is an S3 URI (s3://).",
    )


@dataclass
class SqlQueriesSourceReport(SourceReport):
    num_entries_processed: int = 0
    num_entries_failed: int = 0
    num_queries_aggregator_failures: int = 0
    num_queries_processed_sequential: int = 0
    num_temp_tables_detected: int = 0
    temp_table_patterns_used: List[str] = field(default_factory=list)
    peak_memory_usage_mb: float = 0.0

    sql_aggregator: Optional[SqlAggregatorReport] = None
    schema_resolver_report: Optional[SchemaResolverReport] = None


@platform_name("SQL Queries", id="sql-queries")
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

    **Lazy Schema Loading**:
    - Fetches schemas on-demand during query parsing instead of bulk loading all schemas upfront
    - Caches fetched schemas for future lookups to avoid repeated network requests
    - Reduces initial startup time and memory usage significantly
    - Automatically handles large platforms efficiently without memory issues

    **Query Processing**:
    - Loads the entire query file into memory at once
    - Processes all queries sequentially before generating metadata work units
    - Preserves temp table mappings and lineage relationships to ensure consistent lineage tracking
    - Query deduplication is handled automatically by the SQL parsing aggregator

    ### Incremental Lineage
    When `incremental_lineage` is enabled, this source will emit lineage as patches rather than full overwrites.
    This allows you to add lineage edges without removing existing ones, which is useful for:
    - Gradually building up lineage from multiple sources
    - Preserving manually curated lineage
    - Avoiding conflicts when multiple ingestion processes target the same datasets

    Note: Incremental lineage only applies to UpstreamLineage aspects. Other aspects like queries and usage
    statistics will still be emitted normally.

    ### Temporary Table Support
    For platforms like Athena that don't have native temporary tables, you can use the `temp_table_patterns`
    configuration to specify regex patterns that identify fake temporary tables. This allows the source to
    process these tables like other sources that support native temp tables, enabling proper lineage tracking
    across temporary table operations.
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
            # Create schema resolver report for tracking
            self.report.schema_resolver_report = SchemaResolverReport()

            # Use lazy loading - schemas will be fetched on-demand and cached
            logger.info(
                "Using lazy schema loading - schemas will be fetched on-demand and cached"
            )
            self.schema_resolver = SchemaResolver(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                graph=self.graph,
                report=self.report.schema_resolver_report,
            )
        else:
            self.schema_resolver = None

        self.aggregator = SqlParsingAggregator(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=cast(SchemaResolver, self.schema_resolver)
            if self.schema_resolver
            else None,
            eager_graph_load=False,
            generate_lineage=True,  # TODO: make this configurable
            generate_queries=True,  # TODO: make this configurable
            generate_query_subject_fields=True,  # TODO: make this configurable
            generate_query_usage_statistics=True,  # This enables publishing SELECT query entities, otherwise only mutation queries are published
            generate_usage_statistics=True,
            generate_operations=True,  # TODO: make this configurable
            usage_config=self.config.usage,
            is_temp_table=self.is_temp_table
            if self.config.temp_table_patterns
            else None,
            is_allowed_table=None,
            format_queries=False,
        )
        self.report.sql_aggregator = self.aggregator.report

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqlQueriesSource":
        config = SqlQueriesSourceConfig.model_validate(config_dict)
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

        logger.info("Processing all queries in batch mode")
        yield from self._process_queries_batch()

    def _process_queries_batch(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        """Process all queries in memory (original behavior)."""
        with self.report.new_stage("Collecting queries from file"):
            queries = list(self._parse_query_file())
            logger.info(f"Collected {len(queries)} queries for processing")

        with self.report.new_stage("Processing queries through SQL parsing aggregator"):
            logger.info("Using sequential processing")
            self._process_queries_sequential(queries)

        with self.report.new_stage("Generating metadata work units"):
            logger.info("Generating workunits from SQL parsing aggregator")
            yield from auto_workunit(self.aggregator.gen_metadata())

    def _is_s3_uri(self, path: str) -> bool:
        """Check if the path is an S3 URI."""
        return path.startswith("s3://")

    def _parse_s3_query_file(self) -> Iterable["QueryEntry"]:
        """Parse query file from S3 using smart_open."""
        if not self.config.aws_config:
            raise ValueError("AWS configuration required for S3 file access")

        logger.info(f"Reading query file from S3: {self.config.query_file}")

        try:
            # Use smart_open for efficient S3 streaming, similar to S3FileSystem
            s3_client = self.config.aws_config.get_s3_client()

            with smart_open.open(
                self.config.query_file, mode="r", transport_params={"client": s3_client}
            ) as file_stream:
                for line in file_stream:
                    if line.strip():
                        try:
                            query_dict = json.loads(line, strict=False)
                            entry = QueryEntry.create(query_dict, config=self.config)
                            self.report.num_entries_processed += 1
                            if self.report.num_entries_processed % 1000 == 0:
                                logger.info(
                                    f"Processed {self.report.num_entries_processed} query entries from S3"
                                )
                            yield entry
                        except Exception as e:
                            self.report.num_entries_failed += 1
                            self.report.warning(
                                title="Error processing query from S3",
                                message="Query skipped due to parsing error",
                                context=line.strip(),
                                exc=e,
                            )
        except Exception as e:
            self.report.warning(
                title="Error reading S3 file",
                message="Failed to read S3 file",
                context=self.config.query_file,
                exc=e,
            )
            raise

    def _parse_local_query_file(self) -> Iterable["QueryEntry"]:
        """Parse local query file (existing logic)."""
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

    def _parse_query_file(self) -> Iterable["QueryEntry"]:
        """Parse the query file and yield QueryEntry objects."""
        if self._is_s3_uri(self.config.query_file):
            yield from self._parse_s3_query_file()
        else:
            yield from self._parse_local_query_file()

    def _process_queries_sequential(self, queries: List["QueryEntry"]) -> None:
        """Process queries sequentially."""
        total_queries = len(queries)
        logger.info(f"Processing {total_queries} queries sequentially")

        # Process each query sequentially
        for i, query_entry in enumerate(queries):
            self._add_query_to_aggregator(query_entry)
            self.report.num_queries_processed_sequential += 1

            # Simple progress reporting every 1000 queries
            if (i + 1) % 1000 == 0:
                progress_pct = ((i + 1) / total_queries) * 100
                logger.info(
                    f"Processed {i + 1}/{total_queries} queries ({progress_pct:.1f}%)"
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

    def is_temp_table(self, name: str) -> bool:
        """Check if a table name matches any of the configured temp table patterns."""
        if not self.config.temp_table_patterns:
            return False

        try:
            for pattern in self.config.temp_table_patterns:
                if re.match(pattern, name, flags=re.IGNORECASE):
                    logger.debug(
                        f"Table '{name}' matched temp table pattern: {pattern}"
                    )
                    self.report.num_temp_tables_detected += 1
                    return True
        except re.error as e:
            logger.warning(f"Invalid regex pattern '{pattern}': {e}")

        return False


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

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> Any:
        return None if v is None else parse_user_datetime(str(v))

    @field_validator("user", mode="before")
    @classmethod
    def parse_user(cls, v: Any) -> Any:
        if v is None:
            return None

        return v if isinstance(v, CorpUserUrn) else CorpUserUrn(v)

    @field_validator("downstream_tables", "upstream_tables", mode="before")
    @classmethod
    def parse_tables(cls, v: Any) -> Any:
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
            return cls.model_validate(entry_dict)
        finally:
            cls._validation_context = None
