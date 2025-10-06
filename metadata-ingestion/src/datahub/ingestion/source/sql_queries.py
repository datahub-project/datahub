import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import ClassVar, Iterable, List, Optional, Union, cast

import smart_open
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
from datahub.ingestion.api.source_helpers import auto_workunit, auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
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

    use_schema_resolver: bool = Field(
        description="Read SchemaMetadata aspects from DataHub to aid in SQL parsing. Turn off only for testing.",
        default=True,
        hidden_from_docs=True,
    )
    reporting_batch_size: int = Field(
        default=100,
        description="Number of queries to process before reporting progress. "
        "Smaller batches provide more granular progress updates. "
        "Recommended range: 50-1000 queries per batch.",
        ge=1,
        le=10000,
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
    temp_table_patterns: List[str] = Field(
        description="Regex patterns for temporary tables to filter in lineage ingestion. "
        "Specify regex to match the entire table name. This is useful for platforms like Athena "
        "that don't have native temp tables but use naming patterns for fake temp tables.",
        default=[],
    )
    enable_streaming: bool = Field(
        description="Enable streaming processing for large files. When enabled, queries are processed "
        "one batch at a time instead of loading all queries into memory. Significantly reduces memory usage.",
        default=True,
    )
    streaming_batch_size: int = Field(
        description="Batch size for streaming processing. Smaller batches use less memory but require more processing cycles. "
        "Only used when enable_streaming is True.",
        default=1000,
        ge=100,
        le=10000,
    )

    # AWS/S3 configuration
    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="AWS configuration for S3 access. Required when query_file is an S3 URI (s3://).",
    )

    s3_verify_ssl: Optional[Union[bool, str]] = Field(
        default=True,
        description="Whether to verify SSL certificates when accessing S3. Can be True, False, or a path to a CA bundle.",
    )


@dataclass
class SqlQueriesSourceReport(SourceReport, IngestionStageReport):
    num_entries_processed: int = 0
    num_entries_failed: int = 0
    num_queries_aggregator_failures: int = 0
    num_queries_processed_parallel: int = 0
    num_queries_processed_sequential: int = 0
    num_temp_tables_detected: int = 0
    temp_table_patterns_used: List[str] = field(default_factory=list)
    num_streaming_batches_processed: int = 0
    peak_memory_usage_mb: float = 0.0

    sql_aggregator: Optional[SqlAggregatorReport] = None
    schema_resolver_report: Optional[SchemaResolverReport] = None


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

    ### Performance Optimizations
    This source includes several performance optimizations:

    **Lazy Schema Loading**:
    - Fetches schemas on-demand during query parsing instead of bulk loading all schemas upfront
    - Caches fetched schemas for future lookups to avoid repeated network requests
    - Reduces initial startup time and memory usage significantly
    - Automatically handles large platforms efficiently without memory issues

    **Query Processing Modes**:

    **Non-Streaming Processing** (`enable_streaming: false`):
    - Loads the entire query file into memory at once
    - Processes all queries sequentially before generating metadata work units
    - Uses `reporting_batch_size` for progress reporting frequency (every 100 queries by default)

    **Streaming Processing** (`enable_streaming: true` by default):
    - Reads and processes queries in batches without loading the entire file into memory
    - Dramatically reduces memory usage for large files (50-90% reduction)
    - Configurable streaming batch size (`streaming_batch_size: 1000` by default)
    - Ideal for files with 10,000+ queries or limited memory environments
    - Generates and yields metadata work units after each batch

    **General Features**:
    - Preserves temp table mappings and lineage relationships across all processing modes
    - Query deduplication is handled automatically by the SQL parsing aggregator
    - Uses sequential processing to avoid threading issues with SqlParsingAggregator


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

        if self.config.enable_streaming:
            logger.info("Using streaming processing for optimal memory usage")
            yield from self._process_queries_streaming()
        else:
            logger.info("Using batch processing (all queries loaded into memory)")
            yield from self._process_queries_batch()

        # Log processing statistics
        self._log_processing_statistics()

    def _process_queries_streaming(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        """Process queries in streaming fashion to minimize memory usage."""
        with self.report.new_stage("Streaming query processing"):
            query_batch = []

            for query_entry in self._parse_query_file():
                query_batch.append(query_entry)

                # Process batch when it reaches the streaming batch size
                if len(query_batch) >= self.config.streaming_batch_size:
                    yield from self._process_query_batch(query_batch)
                    query_batch = []

            # Process remaining queries in the final batch
            if query_batch:
                yield from self._process_query_batch(query_batch)

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

    def _process_query_batch(
        self, query_batch: List["QueryEntry"]
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        """Process a batch of queries and yield metadata work units."""
        self.report.num_streaming_batches_processed += 1
        logger.debug(
            f"Processing streaming batch {self.report.num_streaming_batches_processed} with {len(query_batch)} queries"
        )

        # Process the batch
        self._process_queries_sequential(query_batch)

        # Generate and yield metadata work units for this batch
        # Note: No stage created here to avoid excessive stage creation for each batch
        yield from auto_workunit(self.aggregator.gen_metadata())

    def _log_processing_statistics(self) -> None:
        """Log comprehensive processing statistics."""
        total_queries_processed = (
            self.report.num_queries_processed_parallel
            + self.report.num_queries_processed_sequential
        )

        if total_queries_processed > 0:
            if self.report.num_queries_processed_parallel > 0:
                logger.info(
                    f"Query processing: {self.report.num_queries_processed_parallel} queries processed in parallel"
                )
            if self.report.num_queries_processed_sequential > 0:
                logger.info(
                    f"Query processing: {self.report.num_queries_processed_sequential} queries processed sequentially"
                )

        if self.config.enable_streaming:
            logger.info(
                f"Streaming processing: {self.report.num_streaming_batches_processed} batches processed"
            )

        # Log schema cache statistics if using schema resolver
        if self.report.schema_resolver_report:
            total_schema_lookups = (
                self.report.schema_resolver_report.num_schema_cache_hits
                + self.report.schema_resolver_report.num_schema_cache_misses
            )
            if total_schema_lookups > 0:
                cache_hit_rate = (
                    self.report.schema_resolver_report.num_schema_cache_hits
                    / total_schema_lookups
                ) * 100
                logger.info(
                    f"Schema cache statistics: {self.report.schema_resolver_report.num_schema_cache_hits} hits, "
                    f"{self.report.schema_resolver_report.num_schema_cache_misses} misses ({cache_hit_rate:.1f}% hit rate)"
                )

    def _is_s3_uri(self, path: str) -> bool:
        """Check if the path is an S3 URI."""
        return path.startswith("s3://")

    def _parse_s3_query_file_streaming(self) -> Iterable["QueryEntry"]:
        """Parse query file from S3 in streaming fashion using smart_open."""
        if not self.config.aws_config:
            raise ValueError("AWS configuration required for S3 file access")

        logger.info(f"Reading query file from S3: {self.config.query_file}")

        try:
            # Use smart_open for efficient S3 streaming, similar to S3FileSystem
            s3_client = self.config.aws_config.get_s3_client(
                verify_ssl=self.config.s3_verify_ssl
            )
            
            with smart_open.open(
                self.config.query_file,
                mode="r",
                transport_params={"client": s3_client}
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
                message=f"Failed to read S3 file: {self.config.query_file}",
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
            yield from self._parse_s3_query_file_streaming()
        else:
            yield from self._parse_local_query_file()

    def _process_queries_sequential(self, queries: List["QueryEntry"]) -> None:
        """Process queries sequentially with optimized batching for better performance."""
        total_queries = len(queries)
        logger.info(
            f"Processing {total_queries} queries sequentially with batching (reporting_batch_size={self.config.reporting_batch_size})"
        )

        # Process queries in batches for progress reporting granularity
        for i in range(0, total_queries, self.config.reporting_batch_size):
            batch = queries[i : i + self.config.reporting_batch_size]
            batch_num = i // self.config.reporting_batch_size + 1
            total_batches = (
                total_queries + self.config.reporting_batch_size - 1
            ) // self.config.reporting_batch_size

            logger.debug(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} queries)"
            )

            # Process each query in the batch
            for query_entry in batch:
                self._add_query_to_aggregator(query_entry)
                self.report.num_queries_processed_sequential += 1

            # Progress reporting every 10 batches or at the end
            # Adjust reporting frequency based on reporting batch size for better granularity
            report_frequency = max(1, min(10, 1000 // self.config.reporting_batch_size))
            if batch_num % report_frequency == 0 or batch_num == total_batches:
                progress_pct = (
                    self.report.num_queries_processed_sequential / total_queries
                ) * 100
                logger.info(
                    f"Processed {self.report.num_queries_processed_sequential}/{total_queries} queries ({progress_pct:.1f}%)"
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
