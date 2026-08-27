import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any, Dict, Iterable, List, Optional, Union

import smart_open
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from datahub.configuration.common import HiddenFromDocs
from datahub.configuration.datetimes import parse_user_datetime
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
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
)
from datahub.ingestion.api.source import (
    Source,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.ingestion_stage import (
    LINEAGE_EXTRACTION,
    QUERIES_EXTRACTION,
)
from datahub.ingestion.workunit_processors.auto_incremental_lineage import (
    AutoIncrementalLineageProcessor,
)
from datahub.ingestion.workunit_processors.auto_workunits_reporter import (
    AutoWorkunitsReporterProcessor,
)
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
        "Patterns are start-anchored (re.match, like AllowDenyPattern). "
        "For example, 'temp_' matches any table starting with 'temp_'. "
        "This is useful for platforms like Athena "
        "that don't have native temp tables but use naming patterns for fake temp tables.",
        default=[],
    )

    # AWS/S3 configuration
    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="AWS configuration for S3 access. Required when query_file is an S3 URI (s3://).",
    )

    _enable_lazy_schema_loading_removed = pydantic_removed_field(
        "enable_lazy_schema_loading", "August", 2026
    )

    @field_validator("temp_table_patterns")
    @classmethod
    def validate_temp_table_patterns(cls, v: List[str]) -> List[str]:
        for pattern in v:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(
                    f"Invalid regex in temp_table_patterns: '{pattern}': {e}"
                ) from e
        return v

    @model_validator(mode="after")
    def validate_s3_config(self) -> "SqlQueriesSourceConfig":
        if is_s3_uri(self.query_file) and not self.aws_config:
            raise ValueError(
                "aws_config is required when query_file is an S3 URI (s3://)"
            )
        return self

    @cached_property
    def _compiled_temp_table_patterns(self) -> List["re.Pattern[str]"]:
        return [
            re.compile(pattern, re.IGNORECASE) for pattern in self.temp_table_patterns
        ]


@dataclass
class SqlQueriesSourceReport(SourceReport):
    num_entries_processed: int = 0
    num_entries_failed: int = 0
    num_queries_aggregator_failures: int = 0
    num_queries_processed: int = 0
    num_invalid_table_entries: int = 0
    num_temp_table_matches: int = 0

    sql_aggregator: Optional[SqlAggregatorReport] = None
    schema_resolver_report: Optional[SchemaResolverReport] = None


@platform_name("SQL Queries", id="sql-queries")
@config_class(SqlQueriesSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.LINEAGE_COARSE, "Parsed from SQL queries")
@capability(SourceCapability.LINEAGE_FINE, "Parsed from SQL queries")
@capability(SourceCapability.OPERATION_CAPTURE, "Parsed from non-SELECT SQL queries")
class SqlQueriesSource(Source):
    """
    Source that parses SQL queries from a newline-delimited JSON file to generate lineage.

    Implementation notes:
    - Uses SqlParsingAggregator for query parsing and deduplication
    - Optionally uses SchemaResolver to fetch table schemas from DataHub for better parsing accuracy
    - Maintains temp table mappings across queries using session_id
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
            self.report.schema_resolver_report = SchemaResolverReport()

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
            schema_resolver=self.schema_resolver,
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

    def get_allowed_workunit_processors(self):
        return [
            AutoWorkunitsReporterProcessor,
            AutoIncrementalLineageProcessor,
        ]

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        logger.info(f"Parsing queries from {os.path.basename(self.config.query_file)}")

        with self.report.new_stage(QUERIES_EXTRACTION):
            for entry in self._parse_query_file():
                try:
                    self._add_query_to_aggregator(entry)
                    self.report.num_queries_processed += 1
                except (MemoryError, SystemExit, KeyboardInterrupt):
                    raise
                except Exception as e:
                    self.report.num_queries_aggregator_failures += 1
                    self.report.warning(
                        title="Error adding query to aggregator",
                        message="Query skipped due to failure when adding query to SQL parsing aggregator",
                        context=entry.query,
                        exc=e,
                    )

        self._report_run_health()

        with self.report.new_stage(LINEAGE_EXTRACTION):
            logger.info("Generating workunits from SQL parsing aggregator")
            yield from auto_workunit(self.aggregator.gen_metadata())

    def _report_run_health(self) -> None:
        """Report a failure when the run produced nothing usable.

        Without this a run where every entry or every query failed completes
        green with an empty report, indistinguishable from a healthy run with
        nothing to do. Reporting is all this does — the pipeline turns a
        reported failure into a non-zero exit.
        """
        if self.report.num_entries_processed == 0:
            if self.report.num_entries_failed > 0:
                self.report.failure(
                    title="All entries failed to parse",
                    message="Every entry in the input file failed to parse — "
                    "check the file format (expected newline-delimited JSON)",
                    context=f"{self.report.num_entries_failed} entries failed",
                )
            else:
                self.report.warning(
                    title="Empty input",
                    message="No query entries found in input file",
                    context=self.config.query_file,
                )
            return

        if self.report.num_queries_processed == 0:
            self.report.failure(
                title="All queries failed aggregation",
                message="Every query failed during aggregation — "
                "likely a systemic issue (auth, connectivity, config)",
                context=f"{self.report.num_queries_aggregator_failures} failures",
            )

    def _parse_s3_query_file(
        self, aws_config: AwsConnectionConfig
    ) -> Iterable["QueryEntry"]:
        """Parse query file from S3 using smart_open."""
        logger.info(f"Reading query file from S3: {self.config.query_file}")

        try:
            s3_client = aws_config.get_s3_client()
            file_stream_ctx = smart_open.open(
                self.config.query_file, mode="r", transport_params={"client": s3_client}
            )
        except Exception as e:
            self.report.failure(
                title="S3 read error",
                message="Failed to read query file from S3",
                context=self.config.query_file,
                exc=e,
            )
            raise

        with file_stream_ctx as file_stream:
            yield from self._parse_lines(self._guarded_stream(file_stream))

    def _guarded_stream(self, stream: Iterable[str]) -> Iterable[str]:
        """Yield lines, reporting read errors that occur mid-transfer.

        Only stream advancement sits inside the try. Wrapping the yield instead
        would capture exceptions raised by whatever consumes these lines, which
        belong to the consumer, not to reading the file.
        """
        iterator = iter(stream)
        while True:
            try:
                line = next(iterator)
            except StopIteration:
                return
            except Exception as e:
                self.report.failure(
                    title="Query file read error",
                    message="Error reading query file mid-transfer",
                    context=self.config.query_file,
                    exc=e,
                )
                raise
            yield line

    def _parse_local_query_file(self) -> Iterable["QueryEntry"]:
        """Parse local query file."""
        try:
            f = open(self.config.query_file)
        except OSError as e:
            self.report.failure(
                title="Local file read error",
                message="Failed to open local query file",
                context=self.config.query_file,
                exc=e,
            )
            raise

        with f:
            # Guarded too: a truncated read or undecodable bytes surface partway
            # through iteration, well after open() has succeeded.
            yield from self._parse_lines(self._guarded_stream(f))

    def _parse_lines(self, stream: Iterable[str]) -> Iterable["QueryEntry"]:
        """Parse lines from a file stream, yielding QueryEntry objects."""
        for line in stream:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                query_dict = json.loads(stripped, strict=False)
                entry = QueryEntry.create(
                    query_dict, config=self.config, report=self.report
                )
            except (MemoryError, SystemExit, KeyboardInterrupt):
                raise
            except Exception as e:
                # Deliberately broad: this is a per-row parser, and one bad row
                # must never abort the run. Narrowing it to ValueError misses
                # InvalidUrnError (a plain Exception), which a blank "user"
                # field raises — common in exported query logs.
                self.report.num_entries_failed += 1
                self.report.warning(
                    title="Error processing query entry",
                    message="Query skipped due to parsing error",
                    context=stripped,
                    exc=e,
                    log=False,
                )
                if self.report.num_entries_failed % 1000 == 0:
                    # Warnings are sampled, so without this the true volume of a
                    # systematically-malformed file never reaches the log.
                    logger.warning(
                        f"{self.report.num_entries_failed} query entries have failed to parse"
                    )
                continue
            self.report.num_entries_processed += 1
            if self.report.num_entries_processed % 1000 == 0:
                logger.info(
                    f"Processed {self.report.num_entries_processed} query entries"
                )
            yield entry

    def _parse_query_file(self) -> Iterable["QueryEntry"]:
        """Parse the query file and yield QueryEntry objects."""
        if not is_s3_uri(self.config.query_file):
            yield from self._parse_local_query_file()
            return

        aws_config = self.config.aws_config
        if aws_config is None:
            # validate_s3_config rejects this at config time, so this only
            # trips if the config was mutated after construction.
            raise ValueError(f"aws_config is required to read {self.config.query_file}")
        yield from self._parse_s3_query_file(aws_config)

    def _add_query_to_aggregator(self, query_entry: "QueryEntry") -> None:
        """Add a query to the SQL parsing aggregator.

        Raises on systemic errors (graph/auth/connection) so the caller can
        detect and abort.  Per-query data errors should not reach here — they
        are handled during parsing.
        """
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
            if bool(query_entry.upstream_tables) ^ bool(query_entry.downstream_tables):
                side = "upstream" if not query_entry.upstream_tables else "downstream"
                logger.info(
                    "Partial lineage (missing %s), falling back to SQL parsing. Query: %.150s",
                    side,
                    query_entry.query,
                )
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

    def is_temp_table(self, name: str) -> bool:
        """Check if a table name matches any of the configured temp table patterns.

        Uses start-anchored matching (re.match), consistent with AllowDenyPattern.
        """
        for compiled in self.config._compiled_temp_table_patterns:
            if compiled.match(name):
                logger.debug(
                    f"Table '{name}' matched temp table pattern: {compiled.pattern}"
                )
                self.report.num_temp_table_matches += 1
                return True

        return False


class QueryEntry(BaseModel):
    query: str
    timestamp: Optional[datetime] = None
    user: Optional[CorpUserUrn] = None
    downstream_tables: List[DatasetUrn] = Field(default_factory=list)
    upstream_tables: List[DatasetUrn] = Field(default_factory=list)
    session_id: Optional[str] = None

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

        # Exported query logs routinely carry "" for system/background queries.
        # The field is Optional, so treat that as "no actor" exactly like null —
        # rejecting the row instead would discard an otherwise valid query.
        if isinstance(v, str) and not v.strip():
            return None

        return v if isinstance(v, CorpUserUrn) else CorpUserUrn(v)

    @field_validator("downstream_tables", "upstream_tables", mode="before")
    @classmethod
    def parse_tables(cls, v: Any, info: ValidationInfo) -> Any:
        if not v:
            return []

        # A bare string is iterable, so without this it fans out into one URN
        # per character and writes fabricated lineage silently.
        if isinstance(v, (str, bytes)):
            raise ValueError(
                "upstream_tables/downstream_tables must be a list of table names, "
                f"not a bare string: {v!r}"
            )

        context = info.context or {}
        config: Optional[SqlQueriesSourceConfig] = context.get("config")
        # Absent only when a QueryEntry is constructed directly; _parse_lines
        # always supplies one.
        report: Optional[SqlQueriesSourceReport] = context.get("report")

        result: List[DatasetUrn] = []
        for item in v:
            if isinstance(item, DatasetUrn):
                result.append(item)
            elif isinstance(item, str):
                stripped = item.strip()
                if stripped:
                    if config is None:
                        raise ValueError(
                            "Config context required for URN creation from table name strings"
                        )
                    urn_string = make_dataset_urn_with_platform_instance(
                        name=stripped,
                        platform=config.platform,
                        platform_instance=config.platform_instance,
                        env=config.env,
                    )
                    result.append(DatasetUrn.from_string(urn_string))
            elif report is not None:
                # Dropping a lineage hint silently would hide missing lineage,
                # so surface it in the report rather than only the log.
                report.num_invalid_table_entries += 1
                report.warning(
                    title="Invalid table entry",
                    message="Ignoring malformed table reference in lineage hints",
                    context=f"type={type(item).__name__}, value={item!r}",
                )

        return result

    @classmethod
    def create(
        cls,
        entry_dict: Dict[str, Any],
        *,
        config: SqlQueriesSourceConfig,
        report: SqlQueriesSourceReport,
    ) -> "QueryEntry":
        """Create QueryEntry from dict with config context."""
        return cls.model_validate(
            entry_dict, context={"config": config, "report": report}
        )
