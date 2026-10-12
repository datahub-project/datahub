import json
import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any, Dict, Iterable, List, Literal, Optional, Union

import smart_open
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from datahub.configuration.common import ConfigModel, HiddenFromDocs
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
from datahub.ingestion.source.elastic_search_common import ElasticsearchConnectionConfig
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
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint

logger = logging.getLogger(__name__)


class ElasticsearchQueryFieldMappings(ConfigModel):
    """Maps QueryEntry fields to Elasticsearch document fields (dotted paths allowed)."""

    query: str = Field(
        "query", description="Document field holding the SQL query text."
    )
    timestamp: Optional[str] = Field(
        "@timestamp", description="Document field holding the query timestamp."
    )
    user: Optional[str] = Field(
        None, description="Document field holding the executing user."
    )
    session_id: Optional[str] = Field(
        None, description="Document field holding the session/connection id."
    )
    default_db: Optional[str] = Field(
        None,
        description="Document field holding the default database for unqualified table names.",
    )
    default_schema: Optional[str] = Field(
        None,
        description="Document field holding the default schema for unqualified table names.",
    )


class ElasticsearchQuerySourceConfig(ConfigModel):
    type: Literal["elasticsearch"] = "elasticsearch"
    connection: ElasticsearchConnectionConfig = Field(
        default_factory=ElasticsearchConnectionConfig,
        description="Elasticsearch/OpenSearch connection settings.",
    )
    index: str = Field(
        description="Index, alias, or index pattern to read query records from."
    )
    field_mappings: ElasticsearchQueryFieldMappings = Field(
        default_factory=ElasticsearchQueryFieldMappings,
        description="Maps QueryEntry fields to document fields.",
    )
    query_filter: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional Elasticsearch query DSL merged into the bool filter, "
        "e.g. a time range or a term filter to scope which records are read.",
    )
    unescape_sql: bool = Field(
        True,
        description="Unescape backslash-escaped quotes in the SQL text. "
        "Audit logs commonly store SQL with escaped quotes (\\' and \\\").",
    )
    email_domain: Optional[str] = Field(
        None,
        description="If set, bare user values are turned into CorpUser URNs as "
        "'<user>@<email_domain>'.",
    )
    batch_size: int = Field(1000, description="Scroll batch size.")
    scroll_keepalive: str = Field(
        "5m",
        description="How long each scroll batch is kept alive server-side. Raise it "
        "if parsing a batch of batch_size records downstream can take longer than "
        "this, otherwise the scroll can expire mid-run.",
    )


class SqlQueriesSourceConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, IncrementalLineageConfigMixin
):
    query_file: Optional[str] = Field(
        None,
        description="Path to a newline-delimited JSON file of query records "
        "(local path or s3:// URI). Mutually exclusive with query_source.",
    )
    query_source: Optional[ElasticsearchQuerySourceConfig] = Field(
        None,
        description="A non-file backend to read query records from. "
        "Mutually exclusive with query_file.",
    )

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
    def validate_query_source(self) -> "SqlQueriesSourceConfig":
        if bool(self.query_file) == bool(self.query_source):
            raise ValueError("Exactly one of query_file or query_source must be set.")
        if self.query_file and is_s3_uri(self.query_file) and not self.aws_config:
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


class QueryLogReader(ABC):
    """Yields QueryEntry records from a query-log backend.

    Separates the source of query records from the aggregator so a backend can be
    swapped without touching lineage, usage, or reporting.
    """

    @abstractmethod
    def get_query_entries(self) -> Iterable["QueryEntry"]:
        raise NotImplementedError


class FileQueryLogReader(QueryLogReader):
    """Reads query records from a newline-delimited JSON file (local or S3)."""

    def __init__(
        self, config: SqlQueriesSourceConfig, report: SqlQueriesSourceReport
    ) -> None:
        self.config = config
        self.report = report

    def get_query_entries(self) -> Iterable["QueryEntry"]:
        # Guaranteed by the config validator (exactly one of query_file /
        # query_source, and this reader is only built for the file case).
        assert self.config.query_file is not None
        logger.info(f"Parsing queries from {os.path.basename(self.config.query_file)}")
        if not is_s3_uri(self.config.query_file):
            yield from self._parse_local_query_file()
            return

        aws_config = self.config.aws_config
        if aws_config is None:
            # validate_query_source rejects this at config time, so this only
            # trips if the config was mutated after construction.
            raise ValueError(f"aws_config is required to read {self.config.query_file}")
        yield from self._parse_s3_query_file(aws_config)

    def _parse_s3_query_file(
        self, aws_config: AwsConnectionConfig
    ) -> Iterable["QueryEntry"]:
        assert self.config.query_file is not None
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
        assert self.config.query_file is not None
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


class ElasticsearchQueryLogReader(QueryLogReader):
    """Reads query records from an Elasticsearch/OpenSearch index.

    Documents with no SQL in the query field are skipped, so an index that mixes
    query records with other audit events can be read directly.
    """

    def __init__(
        self,
        config: ElasticsearchQuerySourceConfig,
        report: SqlQueriesSourceReport,
    ) -> None:
        self.config = config
        self.report = report

    @staticmethod
    def _get(source: Dict[str, Any], path: Optional[str]) -> Any:
        """Look up a dotted field path in a document source."""
        if not path:
            return None
        current: Any = source
        for part in path.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    def _to_entry(self, source: Dict[str, Any]) -> Optional["QueryEntry"]:
        fm = self.config.field_mappings
        raw_sql = self._get(source, fm.query)
        if raw_sql is None or not str(raw_sql).strip():
            return None

        sql = str(raw_sql)
        if self.config.unescape_sql:
            # Audit logs often store SQL with escaped quotes; undo so it parses.
            sql = sql.replace("\\'", "'").replace('\\"', '"')

        user = self._get(source, fm.user)
        # Treat blank/whitespace users as absent so a system/background row with no
        # actor doesn't become a domain-only "@<email_domain>" corp user.
        user = str(user).strip() if user is not None else ""
        if user and self.config.email_domain and "@" not in user:
            user = f"{user}@{self.config.email_domain}"
        user = user or None

        session_id = self._get(source, fm.session_id)
        # Validate via the model so QueryEntry's field validators run. No config
        # context needed: query records carry no table-name hints.
        return QueryEntry.model_validate(
            {
                "query": sql,
                "timestamp": self._get(source, fm.timestamp),
                "user": user,
                "session_id": str(session_id) if session_id is not None else None,
                "default_db": self._get(source, fm.default_db),
                "default_schema": self._get(source, fm.default_schema),
            },
            context={"report": self.report},
        )

    def get_query_entries(self) -> Iterable["QueryEntry"]:
        try:
            from opensearchpy.helpers import scan
        except ImportError as e:
            raise ImportError(
                "The Elasticsearch query source requires opensearch-py: "
                "pip install 'acryl-datahub[sql-queries]'"
            ) from e

        fm = self.config.field_mappings
        client = self.config.connection.get_client()
        filters: List[Dict[str, Any]] = [{"exists": {"field": fm.query}}]
        if self.config.query_filter:
            filters.append(self.config.query_filter)
        query: Dict[str, Any] = {"query": {"bool": {"filter": filters}}}

        # The aggregator resolves session temp tables from earlier statements, so
        # records must arrive in ascending timestamp order. When a timestamp is
        # mapped, sort on it server-side (preserve_order keeps the scroll in that
        # order, at the cost of the parallel-scroll optimisation); _doc is a stable
        # tiebreaker so records sharing a timestamp keep a deterministic order.
        # Without a timestamp we can't order, so use the faster unordered scroll.
        preserve_order = fm.timestamp is not None
        if preserve_order:
            query["sort"] = [{fm.timestamp: {"order": "asc"}}, {"_doc": "asc"}]

        logger.info(
            f"Reading query records from Elasticsearch index '{self.config.index}'"
        )
        # ponytail: the scroll keepalive must outlast downstream processing of one
        # batch; a batch slower than scroll_keepalive still fails. Upgrade path for
        # very large/slow runs is PIT + search_after, which is stateless per page.
        for hit in scan(
            client,
            index=self.config.index,
            query=query,
            size=self.config.batch_size,
            preserve_order=preserve_order,
            scroll=self.config.scroll_keepalive,
        ):
            source = hit.get("_source", {})
            try:
                entry = self._to_entry(source)
            except (MemoryError, SystemExit, KeyboardInterrupt):
                raise
            except Exception as e:
                # One malformed document must never abort the run; mirror the
                # per-row tolerance of the file reader.
                self.report.num_entries_failed += 1
                self.report.warning(
                    title="Error processing query record",
                    message="Query record skipped due to parsing error",
                    context=str(hit.get("_id")),
                    exc=e,
                    log=False,
                )
                if self.report.num_entries_failed % 1000 == 0:
                    # Warnings are sampled; without this the true volume of a
                    # systematically-malformed index never reaches the log.
                    logger.warning(
                        f"{self.report.num_entries_failed} query records have failed to parse"
                    )
                continue
            if entry is None:
                continue
            self.report.num_entries_processed += 1
            if self.report.num_entries_processed % 1000 == 0:
                logger.info(
                    f"Processed {self.report.num_entries_processed} query records"
                )
            yield entry


@platform_name("SQL Queries", id="sql-queries")
@config_class(SqlQueriesSourceConfig)
@support_status(SupportStatus.GA)
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

        self.reader: QueryLogReader = self._create_reader()

    def _create_reader(self) -> QueryLogReader:
        if self.config.query_source is not None:
            return ElasticsearchQueryLogReader(self.config.query_source, self.report)
        return FileQueryLogReader(self.config, self.report)

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
        with self.report.new_stage(QUERIES_EXTRACTION):
            for entry in self.reader.get_query_entries():
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
                    message="No query entries found in input",
                    context=self.config.query_file
                    or (
                        f"elasticsearch:{self.config.query_source.index}"
                        if self.config.query_source
                        else "unknown"
                    ),
                )
            return

        if self.report.num_queries_processed == 0:
            self.report.failure(
                title="All queries failed aggregation",
                message="Every query failed during aggregation — "
                "likely a systemic issue (auth, connectivity, config)",
                context=f"{self.report.num_queries_aggregator_failures} failures",
            )

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
            # Per-row defaults change how unqualified table names resolve, so the
            # same SQL text under different per-row default_db/default_schema must
            # not collapse onto one query fingerprint (the aggregator's fingerprint
            # is computed from SQL text alone, and a collision last-write-wins the
            # lineage). Config-level defaults are constant across the run and never
            # cause this, so we only disambiguate when the row carries its own.
            query_hash: Optional[str] = None
            if query_entry.default_db or query_entry.default_schema:
                # Gate on truthiness, not "is not None": a blank/empty per-row value
                # falls back to the config default (via `or` below) exactly like an
                # omitted one, so it must not get a distinct fingerprint — otherwise
                # empty-string rows (common in exported logs) would split from the
                # legacy SQL-only Query URN and stop aggregating with it.
                # Disambiguate on the effective (post-config-fallback) defaults so two
                # per-row rows that resolve to the same db/schema still share a
                # fingerprint, and encode the pair structurally so an identifier
                # containing the delimiter can't collide with a different pair.
                query_hash = get_query_fingerprint(
                    query_entry.query,
                    platform=self.config.platform,
                    secondary_id=repr(
                        (
                            query_entry.default_db or self.config.default_db,
                            query_entry.default_schema or self.config.default_schema,
                        )
                    ),
                )
            observed_query = ObservedQuery(
                query=query_entry.query,
                timestamp=query_entry.timestamp,
                user=query_entry.user,
                session_id=query_entry.session_id,
                default_db=query_entry.default_db or self.config.default_db,
                default_schema=query_entry.default_schema or self.config.default_schema,
                override_dialect=self.config.override_dialect,
                query_hash=query_hash,
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
    # Per-row overrides for the source-level default_db/default_schema, so one
    # run can span records from different databases.
    default_db: Optional[str] = None
    default_schema: Optional[str] = None

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
