import logging
from typing import Iterable, Optional, Union

from typing_extensions import assert_never

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    Source,
    SourceCapability,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql_queries.config import (
    ElasticsearchQuerySourceConfig,
    SqlQueriesSourceConfig,
    SqlTableQuerySourceConfig,
)
from datahub.ingestion.source.sql_queries.query_entry import (
    QueryEntry,
    QueryEntryContext,
)
from datahub.ingestion.source.sql_queries.readers.base import QueryLogReader
from datahub.ingestion.source.sql_queries.readers.elasticsearch import (
    ElasticsearchQueryLogReader,
)
from datahub.ingestion.source.sql_queries.readers.file import FileQueryLogReader
from datahub.ingestion.source.sql_queries.readers.sql_table import (
    SqlTableQueryLogReader,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport
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
from datahub.sql_parsing.schema_resolver import SchemaResolver, SchemaResolverReport
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint

logger = logging.getLogger(__name__)


@platform_name("SQL Queries", id="sql-queries")
@config_class(SqlQueriesSourceConfig)
@support_status(SupportStatus.GA)
@capability(SourceCapability.LINEAGE_COARSE, "Parsed from SQL queries")
@capability(SourceCapability.LINEAGE_FINE, "Parsed from SQL queries")
@capability(SourceCapability.OPERATION_CAPTURE, "Parsed from non-SELECT SQL queries")
class SqlQueriesSource(Source):
    """
    Source that parses SQL query records from a query log to generate lineage and usage.

    Implementation notes:
    - A QueryLogReader yields the query records; the source does not care where they come from
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
        query_source = self.config.query_source
        if query_source is None:
            # validate_query_source guarantees one of the two is set.
            assert self.config.query_file is not None
            return self._create_file_reader(self.config.query_file)
        if isinstance(query_source, ElasticsearchQuerySourceConfig):
            return ElasticsearchQueryLogReader(query_source, self.report)
        if isinstance(query_source, SqlTableQuerySourceConfig):
            return SqlTableQueryLogReader(query_source, self.report)
        assert_never(query_source)

    def _create_file_reader(self, query_file: str) -> FileQueryLogReader:
        return FileQueryLogReader(
            query_file=query_file,
            aws_config=self.config.aws_config,
            entry_context=QueryEntryContext(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                report=self.report,
            ),
            report=self.report,
        )

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
        processed = self.report.num_entries_processed
        failed = self.report.num_entries_failed
        skipped = self.report.num_entries_skipped_no_query
        if processed == 0:
            if failed > 0:
                hint = (
                    "check the file format (expected newline-delimited JSON)"
                    if self.config.query_file
                    else "check the query_source field mappings and record format"
                )
                self.report.failure(
                    title="All entries failed to parse",
                    message=f"Every input record failed to parse — {hint}",
                    context=f"{failed} entries failed",
                )
            elif skipped > 0:
                # Usually field_mappings.query names a field or column that
                # doesn't exist (or differs in case), so every record looks
                # empty.
                self.report.failure(
                    title="No records had SQL",
                    message="Records were read, but none had a value in the "
                    "mapped query field — check field_mappings.query",
                    context=f"{skipped} records read from {self._input_label()}",
                )
            else:
                self.report.warning(
                    title="Empty input",
                    message="No query records found in input",
                    context=self._input_label(),
                )
            return

        if failed > processed:
            # Per-entry errors are tolerated so one bad row can't abort a run,
            # but a majority failing points at something systematic (wrong
            # format, or a bug) that a sampled warning would understate.
            self.report.failure(
                title="Most entries failed to parse",
                message="More entries failed to parse than succeeded — "
                "check the input format",
                context=f"{failed} failed, {processed} parsed",
            )

        if self.report.num_queries_processed == 0:
            self.report.failure(
                title="All queries failed aggregation",
                message="Every query failed during aggregation — "
                "likely a systemic issue (auth, connectivity, config)",
                context=f"{self.report.num_queries_aggregator_failures} failures",
            )

    def _input_label(self) -> str:
        if self.config.query_source is not None:
            return self.config.query_source.label
        return self.config.query_file or ""

    def _add_query_to_aggregator(self, query_entry: QueryEntry) -> None:
        if query_entry.upstream_tables and query_entry.downstream_tables:
            logger.debug("Using explicit lineage from query record")
            for downstream_table in query_entry.downstream_tables:
                known_lineage = KnownQueryLineageInfo(
                    query_text=query_entry.query,
                    downstream=str(downstream_table),
                    upstreams=[str(urn) for urn in query_entry.upstream_tables],
                    timestamp=query_entry.timestamp,
                    session_id=query_entry.session_id,
                )
                self.aggregator.add_known_query_lineage(known_lineage)
            return

        if bool(query_entry.upstream_tables) ^ bool(query_entry.downstream_tables):
            side = "upstream" if not query_entry.upstream_tables else "downstream"
            logger.info(
                "Partial lineage (missing %s), falling back to SQL parsing. Query: %.150s",
                side,
                query_entry.query,
            )
        default_db = query_entry.default_db or self.config.default_db
        default_schema = query_entry.default_schema or self.config.default_schema
        self.aggregator.add_observed_query(
            ObservedQuery(
                query=query_entry.query,
                timestamp=query_entry.timestamp,
                user=query_entry.user,
                session_id=query_entry.session_id,
                default_db=default_db,
                default_schema=default_schema,
                override_dialect=self.config.override_dialect,
                query_hash=self._per_row_query_hash(
                    query_entry.query, default_db, default_schema
                ),
            )
        )

    def _per_row_query_hash(
        self, query: str, default_db: Optional[str], default_schema: Optional[str]
    ) -> Optional[str]:
        """Fingerprint a query resolved against defaults other than the config's.

        The aggregator fingerprints SQL text alone, so the same text resolved
        against two databases would collapse onto one Query and last-write-win
        its lineage. Queries resolved against the config defaults keep the
        aggregator's own fingerprint, so their Query URNs don't change. The pair
        is encoded with repr so an identifier containing a delimiter can't
        collide with a different pair.
        """
        if (default_db, default_schema) == (
            self.config.default_db,
            self.config.default_schema,
        ):
            return None
        return get_query_fingerprint(
            query,
            platform=self.config.platform,
            secondary_id=repr((default_db, default_schema)),
        )

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
