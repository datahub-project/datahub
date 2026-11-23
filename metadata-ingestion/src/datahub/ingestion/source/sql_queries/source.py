import logging
from typing import Iterable, Optional, Union

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
from datahub.ingestion.source.sql_queries.config import SqlQueriesSourceConfig
from datahub.ingestion.source.sql_queries.query_entry import (
    QueryEntry,
    QueryEntryContext,
)
from datahub.ingestion.source.sql_queries.readers.base import QueryLogReader
from datahub.ingestion.source.sql_queries.readers.file import FileQueryLogReader
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
        return FileQueryLogReader(
            query_file=self.config.query_file,
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
        if processed == 0:
            if failed > 0:
                self.report.failure(
                    title="All entries failed to parse",
                    message="Every entry in the input file failed to parse — "
                    "check the file format (expected newline-delimited JSON)",
                    context=f"{failed} entries failed",
                )
            else:
                self.report.warning(
                    title="Empty input",
                    message="No query entries found in input file",
                    context=self.config.query_file,
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
        self.aggregator.add_observed_query(
            ObservedQuery(
                query=query_entry.query,
                timestamp=query_entry.timestamp,
                user=query_entry.user,
                session_id=query_entry.session_id,
                default_db=self.config.default_db,
                default_schema=self.config.default_schema,
                override_dialect=self.config.override_dialect,
            )
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
