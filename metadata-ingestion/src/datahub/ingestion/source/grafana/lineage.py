import logging
import re
from typing import Dict, List, Optional, Tuple

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.models import (
    DatasourceRef,
    GrafanaQueryTarget,
    Panel,
)
from datahub.ingestion.source.grafana.report import GrafanaSourceReport
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)

# Precompiled regex patterns for Grafana template variable cleaning
# These patterns remove Grafana-specific template syntax to make SQL parseable

# Time/filter macros with parentheses: $__timeFilter(column), $__timeGroup(...)
# Replace with TRUE as they form complete boolean expressions
_GRAFANA_TIME_MACRO_WITH_ARGS_PATTERN = re.compile(r"\$__time[A-Z]\w*\([^)]*\)")
_GRAFANA_FILTER_MACRO_WITH_ARGS_PATTERN = re.compile(r"\$__[a-z]+Filter\([^)]*\)")

# Time/filter macros WITHOUT parentheses (standalone): $__timeFilter, $__interval
# These are used as predicates and need to be replaced with valid SQL expressions
_GRAFANA_TIME_MACRO_STANDALONE_PATTERN = re.compile(r"\$__time[A-Z]\w*(?!\()")
_GRAFANA_FILTER_MACRO_STANDALONE_PATTERN = re.compile(r"\$__[a-z]+Filter(?!\()")

# Generic macros (with or without args): $__interval, $__range, etc.
_GRAFANA_GENERIC_MACRO_PATTERN = re.compile(r"\$__\w+(?:\([^)]*\))?")

# Bracket and braced variables
_GRAFANA_BRACKET_VAR_PATTERN = re.compile(r"\[\[[^\]]+\]\]")
_GRAFANA_BRACED_VAR_PATTERN = re.compile(r"\$\{[^}]+\}")

# Simple variables NOT inside quotes: $var
# Use negative lookbehind/lookahead to skip variables already in quotes
_GRAFANA_SIMPLE_VAR_PATTERN = re.compile(r"(?<!')(\$[a-zA-Z_][a-zA-Z0-9_]*)(?!')")


def _clean_grafana_template_variables(query: str) -> str:
    """
    Remove Grafana template variables from SQL query for parsing.

    Grafana supports multiple variable syntaxes that break SQL parsers:
    - ${variable} or ${variable:format} - Modern syntax with optional formatting
    - [[variable]] - Deprecated bracket syntax
    - $variable - Simple dollar syntax
    - $__macro(...) or $__macro - Built-in macros (with/without parentheses)

    Supported formatting options (in ${var:format}):
    - csv, pipe, json, raw, etc.

    Supported built-in macros and variables:
    - Time macros: $__timeFilter, $__timeFrom, $__timeTo, $__timeGroup
    - Global variables: $__interval, $__range, $__dashboard, $__user, $__org
    - Advanced: $__interval_ms, $__range_s, $__rate_interval

    Replace with valid SQL placeholders to maintain parseability for lineage extraction.

    Replacement strategy:
    - Macros with args: $__timeFilter(column) -> TRUE (complete boolean expression)
    - Standalone macros: $__timeFilter -> > TIMESTAMP '2000-01-01' (valid predicate)
    - ${...} variables -> 'grafana_var' (string literal)
    - [[...]] identifiers -> grafana_identifier (valid identifier)
    - $simple variables (not in quotes) -> 'grafana_var' (string literal)
    - Variables already in quotes: '$var' -> left unchanged

    Examples:
        ${__from:date:'YYYY/MM/DD'} -> 'grafana_var'
        ${servers:csv} -> 'grafana_var'
        [[table_name]] -> grafana_identifier
        $__timeFilter(column) -> TRUE
        WHERE event_timestamp $__timeFilter -> WHERE event_timestamp > TIMESTAMP '2000-01-01'
        $__interval -> 1
        WHERE status = '$status' -> WHERE status = '$status' (unchanged - already quoted)
        WHERE status = $status -> WHERE status = 'grafana_var'
    """

    # Replace time/filter macros WITH args with TRUE (they form complete boolean expressions)
    # e.g., $__timeFilter(column) -> TRUE
    query = _GRAFANA_TIME_MACRO_WITH_ARGS_PATTERN.sub("TRUE", query)
    query = _GRAFANA_FILTER_MACRO_WITH_ARGS_PATTERN.sub("TRUE", query)

    # Replace standalone time/filter macros with valid predicates
    # e.g., "WHERE event_timestamp $__timeFilter" -> "WHERE event_timestamp > TIMESTAMP '2000-01-01'"
    query = _GRAFANA_TIME_MACRO_STANDALONE_PATTERN.sub(
        "> TIMESTAMP '2000-01-01'", query
    )
    query = _GRAFANA_FILTER_MACRO_STANDALONE_PATTERN.sub(
        "> TIMESTAMP '2000-01-01'", query
    )

    # Replace other macros with 1 (safe numeric value for intervals, ranges, etc.)
    query = _GRAFANA_GENERIC_MACRO_PATTERN.sub("1", query)

    # Replace [[...]] with identifier (deprecated syntax, often used for table/column names)
    query = _GRAFANA_BRACKET_VAR_PATTERN.sub("grafana_identifier", query)

    # Replace ${...} with string literal (handles ${var} and ${var:format})
    query = _GRAFANA_BRACED_VAR_PATTERN.sub("'grafana_var'", query)

    # Replace simple $variable format with string literal (but skip if already in quotes)
    # The regex already has negative lookbehind/lookahead to avoid double-quoting
    query = _GRAFANA_SIMPLE_VAR_PATTERN.sub(r"'\1'", query)

    return query


class LineageExtractor:
    """Handles extraction of lineage information from Grafana panels"""

    def __init__(
        self,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        connection_to_platform_map: Dict[str, PlatformConnectionConfig],
        report: GrafanaSourceReport,
        graph: Optional[DataHubGraph] = None,
        include_column_lineage: bool = True,
    ):
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env
        self.connection_map = connection_to_platform_map
        self.graph = graph
        self.report = report
        self.include_column_lineage = include_column_lineage
        self._warned_missing_graph = False

    def extract_panel_lineage(
        self, panel: Panel, dashboard_uid: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Extract lineage information from a panel."""
        if not panel.datasource_ref:
            return None

        ds_type, ds_uid = self._extract_datasource_info(panel.datasource_ref)
        raw_sql = self._extract_raw_sql(panel.query_targets)
        ds_urn = self._build_dataset_urn(ds_type, ds_uid, dashboard_uid, panel.id)

        # Handle platform-specific lineage
        if ds_uid in self.connection_map:
            if raw_sql:
                parsed_sql = self._parse_sql(raw_sql, self.connection_map[ds_uid])
                if parsed_sql:
                    lineage = self._create_column_lineage(ds_urn, parsed_sql)
                    if lineage:
                        return lineage

            # Fall back to basic lineage if SQL parsing fails or no column lineage created
            return self._create_basic_lineage(
                ds_uid, self.connection_map[ds_uid], ds_urn
            )

        return None

    def _extract_datasource_info(
        self, datasource_ref: "DatasourceRef"
    ) -> Tuple[str, str]:
        """Extract datasource type and UID."""
        return datasource_ref.type or "unknown", datasource_ref.uid or "unknown"

    def _extract_raw_sql(
        self, query_targets: List["GrafanaQueryTarget"]
    ) -> Optional[str]:
        """Extract raw SQL from panel query targets."""
        for target in query_targets:
            # Handle case variations: rawSql, rawSQL, etc.
            for key, value in target.items():
                if key.lower() == "rawsql" and value:
                    return value
        return None

    def _build_dataset_urn(
        self, ds_type: str, ds_uid: str, dashboard_uid: str, panel_id: str
    ) -> str:
        """Build per-panel dataset URN with global uniqueness."""
        dataset_name = f"{ds_type}.{ds_uid}.{dashboard_uid}.{panel_id}"
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.platform_instance,
            env=self.env,
        )

    def _create_basic_lineage(
        self, ds_uid: str, platform_config: PlatformConnectionConfig, ds_urn: str
    ) -> MetadataChangeProposalWrapper:
        """Create basic upstream lineage."""
        name = (
            f"{platform_config.database}.{ds_uid}"
            if platform_config.database
            else ds_uid
        )

        upstream_urn = make_dataset_urn_with_platform_instance(
            platform=platform_config.platform,
            name=name,
            platform_instance=platform_config.platform_instance,
            env=platform_config.env,
        )

        logger.info(f"Generated upstream URN: {upstream_urn}")

        return MetadataChangeProposalWrapper(
            entityUrn=ds_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=upstream_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                ]
            ),
        )

    def _parse_context(
        self, sql: object, platform_config: PlatformConnectionConfig
    ) -> str:
        """Context line for a parsing report entry.

        The query is coerced to text before truncating: rawSql comes from
        user-authored dashboard JSON, so it is not guaranteed to be a string, and
        reporting a failure must not fail on the value it is reporting.
        """
        return f"platform={platform_config.platform}, sql={str(sql)[:200]}"

    def _report_unparseable_sql(
        self,
        sql: str,
        platform_config: PlatformConnectionConfig,
        exc: Optional[BaseException] = None,
    ) -> None:
        """Record a panel query the parser could not read."""
        self.report.report_sql_parsing_failure()
        self.report.warning(
            title="Panel SQL could not be parsed",
            message="Lineage falls back to the datasource, which names a dataset "
            "that may not exist upstream.",
            context=self._parse_context(sql, platform_config),
            exc=exc,
        )

    def _report_unreachable_graph(
        self,
        sql: str,
        platform_config: PlatformConnectionConfig,
        exc: BaseException,
    ) -> None:
        """Record a failure to reach the graph while resolving schemas.

        Kept apart from an unparseable query because the cause and the remedy are
        different: this is one systemic outage, not a per-panel SQL problem, and
        filing it under the same title would bury it behind hundreds of entries
        that suggest the dashboards are at fault.
        """
        self.report.report_sql_parsing_failure()
        self.report.warning(
            title="Unable to reach DataHub graph for SQL parsing",
            message="Schema resolution failed, so panel lineage falls back to the "
            "datasource, which names a dataset that may not exist upstream.",
            context=self._parse_context(sql, platform_config),
            exc=exc,
        )

    def _parse_sql(
        self, sql: str, platform_config: PlatformConnectionConfig
    ) -> Optional[SqlParsingResult]:
        """Parse SQL query for lineage information."""
        if not self.graph:
            # Config-level, so it is true for every panel; warn once rather than
            # logging the same line hundreds of times.
            if not self._warned_missing_graph:
                self._warned_missing_graph = True
                self.report.warning(
                    title="No DataHub graph configured for SQL parsing",
                    message="Panel queries cannot be parsed without a graph, so "
                    "lineage falls back to the datasource, which names a dataset that "
                    "may not exist upstream. Set datahub_api in the recipe.",
                )
            return None

        self.report.report_sql_parsing_attempt()
        try:
            # Clean Grafana template variables before parsing
            # Variables like ${__from}, $datasource, [[var]] break SQL parsers
            cleaned_sql = _clean_grafana_template_variables(sql)

            parsed_sql = create_lineage_sql_parsed_result(
                query=cleaned_sql,
                platform=platform_config.platform,
                platform_instance=platform_config.platform_instance,
                env=platform_config.env,
                default_db=platform_config.database,
                default_schema=platform_config.database_schema,
                graph=self.graph,
            )
        # create_schema_resolver runs outside create_lineage_sql_parsed_result's own
        # try block, so a failure to reach the graph surfaces here as an exception
        # rather than as a parse error on the result. Nothing else raises: every
        # parse error is caught in there and returned on the result.
        except Exception as e:
            self._report_unreachable_graph(sql, platform_config, exc=e)
            return None

        # create_lineage_sql_parsed_result does not raise on a malformed query: it
        # returns a truthy result carrying the error and no tables. Left unread,
        # the run reports success while emitting datasource-UID lineage instead.
        if parsed_sql.debug_info.table_error:
            self._report_unparseable_sql(
                sql, platform_config, exc=parsed_sql.debug_info.table_error
            )
            return None

        if not parsed_sql.in_tables:
            # A stat panel selecting a constant or calling now() is perfectly
            # legitimate and parses cleanly; it simply has no upstream. Reported so
            # the datasource fallback is still traceable, but not as a failure -
            # that would flag ordinary dashboards as broken.
            self.report.report_no_lineage()
            self.report.info(
                title="Panel SQL names no upstream tables",
                message="The query parsed but references no tables, so lineage falls "
                "back to the datasource.",
                context=self._parse_context(sql, platform_config),
            )
            return None

        if parsed_sql.debug_info.column_error and self.include_column_lineage:
            # Table lineage survived, so this is not a parse failure - but column
            # lineage is silently missing unless it is said out loud.
            self.report.warning(
                title="Panel SQL column lineage unavailable",
                message="Table lineage was extracted but column lineage could not be, "
                "so the panel's field-level lineage is incomplete.",
                context=self._parse_context(sql, platform_config),
                exc=parsed_sql.debug_info.column_error,
            )

        self.report.report_sql_parsing_success()
        return parsed_sql

    def _create_column_lineage(
        self,
        dataset_urn: str,
        parsed_sql: SqlParsingResult,
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create column-level lineage and dataset-level lineage from parsed SQL"""
        # Always create dataset-level lineage if we have upstream tables
        if not parsed_sql.in_tables:
            return None

        upstream_lineages = []
        # Add column-level lineage if available and enabled
        if parsed_sql.column_lineage and self.include_column_lineage:
            for col_lineage in parsed_sql.column_lineage:
                upstream_lineages.append(
                    FineGrainedLineageClass(
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=(
                            [
                                make_schema_field_urn(
                                    dataset_urn, col_lineage.downstream.column
                                )
                            ]
                            if col_lineage.downstream.column
                            else []
                        ),
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=col_lineage.upstream_schema_field_urns(),
                    )
                )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=table,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    for table in parsed_sql.in_tables
                ],
                fineGrainedLineages=upstream_lineages if upstream_lineages else None,
            ),
        )
