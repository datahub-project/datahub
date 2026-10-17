import logging
import re
from typing import Dict, FrozenSet, List, Optional, Tuple

import sqlglot

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
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect

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

# What the cleaner substitutes for a variable. Named here rather than inline so
# that _CLEANER_PLACEHOLDER_PATTERN below cannot drift away from them.
_GRAFANA_VAR_PLACEHOLDER = "grafana_var"
_GRAFANA_IDENTIFIER_PLACEHOLDER = "grafana_identifier"

# A Grafana variable that survived into a parsed table name: ${var}, [[var]] or a
# leading $var. The lookbehind leaves identifiers that merely contain a dollar
# sign, such as Oracle's V$SESSION, alone.
_UNRESOLVED_VARIABLE_PATTERN = re.compile(r"\$\{|\[\[|(?<![A-Za-z0-9_])\$[A-Za-z_]")

# A name the cleaner invented because it did not know the real one. Only results
# from the cleaned query are tested against this: a table authored as grafana_var
# is a real table when it comes from the query as written.
_CLEANER_PLACEHOLDER_PATTERN = re.compile(
    rf"(?<![A-Za-z0-9_])(?:{_GRAFANA_VAR_PLACEHOLDER}|{_GRAFANA_IDENTIFIER_PLACEHOLDER})"
    r"(?![A-Za-z0-9_])"
)


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
    query = _GRAFANA_BRACKET_VAR_PATTERN.sub(_GRAFANA_IDENTIFIER_PLACEHOLDER, query)

    # Replace ${...} with string literal (handles ${var} and ${var:format})
    query = _GRAFANA_BRACED_VAR_PATTERN.sub(f"'{_GRAFANA_VAR_PLACEHOLDER}'", query)

    # Replace simple $variable format with string literal (but skip if already in quotes)
    # The regex already has negative lookbehind/lookahead to avoid double-quoting
    query = _GRAFANA_SIMPLE_VAR_PATTERN.sub(r"'\1'", query)

    return query


def _dataset_name(urn: str) -> Optional[str]:
    """The dataset-name component of an upstream URN, or None if it will not parse.

    Only that component is a table name; the platform and env around it are not,
    and matching against the whole URN would test them too - a platform instance
    holding a "$" would make a perfectly good upstream look unresolved. So a URN
    that cannot be read is treated as unresolvable rather than passed on whole.
    """
    try:
        return DatasetUrn.from_string(urn).name
    except Exception as e:
        logger.debug(f"Could not read a dataset name from {urn}: {e}")
        return None


def _names_only_the_qualifier(
    name: str, platform_config: PlatformConnectionConfig
) -> bool:
    """True when the name is nothing but the configured database/schema prefix.

    A table-valued function gives the parser no name to qualify, so
    "FROM some_func($1)" resolves to the prefix on its own - "database.schema" -
    which is not a dataset. With a prefix configured, a real table always carries
    a component after it, so the equality cannot catch one by accident.
    """
    prefix = ".".join(
        part
        for part in (platform_config.database, platform_config.database_schema)
        if part
    )
    return bool(prefix) and name == prefix


def _partition_upstreams(
    result: SqlParsingResult,
    platform_config: PlatformConnectionConfig,
    from_cleaned_query: bool,
) -> Tuple[List[str], List[str]]:
    """Split the parsed upstreams into those naming a real dataset and those not.

    Rejecting the whole result when a single table is unresolvable throws away
    lineage we do know: "SELECT a.x FROM real_table a JOIN [[tbl]] b" names one
    table we can resolve and one we cannot, and the first is still worth emitting.

    This can only save what the chosen query form actually parsed. A bare "$var"
    in a comma-separated FROM list is cleaned to a quoted string that some
    dialects reject outright, and then there are no upstreams to partition and the
    real table beside it is lost as well - as it is without this change.

    from_cleaned_query additionally rejects the names the cleaner invents. Those
    can only appear on that path, so a table genuinely authored as "grafana_var"
    is still accepted when it comes from the query as written.

    Not every invented name is recognisable. On BigQuery sqlglot resolves a
    quoted "$tbl" to the default schema repeated in the table position, which is
    indistinguishable from a table that really is named after its schema, so that
    one is left alone rather than risk dropping a real upstream.

    The cleaner's placeholders are also not unique per query, so a table actually
    named "grafana_identifier" joined to a "[[var]]" collapses to a single name
    when cleaned and is dropped here. Nothing downstream can tell the two apart by
    then - the distinction is lost in the substitution, and today's behaviour of
    emitting the placeholder is only coincidentally right for that one name - so
    the drop is reported rather than guessed at.
    """
    kept: List[str] = []
    dropped: List[str] = []
    for urn in result.in_tables:
        name = _dataset_name(urn)
        unresolvable = name is None or (
            _UNRESOLVED_VARIABLE_PATTERN.search(name) is not None
            or _names_only_the_qualifier(name, platform_config)
            or (
                from_cleaned_query
                and _CLEANER_PLACEHOLDER_PATTERN.search(name) is not None
            )
        )
        (dropped if unresolvable else kept).append(urn)
    return kept, dropped


def _restrict_to_upstreams(
    result: SqlParsingResult, kept: List[str]
) -> SqlParsingResult:
    """The same result with its upstreams narrowed to kept.

    Column lineage is narrowed alongside them: a fine-grained edge pointing at a
    dropped table would create in DataHub the very dataset the drop avoided.
    """
    keep = set(kept)
    column_lineage = None
    if result.column_lineage is not None:
        column_lineage = []
        for col_lineage in result.column_lineage:
            upstreams = [ref for ref in col_lineage.upstreams if ref.table in keep]
            # A column with no upstreams to begin with - a constant, say - is left
            # as it is; only ones emptied by the narrowing are discarded.
            if upstreams or not col_lineage.upstreams:
                column_lineage.append(
                    col_lineage.model_copy(update={"upstreams": upstreams})
                )
    return result.model_copy(
        update={"in_tables": kept, "column_lineage": column_lineage}
    )


# What the probe below substitutes for a variable.
_GRAFANA_PROBE_IDENTIFIER = "grafana_probe"

# ${...}, matched unconditionally. The probe has to substitute inside string
# literals too - an authored '${var}' must become 'grafana_probe' - so it cannot
# share _GRAFANA_BRACED_VAR_PATTERN, which is a candidate for being narrowed to
# leave already-quoted variables alone.
_GRAFANA_ANY_BRACED_VAR_PATTERN = re.compile(r"\$\{[^}]+\}")

# $var, but never a $__ macro and never a dollar sign inside a longer identifier.
#
# Macros are left exactly as the author wrote them. A macro is a Grafana built-in
# standing for a time value or a predicate, so it can never be a table name and
# substituting it would tell the comparison nothing - while the cleaner's macro
# handling is itself capable of breaking a query that parses as authored, because
# $__timeFrom() produces a value but is replaced with TRUE. Leaving macros alone
# keeps the probe parseable wherever the query as authored is.
#
# The lookbehind is the same one _UNRESOLVED_VARIABLE_PATTERN carries, for the
# same reason: "V$SESSION" is a table, not a variable. Substituting inside it
# would make the two parses disagree over a name that was never in doubt, and the
# query would be cleaned - which rewrites "v$session" to "v'$session'" and leaves
# a truncated "v" behind. The cost is that a variable appended to a table name,
# "metrics_$env", is indistinguishable from such an identifier and is emitted as
# authored.
_GRAFANA_PROBE_SIMPLE_VAR_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])\$(?!__)[a-zA-Z_][a-zA-Z0-9_]*"
)


def _probe_grafana_template_variables(query: str) -> str:
    """Substitute every Grafana variable - and only those - with one fixed token.

    This exists to be compared against the query as authored, never to be emitted.
    Deciding whether a variable reached a table name needs a second parse, and the
    cleaner is a poor source of one: it substitutes a quoted string, which is not
    valid where a table name belongs and which mangles any literal the variable
    sat inside, so it frequently fails to parse - and a failed parse leaves the
    comparison blind.

    Because the probe differs from the query as authored in variable positions
    alone, a difference in the table names between the two parses can only have
    come from a variable.
    """
    query = _GRAFANA_BRACKET_VAR_PATTERN.sub(_GRAFANA_PROBE_IDENTIFIER, query)
    query = _GRAFANA_ANY_BRACED_VAR_PATTERN.sub(_GRAFANA_PROBE_IDENTIFIER, query)
    return _GRAFANA_PROBE_SIMPLE_VAR_PATTERN.sub(_GRAFANA_PROBE_IDENTIFIER, query)


def _parsed_table_names(
    query: str, dialect: sqlglot.Dialect
) -> Optional[FrozenSet[str]]:
    """Table names as sqlglot renders them, or None if the query will not parse.

    Rendering, rather than reading each table's name, is what makes the comparison
    work across dialects: Postgres and Snowflake turn a leading "$" into a
    parameter node whose name is empty, and only the rendering still shows the
    sigil. The renderings are not normalised either - both sides of the comparison
    come from the same dialect, so any difference is a real difference.

    This is a bare parse - no schema resolution and no column lineage - so it is
    cheap enough to run twice for every query.
    """
    try:
        statement = sqlglot.parse_one(query, dialect=dialect)
    except Exception as e:
        logger.debug(f"Query did not parse as {dialect}: {e}")
        return None

    if statement is None:
        return None

    return frozenset(
        table.sql(dialect=dialect) for table in statement.find_all(sqlglot.exp.Table)
    )


def _select_query_for_lineage(sql: str, dialect: sqlglot.Dialect) -> Optional[str]:
    """Choose which form of the query to extract lineage from, or None for neither.

    Parse the query as authored, parse a probe of it, and compare the table names:

        names agree    -> no variable reached a table name -> use it as authored
        names differ   -> a variable reached a table name   -> use the cleaned form
        neither parses -> no lineage

    Nothing here asks whether a given name was derived from a variable. The
    cleaner already knows where the variables are - substituting them is its whole
    job - so the *difference* between two parses is the signal. That makes the
    choice dialect-agnostic (a sigil that Postgres drops and Trino keeps is
    irrelevant when both forms are parsed the same way), position-agnostic (a
    variable in a catalog or schema qualifier is caught as readily as one standing
    in for the table itself) and substring-safe ("metrics_$env").
    """
    cleaned = _clean_grafana_template_variables(sql)
    if cleaned == sql:
        # Nothing was substituted, so no variable can have reached a table name.
        return sql if _parsed_table_names(sql, dialect) is not None else None

    authored_names = _parsed_table_names(sql, dialect)
    probe_names = _parsed_table_names(_probe_grafana_template_variables(sql), dialect)

    if authored_names is not None and probe_names is not None:
        return sql if authored_names == probe_names else cleaned

    # No usable comparison, so cleaning is the only remaining hope - it is what
    # rescues [[var]] used as a table name, which no dialect parses. Never fall
    # back to the query as authored here: with no comparison to rely on there is
    # nothing to show that a variable did not reach a table name.
    return cleaned if _parsed_table_names(cleaned, dialect) is not None else None


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

    def _parse_sql(
        self, sql: str, platform_config: PlatformConnectionConfig
    ) -> Optional[SqlParsingResult]:
        """Parse SQL query for lineage information.

        Grafana template variables usually survive sqlglot unchanged - a quoted
        '${var}' is an ordinary string literal, and an unquoted one parses as a
        placeholder - so the query is used as authored wherever that is safe.
        Cleaning is the fallback for the queries that genuinely need it, such as
        [[var]] used as a table name.

        Cleaning unconditionally, as it used to, breaks queries that parse
        perfectly well as written: it substitutes a quoted string for every
        variable, which turns '${var}' into ''grafana_var'' and mangles any
        literal a variable sat inside. create_lineage_sql_parsed_result does not
        raise on the result - it returns a truthy result carrying the error and no
        tables - so the panel silently fell back to lineage naming the Grafana
        datasource UID, a dataset that does not exist upstream.
        """
        if not self.graph:
            logger.warning("No DataHub graph specified for SQL parsing.")
            return None

        try:
            dialect = get_dialect(platform_config.platform)
        except Exception as e:
            # A misspelled platform used to be absorbed by the lineage parser and
            # surfaced as a parse error on the result. Choosing the query form
            # needs the dialect up front, so say so once per datasource instead of
            # degrading every panel to the datasource fallback in silence.
            self.report.warning(
                title="Configured platform is not a known SQL dialect",
                message="Panel queries for this datasource cannot be parsed, so "
                "lineage falls back to the datasource, which names a dataset that "
                "may not exist upstream. Check the platform name in "
                "connection_to_platform_map.",
                context=f"platform={platform_config.platform}",
                exc=e,
            )
            return None

        try:
            query = _select_query_for_lineage(sql, dialect)
        except (TypeError, AttributeError) as e:
            # GrafanaQueryTarget is Dict[str, Any], so rawSql can hold any JSON
            # value despite _extract_raw_sql's Optional[str] annotation, and
            # substitution over a non-string raises. Cleaning used to happen
            # inside this method's try block; keep returning None as it did.
            logger.warning(f"Panel SQL is not a string, so it cannot be parsed: {e}")
            return None

        result: Optional[SqlParsingResult] = None
        kept: List[str] = []
        dropped: List[str] = []
        if query is not None:
            result = self._run_sql_parser(query, platform_config)
            if result is not None:
                kept, dropped = _partition_upstreams(
                    result, platform_config, from_cleaned_query=query != sql
                )

        if result is None or not kept:
            # Returning None rather than a result holding invented names keeps
            # unresolved variables from being emitted as though they were datasets.
            # create_lineage_sql_parsed_result reports a parse failure on the result
            # rather than raising, so the error has to be carried across explicitly
            # or the warning cannot say why the query failed.
            self.report.warning(
                title="Panel SQL did not resolve to upstream tables",
                message="Neither the query as authored nor its cleaned form "
                "produced upstream tables naming real datasets. Lineage falls back "
                "to the datasource, which names a dataset that may not exist "
                "upstream.",
                context=f"platform={platform_config.platform}, sql={sql[:200]}",
                exc=result.debug_info.table_error if result is not None else None,
            )
            return None

        if dropped:
            self.report.warning(
                title="Some panel SQL upstreams could not be resolved",
                message="Part of the query names tables this connector cannot "
                "resolve to real datasets, usually a Grafana variable standing in "
                "for a table name. Those upstreams are dropped; the rest are "
                "emitted.",
                context=f"platform={platform_config.platform}, "
                f"dropped={len(dropped)}, kept={len(kept)}, sql={sql[:200]}",
            )

        return _restrict_to_upstreams(result, kept)

    def _run_sql_parser(
        self, sql: str, platform_config: PlatformConnectionConfig
    ) -> Optional[SqlParsingResult]:
        """Run the SQL parser over one form of the query."""
        try:
            return create_lineage_sql_parsed_result(
                query=sql,
                platform=platform_config.platform,
                platform_instance=platform_config.platform_instance,
                env=platform_config.env,
                default_db=platform_config.database,
                default_schema=platform_config.database_schema,
                graph=self.graph,
            )
        except ValueError as e:
            logger.error(f"SQL parsing error for query: {sql}", exc_info=e)
        except Exception as e:
            logger.exception(f"Unexpected error during SQL parsing: {sql}", exc_info=e)

        return None

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
