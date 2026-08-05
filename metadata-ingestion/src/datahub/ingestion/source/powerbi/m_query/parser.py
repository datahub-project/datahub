import logging
import re
from typing import Dict, List, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
)
from datahub.ingestion.source.powerbi.m_query import (
    dax_resolver,
    pattern_handler,
    resolver as mquery_resolver,
)
from datahub.ingestion.source.powerbi.m_query._bridge import (
    MQueryBridgeError,
    MQueryParseError,
    _clear_bridge,
    get_bridge,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    TRACE_POWERBI_MQUERY_PARSER,
    Lineage,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Table,
    matching_sibling_tables as match_sibling_tables,
)
from datahub.utilities.threading_timeout import TimeoutException, threading_timeout

logger = logging.getLogger(__name__)

# Signals that an expression is M-Query. `let` alone is not enough: it is
# sufficient but not necessary for M, so keying off its absence would send a
# *malformed* M-Query to the DAX extractor, fabricating lineage from an
# expression we failed to parse.
#
# `let` as a whole word — a substring check would misfire on names like "Outlet"
# or "Complete" that merely contain the letters "let".
_M_LET_KEYWORD = re.compile(r"\blet\b", re.IGNORECASE)
# M library functions are always namespaced (Table.Combine, Sql.Database,
# Json.Document); DAX functions never are.
_M_NAMESPACED_CALL = re.compile(r"\b[A-Za-z_]\w*\.[A-Za-z_]\w*\s*\(")
# M-only leading keywords and intrinsic literals.
_M_ONLY_SYNTAX = re.compile(
    r"^\s*(try|each|if|section|shared)\b"
    r"|#(table|date|datetime|datetimezone|duration|time|binary)\s*\(",
    re.IGNORECASE,
)


def _looks_like_m_query(expression: str) -> bool:
    """Whether *expression* is M-Query (as opposed to a DAX table expression)."""
    return bool(
        _M_LET_KEYWORD.search(expression)
        or _M_NAMESPACED_CALL.search(expression)
        or _M_ONLY_SYNTAX.search(expression)
    )


def _parse_with_bridge(expression: str, timeout: int) -> Dict[int, dict]:
    """Call the bridge and return the NodeIdMap dict.
    Clears the singleton on bridge crash or timeout so the next call gets a fresh context.
    """
    try:
        with threading_timeout(timeout):
            return get_bridge().parse(expression)
    except MQueryBridgeError:
        _clear_bridge()
        raise
    except TimeoutException:
        # The timeout interrupts the Python thread mid-V8-eval, leaving the MiniRacer
        # context in an undefined state. Clear the singleton so the next call gets a
        # fresh context rather than reusing the potentially-corrupted one.
        _clear_bridge()
        raise


def get_upstream_tables(
    table: Table,
    reporter: PowerBiDashboardSourceReport,
    platform_instance_resolver: AbstractDataPlatformInstanceResolver,
    ctx: PipelineContext,
    config: PowerBiDashboardSourceConfig,
    parameters: Optional[Dict[str, str]] = None,
) -> List[Lineage]:
    """Parse the M-Query expression on *table* and return upstream lineage.

    Covers external data sources (recognized M data-access functions), DAX
    calculated tables, and references to sibling tables in the same dataset
    (surfaced on ``Lineage.powerbi_table_upstreams`` for the mapper to resolve
    to URNs).

    Returns an empty list when the expression is absent or empty, when it is a
    NativeQuery the caller has opted out of (``native_query_parsing=False``), or
    when no upstream could be extracted.
    """
    parameters = parameters or {}

    if table.expression is None:
        logger.debug("There is no M-Query expression in table %s", table.full_name)
        return []

    expression = table.expression

    if not expression.strip():
        logger.debug("Empty M-Query expression in table %s — skipping", table.full_name)
        return []

    if TRACE_POWERBI_MQUERY_PARSER:
        logger.debug(
            "Processing %s m-query expression for lineage extraction. Expression = %s",
            table.full_name,
            expression,
        )

    # Replaces validator.py — correctly suppresses only NativeQuery expressions,
    # fixing the prior bug where native_query_parsing=False suppressed all parsing.
    if not config.native_query_parsing and "Value.NativeQuery" in expression:
        logger.debug(
            "Skipping NativeQuery expression (native_query_parsing=False) for %s",
            table.full_name,
        )
        reporter.m_query_native_query_skipped += 1
        return []

    reporter.m_query_parse_attempts += 1

    try:
        with reporter.m_query_parse_timer:
            node_map = _parse_with_bridge(expression, config.m_query_parse_timeout)
    except TimeoutException:
        reporter.m_query_parse_timeouts += 1
        reporter.warning(
            title="M-Query Parsing Timeout",
            message=f"M-Query parsing timed out after {config.m_query_parse_timeout} seconds. Lineage for this table will not be extracted.",
            context=f"table-full-name={table.full_name}, expression={expression}",
        )
        return []
    except MQueryParseError as e:
        # A genuine M-Query that failed to parse is a real failure — never
        # reinterpret it as DAX. DAX's `Table[Column]` is lexically identical to M
        # record access `id[Field]`, so the DAX extractor would otherwise
        # fabricate references from a broken M-Query and hide the parse error.
        if _looks_like_m_query(expression):
            reporter.m_query_parse_unknown_errors += 1
            reporter.warning(
                title="Unable to parse M-Query expression",
                message="Got a parse error while parsing the expression. Lineage will be missing for this table.",
                context=f"table-full-name={table.full_name}, expression={expression}",
                exc=e,
            )
            return []

        # No `let` keyword — most often a DAX calculated-table expression (e.g.
        # summarize('T', ...)). Try to extract sibling-table references before
        # treating it as an unsupported non-M expression.
        table_refs = (
            dax_resolver.extract_dax_table_references(expression, reporter=reporter)
            if config.extract_table_to_table_lineage
            else []
        )
        if table_refs:
            reporter.m_query_dax_table_lineage += 1
            return [Lineage(powerbi_table_upstreams=table_refs)]

        reporter.m_query_non_mquery_expressions += 1
        logger.info(
            "Non-M-Query expression in table %s — skipping lineage extraction "
            "(no 'let' keyword). Expression: %s. Error: %s",
            table.full_name,
            expression,
            e,
        )
        return []
    except MQueryBridgeError as e:
        reporter.m_query_parse_unknown_errors += 1
        reporter.warning(
            title="Unable to parse M-Query expression",
            message="Got a parse error while parsing the expression. Lineage will be missing for this table.",
            context=f"table-full-name={table.full_name}",
            exc=e,
        )
        return []

    reporter.m_query_parse_successes += 1

    try:
        data_access_func_details = mquery_resolver.resolve_to_data_access_functions(
            node_map, parameters=parameters
        )

        lineages: List[Lineage] = []

        for f_detail in data_access_func_details:
            supported_pattern = pattern_handler.SupportedPattern.get_pattern_handler(
                f_detail.data_access_function_name
            )
            if supported_pattern is None:
                logger.debug(
                    "No handler for data access function %s",
                    f_detail.data_access_function_name,
                )
                continue
            lineage = supported_pattern.handler()(
                ctx=ctx,
                table=table,
                config=config,
                reporter=reporter,
                platform_instance_resolver=platform_instance_resolver,
            ).create_lineage(f_detail)
            if lineage.upstreams:
                lineages.append(lineage)

        data_source_found = bool(lineages)

        # The expression may also reference another table in the same dataset by
        # name (table-to-table lineage), collected regardless of whether an
        # external data source was found since an M-Query can combine both
        # (e.g. Table.Combine({Sql.Database(...), SiblingTable})). Only names that
        # match a real sibling count, so stray identifiers in unsupported sources
        # don't inflate resolver_successes or hide the debug below.
        #
        # Contained separately: a defect here must not discard the external
        # data-source lineage already collected above.
        matched_siblings: List[str] = []
        if config.extract_table_to_table_lineage:
            try:
                candidates = mquery_resolver.resolve_to_table_references(
                    node_map, parameters=parameters
                )
                matched_siblings = [
                    sibling.name for sibling in match_sibling_tables(table, candidates)
                ]
            except Exception as e:
                reporter.m_query_table_reference_errors += 1
                reporter.warning(
                    title="Table-to-table reference extraction failed",
                    message="Sibling-table lineage will be missing for this table; "
                    "external data-source lineage is unaffected. This is likely a "
                    "connector defect rather than a bad expression.",
                    context=f"table-full-name={table.full_name}",
                    exc=e,
                )
        else:
            reporter.m_query_table_to_table_disabled += 1

        matched_sibling_ref = bool(matched_siblings)
        if matched_siblings:
            lineages.append(Lineage(powerbi_table_upstreams=matched_siblings))

        if data_source_found or matched_sibling_ref:
            reporter.m_query_resolver_successes += 1
        else:
            reporter.m_query_resolver_no_lineage += 1
            if data_access_func_details:
                # Function(s) were recognized but all handlers returned empty —
                # the per-handler debug logs above explain why. Log the expression
                # here so it can be copy-pasted into a local test for investigation.
                logger.debug(
                    "Recognized function(s) %s but no lineage extracted for table %s."
                    " To reproduce locally: %r",
                    [f.data_access_function_name for f in data_access_func_details],
                    table.full_name,
                    expression,
                )
            else:
                logger.debug(
                    "No recognized data-access function found in expression for table"
                    " %s. Expression may use an unsupported source (e.g. Web.Contents,"
                    " Excel.Workbook). To add support, reproduce with: %r",
                    table.full_name,
                    expression,
                )

        return lineages

    except Exception as e:
        reporter.m_query_resolver_errors += 1
        reporter.warning(
            title="Unknown M-Query Pattern",
            message="Encountered an unknown M-Query Expression",
            context=f"table-full-name={table.full_name}, expression={expression}, message={e}",
            exc=e,
        )
        return []
