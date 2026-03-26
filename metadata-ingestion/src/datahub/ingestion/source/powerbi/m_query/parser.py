import logging
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
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table
from datahub.utilities.threading_timeout import TimeoutException, threading_timeout

logger = logging.getLogger(__name__)


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

    Returns an empty list when the expression is absent, empty, a DAX
    computed-table expression (no ``let`` keyword), or a NativeQuery that the
    caller has opted out of (``native_query_parsing=False``).
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
        # Expressions without a `let` keyword are almost certainly not M-Query
        # (e.g. DAX computed-table expressions like SUMMARIZE(...)). The old
        # Lark parser happened to parse these and then logged INFO "Non-Data
        # Platform Expression". Preserve that behaviour: only warn when the
        # expression looks like it was intended to be M-Query.
        if "let" not in expression.lower():
            reporter.m_query_non_mquery_expressions += 1
            logger.info(
                "Non-M-Query expression in table %s — skipping lineage extraction "
                "(expression does not contain 'let'). Expression: %s. Error: %s",
                table.full_name,
                expression,
                e,
            )
        else:
            reporter.m_query_parse_unknown_errors += 1
            reporter.warning(
                title="Unable to parse M-Query expression",
                message="Got a parse error while parsing the expression. Lineage will be missing for this table.",
                context=f"table-full-name={table.full_name}, expression={expression}",
                exc=e,
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

        if not data_access_func_details:
            logger.debug(
                "No recognized data-access function found in expression for table %s."
                " Expression may use an unsupported source (e.g. Web.Contents,"
                " Excel.Workbook). To add support, reproduce with: %r",
                table.full_name,
                expression,
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

        if lineages:
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
