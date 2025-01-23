import functools
import importlib.resources as pkg_resource
import logging
import os
from typing import Dict, List

import lark
from lark import Lark, Tree

import datahub.ingestion.source.powerbi.m_query.data_classes
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
)
from datahub.ingestion.source.powerbi.m_query import resolver, validator
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    TRACE_POWERBI_MQUERY_PARSER,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table
from datahub.utilities.threading_timeout import TimeoutException, threading_timeout

logger = logging.getLogger(__name__)

_M_QUERY_PARSE_TIMEOUT = int(os.getenv("DATAHUB_POWERBI_M_QUERY_PARSE_TIMEOUT", 60))


@functools.lru_cache(maxsize=1)
def get_lark_parser() -> Lark:
    # Read lexical grammar as text
    grammar: str = pkg_resource.read_text(
        "datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule"
    )
    # Create lark parser for the grammar text
    return Lark(grammar, start="let_expression", regex=True)


def _parse_expression(expression: str, parse_timeout: int = 60) -> Tree:
    lark_parser: Lark = get_lark_parser()

    # Replace U+00a0 NO-BREAK SPACE with a normal space.
    # Sometimes PowerBI returns expressions with this character and it breaks the parser.
    expression = expression.replace("\u00a0", " ")

    # Parser resolves the variable=null value to variable='', and in the Tree we get empty string
    # to distinguish between an empty and null set =null to ="null"
    expression = expression.replace("=null", '="null"')

    logger.debug(f"Parsing expression = {expression}")
    with threading_timeout(parse_timeout):
        parse_tree: Tree = lark_parser.parse(expression)

    if TRACE_POWERBI_MQUERY_PARSER:
        logger.debug(parse_tree.pretty())

    return parse_tree


def get_upstream_tables(
    table: Table,
    reporter: PowerBiDashboardSourceReport,
    platform_instance_resolver: AbstractDataPlatformInstanceResolver,
    ctx: PipelineContext,
    config: PowerBiDashboardSourceConfig,
    parameters: Dict[str, str] = {},
) -> List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage]:
    if table.expression is None:
        logger.debug(f"There is no M-Query expression in table {table.full_name}")
        return []

    parameters = parameters or {}

    logger.debug(
        f"Processing {table.full_name} m-query expression for lineage extraction. Expression = {table.expression}"
    )

    try:
        valid, message = validator.validate_parse_tree(
            table.expression, native_query_enabled=config.native_query_parsing
        )
        if valid is False:
            assert message is not None
            logger.debug(f"Validation failed: {message}")
            reporter.info(
                title="Non-Data Platform Expression",
                message=message,
                context=f"table-full-name={table.full_name}, expression={table.expression}, message={message}",
            )
            reporter.m_query_parse_validation_errors += 1
            return []

        with reporter.m_query_parse_timer:
            reporter.m_query_parse_attempts += 1
            parse_tree: Tree = _parse_expression(
                table.expression, parse_timeout=config.m_query_parse_timeout
            )

    except KeyboardInterrupt:
        raise
    except TimeoutException:
        reporter.m_query_parse_timeouts += 1
        reporter.warning(
            title="M-Query Parsing Timeout",
            message=f"M-Query parsing timed out after {config.m_query_parse_timeout} seconds. Lineage for this table will not be extracted.",
            context=f"table-full-name={table.full_name}, expression={table.expression}",
        )
        return []
    except (
        BaseException
    ) as e:  # TODO: Debug why BaseException is needed here and below.
        if isinstance(e, lark.exceptions.UnexpectedCharacters):
            error_type = "Unexpected Character Error"
            reporter.m_query_parse_unexpected_character_errors += 1
        else:
            error_type = "Unknown Parsing Error"
            reporter.m_query_parse_unknown_errors += 1

        reporter.warning(
            title="Unable to parse M-Query expression",
            message=f"Got an '{error_type}' while parsing the expression. Lineage will be missing for this table.",
            context=f"table-full-name={table.full_name}, expression={table.expression}",
            exc=e,
        )
        return []
    reporter.m_query_parse_successes += 1

    try:
        lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
            resolver.MQueryResolver(
                table=table,
                parse_tree=parse_tree,
                reporter=reporter,
                parameters=parameters,
            ).resolve_to_lineage(
                ctx=ctx,
                config=config,
                platform_instance_resolver=platform_instance_resolver,
            )
        )

        if lineage:
            reporter.m_query_resolver_successes += 1
        else:
            reporter.m_query_resolver_no_lineage += 1
        return lineage

    except BaseException as e:
        reporter.m_query_resolver_errors += 1
        reporter.warning(
            title="Unknown M-Query Pattern",
            message="Encountered a unknown M-Query Expression",
            context=f"table-full-name={table.full_name}, expression={table.expression}, message={e}",
            exc=e,
        )
        return []
