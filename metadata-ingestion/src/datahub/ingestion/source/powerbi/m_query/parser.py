import functools
import importlib.resources as pkg_resource
import logging
from typing import Dict, List

import lark
from lark import Lark, Tree

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

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def get_lark_parser() -> Lark:
    # Read lexical grammar as text
    grammar: str = pkg_resource.read_text(
        "datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule"
    )
    # Create lark parser for the grammar text
    return Lark(grammar, start="let_expression", regex=True)


def _parse_expression(expression: str) -> Tree:
    lark_parser: Lark = get_lark_parser()

    # Replace U+00a0 NO-BREAK SPACE with a normal space.
    # Sometimes PowerBI returns expressions with this character and it breaks the parser.
    expression = expression.replace("\u00a0", " ")

    logger.debug(f"Parsing expression = {expression}")
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
) -> List[resolver.Lineage]:

    if table.expression is None:
        logger.debug(f"Expression is none for table {table.full_name}")
        return []

    parameters = parameters or {}

    try:
        parse_tree: Tree = _parse_expression(table.expression)

        valid, message = validator.validate_parse_tree(
            parse_tree, native_query_enabled=config.native_query_parsing
        )
        if valid is False:
            assert message is not None
            logger.debug(f"Validation failed: {message}")
            reporter.report_warning(table.full_name, message)
            return []
    except (
        BaseException
    ) as e:  # TODO: Debug why BaseException is needed here and below.
        if isinstance(e, lark.exceptions.UnexpectedCharacters):
            message = "Unsupported m-query expression"
        else:
            message = "Failed to parse m-query expression"

        reporter.report_warning(table.full_name, message)
        logger.info(f"{message} for table {table.full_name}")
        logger.debug(f"Stack trace for {table.full_name}:", exc_info=e)
        return []

    try:
        return resolver.MQueryResolver(
            table=table,
            parse_tree=parse_tree,
            reporter=reporter,
            parameters=parameters,
        ).resolve_to_data_platform_table_list(
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )

    except BaseException as e:
        reporter.report_warning(table.full_name, "Failed to process m-query expression")
        logger.info(
            f"Failed to process m-query expression for table {table.full_name}: {str(e)}"
        )
        logger.debug(f"Stack trace for {table.full_name}:", exc_info=e)

    return []
