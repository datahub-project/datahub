import importlib.resources as pkg_resource
import logging
import sys
from typing import List, Optional, cast

import lark
from lark import Lark, Tree

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.m_query import resolver, validator
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table

logger = logging.getLogger(__name__)

lark_parser: Optional[Lark] = None


def get_lark_parser():
    global lark_parser
    if lark_parser is not None:
        return lark_parser

    # Read lexical grammar as text
    grammar: str = pkg_resource.read_text(
        "datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule"
    )
    # Create lark parser for the grammar text
    return Lark(grammar, start="let_expression", regex=True)


def _parse_expression(expression: str) -> Tree:
    lark_parser: Lark = get_lark_parser()

    parse_tree: Tree = lark_parser.parse(expression)

    logger.debug(f"Parsing expression = {expression}")

    if (
        logger.level == logging.DEBUG
    ):  # Guard condition to avoid heavy pretty() function call
        logger.debug(parse_tree.pretty())

    return parse_tree


def get_upstream_tables(
    table: Table,
    reporter: PowerBiDashboardSourceReport,
    native_query_enabled: bool = True,
) -> List[resolver.DataPlatformTable]:
    if table.expression is None:
        logger.debug(f"Expression is none for table {table.full_name}")
        return []

    try:
        parse_tree: Tree = _parse_expression(table.expression)

        valid, message = validator.validate_parse_tree(
            parse_tree, native_query_enabled=native_query_enabled
        )
        if valid is False:
            logger.debug(f"Validation failed: {cast(str, message)}")
            reporter.report_warning(table.full_name, cast(str, message))
            return []

        return resolver.MQueryResolver(
            table=table,
            parse_tree=parse_tree,
            reporter=reporter,
        ).resolve_to_data_platform_table_list()  # type: ignore

    except:  # noqa: E722
        # It will catch all type of exceptions, so that ingestion can continue without lineage information
        _, e, _ = sys.exc_info()
        logger.warning(str(e))
        if isinstance(e, lark.exceptions.UnexpectedCharacters):
            reporter.report_warning(
                table.full_name, f"UnSupported expression = {table.expression}"
            )

        logger.debug(f"Fail to parse expression {table.expression}", exc_info=e)

    return []
