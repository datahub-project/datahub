import importlib.resources as pkg_resource
import logging
from typing import List, cast

import lark
from lark import Lark, Tree

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.m_query import resolver, validator
from datahub.ingestion.source.powerbi.proxy import PowerBiAPI

LOGGER = logging.getLogger(__name__)


def _parse_expression(expression: str) -> Tree:
    # Read lexical grammar as text
    grammar: str = pkg_resource.read_text(
        "datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule"
    )

    # Create lark parser for the grammar text
    lark_parser = Lark(grammar, start="let_expression", regex=True)

    parse_tree: Tree = lark_parser.parse(expression)

    LOGGER.debug("Parse Tree")
    if (
        LOGGER.level == logging.DEBUG
    ):  # Guard condition to avoid heavy pretty() function call
        LOGGER.debug(parse_tree.pretty())

    return parse_tree


def get_upstream_tables(
    table: PowerBiAPI.Table,
    reporter: PowerBiDashboardSourceReport,
    native_query_enabled: bool = True,
) -> List[resolver.DataPlatformTable]:
    if table.expression is None:
        reporter.report_warning(table.full_name, "Expression is none")
        return []

    try:
        parse_tree: Tree = _parse_expression(table.expression)
        valid, message = validator.validate_parse_tree(
            parse_tree, native_query_enabled=native_query_enabled
        )
        if valid is False:
            LOGGER.debug("Validation failed: %s", cast(str, message))
            reporter.report_warning(table.full_name, cast(str, message))
            return []
    except lark.exceptions.UnexpectedCharacters as e:
        LOGGER.debug(f"Fail to parse expression {table.expression}", exc_info=e)
        reporter.report_warning(
            table.full_name, f"UnSupported expression = {table.expression}"
        )
        return []

    return resolver.MQueryResolver(
        table=table,
        parse_tree=parse_tree,
        reporter=reporter,
    ).resolve_to_data_platform_table_list()  # type: ignore
