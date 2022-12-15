import importlib.resources as pkg_resource
import logging
from typing import List, Optional

import lark
from lark import Lark, Tree

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.proxy import PowerBiAPI
from datahub.ingestion.source.powerbi.m_query import validator
from datahub.ingestion.source.powerbi.m_query import resolver

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
    table: PowerBiAPI.Table, reporter: PowerBiDashboardSourceReport
) -> List[resolver.DataPlatformTable]:
    if table.expression is None:
        reporter.report_warning(table.full_name, "Expression is none")
        return []

    try:
        parse_tree: Tree = _parse_expression(table.expression)
    except lark.exceptions.UnexpectedCharacters as e:
        LOGGER.debug(f"Fail to parse expression {table.expression}", exc_info=e)
        reporter.report_warning(
            table.full_name, f"UnSupported expression = {table.expression}"
        )
        return []

    resolver_enum: Optional[resolver.SupportedDataPlatform] = resolver.get_resolver(parse_tree)
    if resolver_enum is None:
        LOGGER.debug("Table full-name = %s", table.full_name)
        LOGGER.debug("Expression = %s", table.expression)
        reporter.report_warning(
            table.full_name,
            f"{table.full_name} M-Query resolver not found for the table expression",
        )
        return []

    return resolver_enum.get_m_query_resolver()(
        table=table,
        parse_tree=parse_tree,
        data_platform_pair=resolver_enum.get_data_platform_pair(),
        reporter=reporter,
    ).resolve_to_data_platform_table_list()  # type: ignore
