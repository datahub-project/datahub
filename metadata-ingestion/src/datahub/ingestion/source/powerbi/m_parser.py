import importlib.resources as pkg_resource
import logging

from lark import Lark, Tree

logger = logging.getLogger(__name__)


def parse_expression(expression: str) -> Tree:
    grammar: str = pkg_resource.read_text(
        "datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule"
    )
    lark_parser = Lark(grammar, start="let_expression", regex=True)

    parse_tree: Tree = lark_parser.parse(expression)

    logger.debug("Parse Tree")
    logger.debug(parse_tree.pretty())

    return parse_tree
