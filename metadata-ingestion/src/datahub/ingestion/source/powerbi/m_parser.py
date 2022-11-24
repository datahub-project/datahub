import importlib.resources as pkg_resource
import logging
from typing import List, Optional

from lark import Lark, Tree

logger = logging.getLogger(__name__)


def get_output_dataset(root: Tree) -> Optional[str]:
    def get_token_list_for_any(tree: Tree, rules: List[str]) -> List[Tree]:
        for rule in rules:
            token_list = [x for x in tree.find_data(rule)]
            if len(token_list) > 0:
                return token_list

        return []

    for tree in root.find_data("in_expression"):
        for child1 in get_token_list_for_any(
            tree, ["letter_character", "quoted_identifier"]
        ):
            return child1.children[0].value  # type: ignore

    return None


def parse_expression(expression: str) -> Tree:
    grammar: str = pkg_resource.read_text(
        "datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule"
    )
    lark_parser = Lark(grammar, start="let_expression", regex=True)

    parse_tree: Tree = lark_parser.parse(expression)

    logger.debug("Parse Tree")
    logger.debug(parse_tree.pretty())

    return parse_tree
