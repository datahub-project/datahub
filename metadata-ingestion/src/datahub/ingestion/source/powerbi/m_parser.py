from abc import ABC

from dataclasses import  dataclass
import importlib.resources as pkg_resource
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.proxy import PowerBiAPI
import logging
from typing import List, Optional, Any, Dict

from lark import Lark, Tree, Token

logger = logging.getLogger(__name__)


@dataclass
class DataPlatformTable:
    name: str
    full_name: str
    platform_type: str


class AbstractMQueryResolver(ABC):
    pass


class AbstractDataAccessMQueryResolver(AbstractMQueryResolver, ABC):
    pass


class PostgresMQueryResolver(AbstractDataAccessMQueryResolver):
    pass


class OracleMQueryResolver(AbstractDataAccessMQueryResolver):
    pass


class SnowflakeMQueryResolver(AbstractDataAccessMQueryResolver):
    pass


class AbstractTableAccessMQueryResolver(AbstractDataAccessMQueryResolver, ABC):
    pass


class TableCombineMQueryResolver(AbstractTableAccessMQueryResolver):
    pass


DATA_ACCESS_RESOLVER: Dict[str, AbstractMQueryResolver.__class__] = {
    "PostgreSQL.Database": PostgresMQueryResolver,
    "Oracle.Database": OracleMQueryResolver,
    "Snowflake.Database": SnowflakeMQueryResolver,
}

TABLE_ACCESS_RESOLVER: Dict[str, AbstractMQueryResolver.__class__] = {
    "Table.Combine": TableCombineMQueryResolver,
}


def get_output_variable(root: Tree) -> Optional[str]:
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
    # Read lexical grammar as text
    grammar: str = pkg_resource.read_text(
        "datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule"
    )

    # Create lark parser for the grammar text
    lark_parser = Lark(grammar, start="let_expression", regex=True)

    parse_tree: Tree = lark_parser.parse(expression)

    logger.debug("Parse Tree")
    if logger.level == logging.DEBUG:  # Guard condition to avoid heavy pretty() function call
        logger.debug(parse_tree.pretty())

    return parse_tree


def get_upstream_tables(table: PowerBiAPI.Table, reporter: PowerBiDashboardSourceReport) -> List[DataPlatformTable]:
    parse_tree = parse_expression(table.expression)

    output_variable = get_output_variable(parse_tree)

    filter: Any = parse_tree.find_data("invoke_expression")
    tokens: List[Any] = list(filter)
    print("Length = {}".format(len(tokens)))
    for tree in tokens:
        print(tree.pretty())

    # filter: Any = parse_tree.find_data("variable")
    # def find_variable(node: Tree, variable: str) -> bool:
    #     for internal_child in node.children:
    #         if isinstance(internal_child, Token):
    #             if internal_child.value == variable:
    #                 return True
    #             continue
    #         return find_variable(internal_child, variable)
    #
    #     return False
    #
    # for tree in filter:
    #     if find_variable(tree, output_variable):
    #         print("Mohd1")
    #         print(tree.pretty())
    #         for node in tree.find_data("field_selection"):
    #             print("Mohd2")
    #             print(node)

    return [
        DataPlatformTable(
            name="postgres_table",
            full_name="book.public.test",
            platform_type="PostgreSql"
        ),
        DataPlatformTable(
            name="oracle_table",
            full_name="book.public.test",
            platform_type="Oracle"
        ),
        DataPlatformTable(
            name="snowflake_table",
            full_name="book.public.test",
            platform_type="Snowflake"
        ),
    ]
