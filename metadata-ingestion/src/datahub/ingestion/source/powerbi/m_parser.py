import importlib.resources as pkg_resource
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast

import lark
from lark import Lark, Token, Tree

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.proxy import PowerBiAPI

LOGGER = logging.getLogger(__name__)


@dataclass
class DataPlatformTable:
    name: str
    full_name: str
    platform_type: str


class SupportedDataPlatform(Enum):
    POSTGRES_SQL = "PostgreSQL"
    ORACLE = "Oracle"
    SNOWFLAKE = "Snowflake"


POWERBI_TO_DATAHUB_DATA_PLATFORM_MAPPING: Dict[str, str] = {
    SupportedDataPlatform.POSTGRES_SQL.value: "postgres",
    SupportedDataPlatform.ORACLE.value: "oracle",
    SupportedDataPlatform.SNOWFLAKE.value: "snowflake",
}


def _get_output_variable(root: Tree) -> Optional[str]:
    in_expression_tree: Optional[Tree] = _get_first_rule(root, "in_expression")
    if in_expression_tree is None:
        return None
    # Get list of terminal value
    # Remove any whitespaces
    # Remove any spaces
    return "".join(
        _strip_char_from_list(
            _remove_whitespaces_from_list(_token_values(in_expression_tree)), " "
        )
    )


def _get_variable_statement(parse_tree: Tree, variable: str) -> Optional[Tree]:
    _filter = parse_tree.find_data("variable")
    # filter will return statement of the form <variable-name> = <expression>
    # We are searching for Tree where variable-name is matching with provided variable
    for tree in _filter:
        values: List[str] = _token_values(tree.children[0])
        actual_value: str = "".join(_strip_char_from_list(values, " "))
        LOGGER.debug("Actual Value = %s", actual_value)
        LOGGER.debug("Expected Value = %s", variable)

        if actual_value == variable:
            return tree

    LOGGER.info("Provided variable(%s) not found in variable rule", variable)

    return None


def _get_first_rule(tree: Tree, rule: str) -> Optional[Tree]:
    """
    Lark library doesn't have advance search function.
    This function will return the first tree of provided rule
    :param tree: Tree to search for the expression rule
    :return: Tree
    """

    def internal(node: Union[Tree, Token]) -> Optional[Tree]:
        if isinstance(node, Tree) and node.data == rule:
            return node
        if isinstance(node, Token):
            return None

        for child in cast(Tree, node).children:
            child_node: Optional[Tree] = internal(child)
            if child_node is not None:
                return child_node

        return None

    expression_tree: Optional[Tree] = internal(tree)

    return expression_tree


def _token_values(tree: Tree) -> List[str]:
    """

    :param tree: Tree to traverse
    :return: List of leaf token data
    """
    values: List[str] = []

    def internal(node: Union[Tree, Token]) -> None:
        if isinstance(node, Token):
            values.append(cast(Token, node).value)
            return

        for child in node.children:
            internal(child)

    internal(tree)

    return values


def _remove_whitespaces_from_list(values: List[str]) -> List[str]:
    result: List[str] = []
    for item in values:
        if item.strip() not in ("", "\n", "\t"):
            result.append(item)

    return result


def _strip_char_from_list(values: List[str], char: str) -> List[str]:
    result: List[str] = []
    for item in values:
        result.append(item.strip(char))

    return result


def _make_function_name(tree: Tree) -> str:
    values: List[str] = _token_values(tree)
    return ".".join(values)


class AbstractMQueryResolver(ABC):
    pass


class AbstractDataAccessMQueryResolver(AbstractMQueryResolver, ABC):
    table: PowerBiAPI.Table
    parse_tree: Tree
    reporter: PowerBiDashboardSourceReport

    def __init__(
        self,
        table: PowerBiAPI.Table,
        parse_tree: Tree,
        reporter: PowerBiDashboardSourceReport,
    ):
        self.table = table
        self.parse_tree = parse_tree
        self.reporter = reporter
        self.first_expression_func = partial(_get_first_rule, rule="expression")
        self.first_item_selector_func = partial(_get_first_rule, rule="item_selector")
        self.first_arg_list_func = partial(_get_first_rule, rule="argument_list")
        self.first_identifier_func = partial(_get_first_rule, rule="identifier")

    @abstractmethod
    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        pass


class BaseMQueryResolver(AbstractDataAccessMQueryResolver, ABC):
    def get_item_selector_tokens(
        self, variable_statement: Tree
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        expression_tree: Optional[Tree] = self.first_expression_func(variable_statement)
        if expression_tree is None:
            LOGGER.debug("Expression tree not found")
            LOGGER.debug(variable_statement.pretty())
            return None, None

        item_selector: Optional[Tree] = self.first_item_selector_func(expression_tree)
        if item_selector is None:
            LOGGER.debug("Item Selector not found in tree")
            LOGGER.debug(variable_statement.pretty())
            return None, None

        identifier_tree: Optional[Tree] = self.first_identifier_func(expression_tree)
        if identifier_tree is None:
            LOGGER.debug("Identifier not found in tree")
            LOGGER.debug(variable_statement.pretty())
            return None, None

        # remove whitespaces and quotes from token
        tokens: List[str] = _strip_char_from_list(
            _remove_whitespaces_from_list(_token_values(cast(Tree, item_selector))),
            '"',
        )
        identifier: List[str] = _token_values(
            cast(Tree, identifier_tree)
        )  # type :ignore
        # convert tokens to dict
        iterator = iter(tokens)
        # cast to satisfy lint
        return identifier[0], dict(zip(iterator, iterator))

    def get_argument_list(self, variable_statement: Tree) -> Optional[List[str]]:
        expression_tree: Optional[Tree] = self.first_expression_func(variable_statement)
        if expression_tree is None:
            LOGGER.debug("First expression rule not found in input tree")
            return None

        argument_list: Optional[Tree] = self.first_arg_list_func(expression_tree)
        if argument_list is None:
            LOGGER.debug("First argument-list rule not found in input tree")
            return None

        # remove whitespaces and quotes from token
        tokens: List[str] = _strip_char_from_list(
            _remove_whitespaces_from_list(_token_values(argument_list)), '"'
        )
        return tokens

    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        data_platform_tables: List[DataPlatformTable] = []
        # Look for output variable
        output_variable: Optional[str] = _get_output_variable(self.parse_tree)
        if output_variable is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-output-variable",
                "output-variable not found in table expression",
            )
            return data_platform_tables

        full_table_name: Optional[str] = self.get_full_table_name(output_variable)
        if full_table_name is None:
            LOGGER.debug(
                "Fail to form full_table_name for PowerBI DataSet table %s",
                self.table.full_name,
            )
            return data_platform_tables

        return [
            DataPlatformTable(
                name=full_table_name.split(".")[-1],
                full_name=full_table_name,
                platform_type=self.get_platform(),
            ),
        ]

    @abstractmethod
    def get_platform(self) -> str:
        pass

    @abstractmethod
    def get_full_table_name(self, output_variable: str) -> Optional[str]:
        pass


class PostgresMQueryResolver(BaseMQueryResolver):
    def get_full_table_name(self, output_variable: str) -> Optional[str]:
        variable_statement: Optional[Tree] = _get_variable_statement(
            self.parse_tree, output_variable
        )
        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                f"output variable ({output_variable}) statement not found in table expression",
            )
            return None
        source, tokens = self.get_item_selector_tokens(cast(Tree, variable_statement))
        if source is None or tokens is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "Schema detail not found in table expression",
            )
            return None

        schema_name: str = tokens["Schema"]
        table_name: str = tokens["Item"]
        # Look for database-name
        variable_statement = _get_variable_statement(self.parse_tree, source)
        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-source-statement",
                f"source variable {source} statement not found in table expression",
            )
            return None
        arg_list = self.get_argument_list(cast(Tree, variable_statement))
        if arg_list is None or len(arg_list) < 1:
            self.reporter.report_warning(
                f"{self.table.full_name}-database-arg-list",
                "Expected number of argument not found in data-access function of table expression",
            )
            return None

        database_name: str = cast(List[str], arg_list)[1]  # 1st token is database name
        return cast(Optional[str], f"{database_name}.{schema_name}.{table_name}")

    def get_platform(self) -> str:
        return SupportedDataPlatform.POSTGRES_SQL.value


class OracleMQueryResolver(BaseMQueryResolver):
    def get_platform(self) -> str:
        return SupportedDataPlatform.ORACLE.value

    def _get_db_name(self, value: str) -> Optional[str]:
        error_message: str = f"The target argument ({value}) should in the format of <host-name>:<port>/<db-name>[.<domain>]"
        splitter_result: List[str] = value.split("/")
        if len(splitter_result) != 2:
            self.reporter.report_warning(
                f"{self.table.full_name}-oracle-target", error_message
            )
            return None

        db_name = splitter_result[1].split(".")[0]

        return db_name

    def get_full_table_name(self, output_variable: str) -> Optional[str]:
        # Find step for the output variable
        variable_statement: Optional[Tree] = _get_variable_statement(
            self.parse_tree, output_variable
        )

        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                f"output variable ({output_variable}) statement not found in table expression",
            )
            return None

        schema_variable, tokens = self.get_item_selector_tokens(
            cast(Tree, variable_statement)
        )
        if schema_variable is None or tokens is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "table name not found in table expression",
            )
            return None

        table_name: str = tokens["Name"]

        # Find step for the schema variable
        variable_statement = _get_variable_statement(
            self.parse_tree, cast(str, schema_variable)
        )
        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-schema-variable-statement",
                f"schema variable ({schema_variable}) statement not found in table expression",
            )
            return None

        source_variable, tokens = self.get_item_selector_tokens(variable_statement)
        if source_variable is None or tokens is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "Schema not found in table expression",
            )
            return None

        schema_name: str = tokens["Schema"]

        # Find step for the database access variable
        variable_statement = _get_variable_statement(self.parse_tree, source_variable)
        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-source-variable-statement",
                f"schema variable ({source_variable}) statement not found in table expression",
            )
            return None
        arg_list = self.get_argument_list(variable_statement)
        if arg_list is None or len(arg_list) < 1:
            self.reporter.report_warning(
                f"{self.table.full_name}-database-arg-list",
                "Expected number of argument not found in data-access function of table expression",
            )
            return None
        # The first argument has database name. format localhost:1521/salesdb.GSLAB.COM
        db_name: Optional[str] = self._get_db_name(arg_list[0])
        if db_name is None:
            LOGGER.debug(f"Fail to extract db name from the target {arg_list}")

        return f"{db_name}.{schema_name}.{table_name}"


class SnowflakeMQueryResolver(BaseMQueryResolver):
    def get_platform(self) -> str:
        return SupportedDataPlatform.SNOWFLAKE.value

    def get_full_table_name(self, output_variable: str) -> Optional[str]:
        # Find step for the output variable
        variable_statement: Optional[Tree] = _get_variable_statement(
            self.parse_tree, output_variable
        )

        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                f"output variable ({output_variable}) statement not found in table expression",
            )
            return None

        schema_variable, tokens = self.get_item_selector_tokens(variable_statement)
        if schema_variable is None or tokens is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "table name not found in table expression",
            )
            return None

        table_name: str = tokens["Name"]

        # Find step for the schema variable
        variable_statement = _get_variable_statement(self.parse_tree, schema_variable)
        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-schema-variable-statement",
                f"schema variable ({schema_variable}) statement not found in table expression",
            )
            return None

        source_variable, tokens = self.get_item_selector_tokens(variable_statement)
        if source_variable is None or tokens is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "schema name not found in table expression",
            )
            return None

        schema_name: str = tokens["Name"]

        # Find step for the database access variable
        variable_statement = _get_variable_statement(self.parse_tree, source_variable)
        if variable_statement is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-source-variable-statement",
                f"schema variable ({source_variable}) statement not found in table expression",
            )
            return None
        _, tokens = self.get_item_selector_tokens(variable_statement)
        if tokens is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "database name not found in table expression",
            )
            return None

        db_name: str = tokens["Name"]

        return f"{db_name}.{schema_name}.{table_name}"


def _get_resolver(parse_tree: Tree) -> Optional[Type["BaseMQueryResolver"]]:

    _filter: Any = parse_tree.find_data("invoke_expression")

    letter_tree: Tree = next(_filter).children[0]
    data_access_func: str = _make_function_name(letter_tree)

    LOGGER.debug(
        "Looking for data-access(%s) resolver in data-access-function registry %s",
        data_access_func,
        DATA_ACCESS_RESOLVER,
    )

    if DATA_ACCESS_RESOLVER.get(data_access_func) is None:
        LOGGER.info("Resolver not found for %s", data_access_func)
        return None

    return DATA_ACCESS_RESOLVER[data_access_func]


# Register M-Query resolver for specific database platform
DATA_ACCESS_RESOLVER = {
    f"{SupportedDataPlatform.POSTGRES_SQL.value}.Database": PostgresMQueryResolver,
    f"{SupportedDataPlatform.ORACLE.value}.Database": OracleMQueryResolver,
    f"{SupportedDataPlatform.SNOWFLAKE.value}.Databases": SnowflakeMQueryResolver,
}  # type :ignore


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
) -> List[DataPlatformTable]:
    if table.expression is None:
        reporter.report_warning(table.full_name, "Expression is none")
        return []

    try:
        parse_tree: Tree = _parse_expression(table.expression)
    except lark.exceptions.UnexpectedCharacters:
        reporter.report_warning(
            table.full_name, f"UnSupported expression = {table.expression}"
        )
        return []

    trees: List[Tree] = list(parse_tree.find_data("invoke_expression"))
    if len(trees) > 1:
        reporter.report_warning(
            table.full_name, f"{table.full_name} has more than one invoke expression"
        )
        return []

    resolver: Optional[Type[BaseMQueryResolver]] = _get_resolver(parse_tree)
    if resolver is None:
        LOGGER.debug("Table full-name = %s", table.full_name)
        LOGGER.debug("Expression = %s", table.expression)
        reporter.report_warning(
            table.full_name,
            f"{table.full_name} M-Query resolver not found for the table expression",
        )
        return []

    return resolver(
        table, parse_tree, reporter
    ).resolve_to_data_platform_table_list()  # type: ignore
