import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, cast, Tuple, Type, Any

from lark import Tree

from dataclasses import dataclass
from enum import Enum

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.proxy import PowerBiAPI

from datahub.ingestion.source.powerbi.m_query import tree_function

LOGGER = logging.getLogger(__name__)


@dataclass
class DataPlatformPair:
    datahub_data_platform_name: str
    powerbi_data_platform_name: str


@dataclass
class DataPlatformTable:
    name: str
    full_name: str
    data_platform_pair: DataPlatformPair


class AbstractMQueryResolver(ABC):
    pass


class AbstractDataAccessMQueryResolver(AbstractMQueryResolver, ABC):
    table: PowerBiAPI.Table
    parse_tree: Tree
    reporter: PowerBiDashboardSourceReport
    data_platform_pair: DataPlatformPair

    def __init__(
        self,
        table: PowerBiAPI.Table,
        parse_tree: Tree,
        data_platform_pair: DataPlatformPair,
        reporter: PowerBiDashboardSourceReport,
    ):
        self.table = table
        self.parse_tree = parse_tree
        self.reporter = reporter
        self.data_platform_pair = data_platform_pair

    @abstractmethod
    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        pass


class BaseMQueryResolver(AbstractDataAccessMQueryResolver, ABC):
    def get_item_selector_tokens(
        self, variable_statement: Tree
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        expression_tree: Optional[Tree] = tree_function.first_expression_func(variable_statement)
        if expression_tree is None:
            LOGGER.debug("Expression tree not found")
            LOGGER.debug(variable_statement.pretty())
            return None, None

        item_selector: Optional[Tree] = tree_function.first_item_selector_func(expression_tree)
        if item_selector is None:
            LOGGER.debug("Item Selector not found in tree")
            LOGGER.debug(variable_statement.pretty())
            return None, None

        identifier_tree: Optional[Tree] = tree_function.first_identifier_func(expression_tree)
        if identifier_tree is None:
            LOGGER.debug("Identifier not found in tree")
            LOGGER.debug(variable_statement.pretty())
            return None, None

        # remove whitespaces and quotes from token
        tokens: List[str] = tree_function.strip_char_from_list(
            tree_function.remove_whitespaces_from_list(tree_function.token_values(cast(Tree, item_selector))),
            '"',
        )
        identifier: List[str] = tree_function.token_values(
            cast(Tree, identifier_tree)
        )  # type :ignore
        # convert tokens to dict
        iterator = iter(tokens)
        # cast to satisfy lint
        return identifier[0], dict(zip(iterator, iterator))

    def get_argument_list(self, variable_statement: Tree) -> Optional[List[str]]:
        expression_tree: Optional[Tree] = tree_function.first_expression_func(variable_statement)
        if expression_tree is None:
            LOGGER.debug("First expression rule not found in input tree")
            return None

        argument_list: Optional[Tree] = tree_function.first_arg_list_func(expression_tree)
        if argument_list is None:
            LOGGER.debug("First argument-list rule not found in input tree")
            return None

        # remove whitespaces and quotes from token
        tokens: List[str] = tree_function.strip_char_from_list(
            tree_function.remove_whitespaces_from_list(tree_function.token_values(argument_list)), '"'
        )
        return tokens

    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        data_platform_tables: List[DataPlatformTable] = []
        # Look for output variable
        output_variable: Optional[str] = tree_function.get_output_variable(self.parse_tree)
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
                data_platform_pair=self.data_platform_pair
            ),
        ]

    @abstractmethod
    def get_full_table_name(self, output_variable: str) -> Optional[str]:
        pass


class DefaultTwoStepDataAccessSources(BaseMQueryResolver, ABC):
    """
    These are the DataSource for which PowerBI Desktop generates default M-Query of following pattern
        let
            Source = Sql.Database("localhost", "library"),
            dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]
        in
            dbo_book_issue
    """

    def get_full_table_name(self, output_variable: str) -> Optional[str]:
        variable_statement: Optional[Tree] = tree_function.get_variable_statement(
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
        variable_statement = tree_function.get_variable_statement(self.parse_tree, source)
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


class PostgresMQueryResolver(DefaultTwoStepDataAccessSources):
    pass


class MSSqlMQueryResolver(DefaultTwoStepDataAccessSources):
    pass


class OracleMQueryResolver(BaseMQueryResolver):

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
        variable_statement: Optional[Tree] = tree_function.get_variable_statement(
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
        variable_statement = tree_function.get_variable_statement(
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
        variable_statement = tree_function.get_variable_statement(self.parse_tree, source_variable)
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

    def get_full_table_name(self, output_variable: str) -> Optional[str]:
        # Find step for the output variable
        variable_statement: Optional[Tree] = tree_function.get_variable_statement(
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
        variable_statement = tree_function.get_variable_statement(self.parse_tree, schema_variable)
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
        variable_statement = tree_function.get_variable_statement(self.parse_tree, source_variable)
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


class SupportedDataPlatform(Enum):
    POSTGRES_SQL = (
        DataPlatformPair(
            powerbi_data_platform_name="PostgreSQL",
            datahub_data_platform_name="postgres"
        ),
        PostgresMQueryResolver
    )
    ORACLE = (
        DataPlatformPair(
            powerbi_data_platform_name="Oracle",
            datahub_data_platform_name="oracle"
        ),
        OracleMQueryResolver
    )
    SNOWFLAKE = (
        DataPlatformPair(
            powerbi_data_platform_name="Snowflake",
            datahub_data_platform_name="snowflake"
        ),
        SnowflakeMQueryResolver
    )
    MS_SQL = (
        DataPlatformPair(
            powerbi_data_platform_name="Sql",
            datahub_data_platform_name="mssql"
        ),
        MSSqlMQueryResolver
    )

    def get_data_platform_pair(self) -> DataPlatformPair:
        return self.value[0]

    def get_m_query_resolver(self) -> Type[BaseMQueryResolver]:
        return self.value[1]


def get_resolver(parse_tree: Tree) -> Optional[SupportedDataPlatform]:

    _filter: Any = parse_tree.find_data("invoke_expression")

    letter_tree: Tree = next(_filter).children[0]
    data_access_func: str = tree_function.make_function_name(letter_tree)

    LOGGER.debug(
        "Looking for data-access(%s) resolver",
        data_access_func,
    )

    # Take platform name from data_access_func variable
    platform_name: str = data_access_func.split(".")[0]
    for platform in SupportedDataPlatform:
        if platform.get_data_platform_pair().powerbi_data_platform_name == platform_name:
            return platform

    LOGGER.info("M-Query resolver not found for data access function %s", data_access_func)

    return None
