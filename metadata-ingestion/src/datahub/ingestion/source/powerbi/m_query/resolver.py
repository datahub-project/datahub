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


class FullTableNameCreator(ABC):
    @abstractmethod
    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        pass


class AbstractDataAccessMQueryResolver(ABC):
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
        self.specific_resolver = {}

    @abstractmethod
    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        pass


class BaseMQueryResolver(AbstractDataAccessMQueryResolver, ABC):
    @staticmethod
    def get_item_selector_tokens(
            expression_tree: Tree
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:

        item_selector: Optional[Tree] = tree_function.first_item_selector_func(expression_tree)
        if item_selector is None:
            LOGGER.debug("Item Selector not found in tree")
            LOGGER.debug(expression_tree.pretty())
            return None, None

        identifier_tree: Optional[Tree] = tree_function.first_identifier_func(expression_tree)
        if identifier_tree is None:
            LOGGER.debug("Identifier not found in tree")
            LOGGER.debug(item_selector.pretty())
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

    def get_argument_list(self, variable_statement: Tree) -> Optional[Tree]:
        expression_tree: Optional[Tree] = tree_function.first_expression_func(variable_statement)
        if expression_tree is None:
            LOGGER.debug("First expression rule not found in input tree")
            return None

        argument_list: Optional[Tree] = tree_function.first_arg_list_func(expression_tree)
        if argument_list is None:
            LOGGER.debug("First argument-list rule not found in input tree")
            return None

        return argument_list

    def make_token_dict(self, identifier: str) -> Dict[str, Any]:
        token_dict: Dict[str, Any] = {}

        def fill_token_dict(identifier: str, supported_data_access_func: List[str], t_dict: Dict[str, Any]) -> None:
            """
            1) Find statement where identifier appear in the left-hand side i.e. identifier  = expression
            2) Check expression is function invocation i.e. invoke_expression or item_selector
            3) if it is function invocation and this function is not the data-access function then take first argument
               i.e. identifier and call the function recursively
            4) if it is item_selector then take identifier and key-value pair,
               add identifier and key-value pair in current_selector and call the function recursively
            5) This recursion will continue till we reach to data-access function and during recursion we will fill
               token_dict dictionary for all item_selector we find during traversal.

            :param identifier: variable to look for
            :param supported_data_access_func: List of supported data-access functions
            :param t_dict: dict where key is identifier and value is key-value pair which represent item selected from
                           identifier
            :return: None
            """
            v_statement: Optional[Tree] = tree_function.get_variable_statement(
                self.parse_tree, identifier
            )
            if v_statement is None:
                self.reporter.report_warning(
                    f"{self.table.full_name}-variable-statement",
                    f"output variable ({identifier}) statement not found in table expression",
                )
                return None

            expression_tree: Optional[Tree] = tree_function.first_expression_func(v_statement)
            if expression_tree is None:
                LOGGER.debug("Expression tree not found")
                LOGGER.debug(v_statement.pretty())
                return None
            invoke_expression: Optional[Tree] = tree_function.first_invoke_expression_func(expression_tree)
            if invoke_expression is not None:
                letter_tree: Tree = invoke_expression.children[0]
                data_access_func: str = tree_function.make_function_name(letter_tree)
                if data_access_func in supported_data_access_func:
                    token_dict.update(
                        {
                            f"{data_access_func}": {
                                "arg_list": self.get_argument_list(expression_tree),
                                **t_dict,
                            }
                        }
                    )
                    return

                first_arg_tree: Optional[Tree] = tree_function.first_arg_list_func(invoke_expression)
                if first_arg_tree is None:
                    LOGGER.debug("Function invocation without argument in expression = %s", invoke_expression.pretty())
                    self.reporter.report_warning(
                        f"{self.table.full_name}-variable-statement",
                        f"Function invocation without argument",
                    )
                    return None
                type_expression: Optional[Tree] = tree_function.first_type_expression_func(first_arg_tree)
                if type_expression is None:
                    LOGGER.debug("Type expression not found in expression = %s", first_arg_tree.pretty())
                    self.reporter.report_warning(
                        f"{self.table.full_name}-variable-statement",
                        f"Type expression not found",
                    )
                    return None

                tokens: List[str] = tree_function.token_values(type_expression)
                if len(tokens) != 1:
                    LOGGER.debug("type-expression has more than one identifier = %s", type_expression.pretty())
                    self.reporter.report_warning(
                        f"{self.table.full_name}-variable-statement",
                        f"Unsupported type expression",
                    )
                    return None
                new_identifier: str = tokens[0]
                fill_token_dict(new_identifier, supported_data_access_func, t_dict)
            else:
                identifier, key_vs_value = self.get_item_selector_tokens(
                    tree_function.first_expression_func(expression_tree)
                )
                current_selector: Dict[str, Any] = {
                    f"{identifier}": {
                        "item_selectors": [key_vs_value],
                        **t_dict,
                    }
                }
                fill_token_dict(identifier, supported_data_access_func, current_selector)

        fill_token_dict(identifier, SupportedResolver.get_function_names(), {})

        return token_dict

    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        data_platform_tables: List[DataPlatformTable] = []

        output_variable: Optional[str] = tree_function.get_output_variable(self.parse_tree)
        if output_variable is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-output-variable",
                "output-variable not found in table expression",
            )
            return data_platform_tables

        token_dict: Dict[str, Any] = self.make_token_dict(output_variable)

        # each key is data-access function
        for data_access_func in token_dict.keys():
            supported_resolver = SupportedResolver.get_resolver(data_access_func)
            if supported_resolver is None:
                LOGGER.debug("Resolver not found for the data-access-function %s", data_access_func)
                self.reporter.report_warning(
                    f"{self.table.full_name}-data-access-function",
                    f"Resolver not found for data-access-function = {data_access_func}"
                )
                continue

            table_full_name_creator: FullTableNameCreator = supported_resolver.get_table_full_name_creator()()
            for table_full_name in table_full_name_creator.get_full_table_names(token_dict):
                data_platform_tables.append(
                    DataPlatformTable(
                        name=table_full_name.split(".")[-1],
                        full_name=table_full_name,
                        data_platform_pair=supported_resolver.get_data_platform_pair()
                    )
                )

        return data_platform_tables


class DefaultTwoStepDataAccessSources(FullTableNameCreator):
    """
    These are the DataSource for which PowerBI Desktop generates default M-Query of following pattern
        let
            Source = Sql.Database("localhost", "library"),
            dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]
        in
            dbo_book_issue
    """

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
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


class PostgresFullTableNameCreator(DefaultTwoStepDataAccessSources):
    pass


class MSSqlFullTableNameCreator(DefaultTwoStepDataAccessSources):
    pass


class OracleFullTableNameCreator(FullTableNameCreator):

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

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
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


class SnowflakeFullTableNameCreator(FullTableNameCreator):

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
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


class NativeQueryFullTableNameCreator(FullTableNameCreator):

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        pass


class FunctionName(Enum):
    NATIVE_QUERY = "Value.NativeQuery"
    POSTGRESQL_DATA_ACCESS = "PostgreSQL.Database"
    ORACLE_DATA_ACCESS = "Oracle.Database"
    SNOWFLAKE_DATA_ACCESS = "Snowflake.Databases"
    MSSQL_DATA_ACCESS = "Sql.Database"


class SupportedResolver(Enum):
    POSTGRES_SQL = (
        DataPlatformPair(
            powerbi_data_platform_name="PostgreSQL",
            datahub_data_platform_name="postgres"
        ),
        PostgresFullTableNameCreator,
        FunctionName.POSTGRESQL_DATA_ACCESS,
    )

    ORACLE = (
        DataPlatformPair(
            powerbi_data_platform_name="Oracle",
            datahub_data_platform_name="oracle"
        ),
        OracleFullTableNameCreator,
        FunctionName.ORACLE_DATA_ACCESS,
    )

    SNOWFLAKE = (
        DataPlatformPair(
            powerbi_data_platform_name="Snowflake",
            datahub_data_platform_name="snowflake"
        ),
        SnowflakeFullTableNameCreator,
        FunctionName.SNOWFLAKE_DATA_ACCESS,
    )

    MS_SQL = (
        DataPlatformPair(
            powerbi_data_platform_name="Sql",
            datahub_data_platform_name="mssql"
        ),
        MSSqlFullTableNameCreator,
        FunctionName.MSSQL_DATA_ACCESS,
    )

    NATIVE_QUERY = (
        None,
        NativeQueryFullTableNameCreator,
        FunctionName.NATIVE_QUERY,
    )

    def get_data_platform_pair(self) -> DataPlatformPair:
        return self.value[0]

    def get_table_full_name_creator(self) -> Type[FullTableNameCreator]:
        return self.value[1]

    def get_function_name(self) -> str:
        return self.value[2].value

    @staticmethod
    def get_function_names() -> List[str]:
        functions: List[str] = []
        for supported_resolver in SupportedResolver:
            functions.append(
                supported_resolver.get_function_name()
            )

        return functions

    @staticmethod
    def get_resolver(function_name: str) -> Optional["SupportedResolver"]:
        for supported_resolver in SupportedResolver:
            if function_name == supported_resolver.get_function_name():
                return supported_resolver

        return None
