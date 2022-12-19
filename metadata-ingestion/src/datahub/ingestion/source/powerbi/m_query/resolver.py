import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, cast, Tuple, Type, Any

from lark import Tree

from dataclasses import dataclass
from enum import Enum

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.proxy import PowerBiAPI

from datahub.ingestion.source.powerbi.m_query import tree_function, native_sql_parser

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


class SupportedDataPlatform(Enum):
    POSTGRES_SQL = DataPlatformPair(
            powerbi_data_platform_name="PostgreSQL",
            datahub_data_platform_name="postgres"
        )

    ORACLE = DataPlatformPair(
            powerbi_data_platform_name="Oracle",
            datahub_data_platform_name="oracle"
        )

    SNOWFLAKE = DataPlatformPair(
            powerbi_data_platform_name="Snowflake",
            datahub_data_platform_name="snowflake"
        )

    MS_SQL = DataPlatformPair(
            powerbi_data_platform_name="Sql",
            datahub_data_platform_name="mssql"
        )


class AbstractTableFullNameCreator(ABC):
    @abstractmethod
    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        pass

    @abstractmethod
    def get_platform_pair(self) -> DataPlatformPair:
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
                new_identifier, key_vs_value = self.get_item_selector_tokens(
                    tree_function.first_expression_func(expression_tree)
                )
                current_selector: Dict[str, Any] = {
                    f"{new_identifier}": {
                        "item_selectors": [
                            {
                                "items": key_vs_value,
                                "assigned_to": identifier
                            }
                        ],
                        **t_dict,
                    }
                }
                fill_token_dict(new_identifier, supported_data_access_func, current_selector)

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

            table_full_name_creator: AbstractTableFullNameCreator = supported_resolver.get_table_full_name_creator()()
            for table_full_name in table_full_name_creator.get_full_table_names(token_dict):
                data_platform_tables.append(
                    DataPlatformTable(
                        name=table_full_name.split(".")[-1],
                        full_name=table_full_name,
                        data_platform_pair=table_full_name_creator.get_platform_pair()
                    )
                )

        return data_platform_tables


class DefaultTwoStepDataAccessSources(AbstractTableFullNameCreator, ABC):
    """
    These are the DataSource for which PowerBI Desktop generates default M-Query of following pattern
        let
            Source = Sql.Database("localhost", "library"),
            dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]
        in
            dbo_book_issue
    """

    def two_level_access_pattern(self, token_dict: Dict[str, Any]) -> List[str]:
        full_table_names: List[str] = []

        LOGGER.debug("Processing PostgreSQL token-dict %s", token_dict)

        for data_access_function in token_dict:
            arguments: List[str] = tree_function.strip_char_from_list(
                values=tree_function.remove_whitespaces_from_list(
                            tree_function.token_values(token_dict[data_access_function]["arg_list"])
                        ),
                char="\""
            )
            # delete arg_list as we consumed it and don't want to process it in next step
            if len(arguments) != 2:
                LOGGER.debug("Expected 2 arguments, but got {%s}", len(arguments))
                return full_table_names

            del token_dict[data_access_function]["arg_list"]

            db_name: str = arguments[1]
            for source in token_dict[data_access_function]:
                source_dict: Dict[str, Any] = token_dict[data_access_function][source]
                for schema in source_dict["item_selectors"]:
                    schema_name: str = schema["items"]["Schema"]
                    table_name: str = schema["items"]["Item"]
                    full_table_names.append(
                        f"{db_name}.{schema_name}.{table_name}"
                    )

        LOGGER.debug("PostgreSQL full-table-names = %s", full_table_names)

        return full_table_names


class PostgresTableFullNameCreator(DefaultTwoStepDataAccessSources):
    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        return self.two_level_access_pattern(token_dict)

    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.POSTGRES_SQL.value


class MSSqlTableFullNameCreator(DefaultTwoStepDataAccessSources):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.MS_SQL.value

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        full_table_names: List[str] = []
        data_access_dict: Dict[str, Any] = list(token_dict.values())[0]

        arguments: List[str] = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                        tree_function.token_values(data_access_dict["arg_list"])
                    ),
            char="\""
        )

        if len(arguments) == 2:
            # It is regular case of MS-SQL
            LOGGER.debug("Handling with regular case")
            return self.two_level_access_pattern(token_dict)

        if len(arguments) >= 4 and arguments[2] != "Query":
            LOGGER.debug("Unsupported case is found. Second index is not the Query")
            return full_table_names

        db_name: str = arguments[1]
        tables: List[str] = native_sql_parser.get_tables(arguments[3])
        for table in tables:
            schema_and_table: List[str] = table.split(".")
            if len(schema_and_table) == 1:
                # schema name is not present. Default schema name in MS-SQL is dbo
                # https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/ownership-and-user-schema-separation?view=sql-server-ver16
                schema_and_table.insert(0, "dbo")

            full_table_names.append(
                f"{db_name}.{schema_and_table[0]}.{schema_and_table[1]}"
            )
        LOGGER.debug("MS-SQL full-table-names %s", full_table_names)

        return full_table_names


class OracleTableFullNameCreator(AbstractTableFullNameCreator):
    def get_platform_pair(self) -> DataPlatformPair:
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

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        full_table_names: List[str] = []

        LOGGER.debug("Processing Oracle token-dict %s", token_dict)

        for data_access_function in token_dict:
            arguments: List[str] = tree_function.remove_whitespaces_from_list(
                tree_function.token_values(token_dict[data_access_function]["arg_list"]))
            # delete arg_list as we consumed it and don't want to process it in next step
            del token_dict[data_access_function]["arg_list"]

            for source in token_dict[data_access_function]:
                source_dict: Dict[str, Any] = token_dict[data_access_function][source]

                db_name: Optional[str] = self._get_db_name(arguments[0])
                if db_name is None:
                    return full_table_names

                for schema in source_dict["item_selectors"]:
                    schema_name: str = schema["items"]["Schema"]
                    for item_selectors in source_dict[schema["assigned_to"]]:
                        for item_selector in source_dict[schema["assigned_to"]][item_selectors]:
                            table_name: str = item_selector["items"]["Name"]
                            full_table_names.append(
                                f"{db_name}.{schema_name}.{table_name}"
                            )

        return full_table_names


class SnowflakeTableFullNameCreator(AbstractTableFullNameCreator):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.SNOWFLAKE.value

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        full_table_names: List[str] = []

        LOGGER.debug("Processing Snowflake token-dict %s", token_dict)

        data_access_dict: Dict[str, Any] = list(token_dict.values())[0]
        del data_access_dict["arg_list"]

        for source in data_access_dict:
            for db_its in data_access_dict[source]["item_selectors"]:
                db_name: str = db_its["items"]["Name"]
                for schema_its in data_access_dict[source][db_its["assigned_to"]]["item_selectors"]:
                    schema_name: str = schema_its["items"]["Name"]
                    for table_its in data_access_dict[source][db_its["assigned_to"]][schema_its["assigned_to"]]["item_selectors"]:
                        table_name: str = table_its["items"]["Name"]
                        full_table_names.append(
                            f"{db_name}.{schema_name}.{table_name}"
                        )

        LOGGER.debug("Snowflake full-table-name %s", full_table_names)

        return full_table_names


class NativeQueryTableFullNameCreator(AbstractTableFullNameCreator):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.POSTGRES_SQL.value

    def get_full_table_names(self, token_dict: Dict[str, Any]) -> List[str]:
        print("===NATIVE========")
        for source in token_dict:
            print(tree_function.token_values(token_dict[source]["arg_list"]))
        return []


class FunctionName(Enum):
    NATIVE_QUERY = "Value.NativeQuery"
    POSTGRESQL_DATA_ACCESS = "PostgreSQL.Database"
    ORACLE_DATA_ACCESS = "Oracle.Database"
    SNOWFLAKE_DATA_ACCESS = "Snowflake.Databases"
    MSSQL_DATA_ACCESS = "Sql.Database"


class SupportedResolver(Enum):
    POSTGRES_SQL = (
        PostgresTableFullNameCreator,
        FunctionName.POSTGRESQL_DATA_ACCESS,
    )

    ORACLE = (
        OracleTableFullNameCreator,
        FunctionName.ORACLE_DATA_ACCESS,
    )

    SNOWFLAKE = (
        SnowflakeTableFullNameCreator,
        FunctionName.SNOWFLAKE_DATA_ACCESS,
    )

    MS_SQL = (
        MSSqlTableFullNameCreator,
        FunctionName.MSSQL_DATA_ACCESS,
    )

    NATIVE_QUERY = (
        NativeQueryTableFullNameCreator,
        FunctionName.NATIVE_QUERY,
    )

    def get_table_full_name_creator(self) -> Type[AbstractTableFullNameCreator]:
        return self.value[0]

    def get_function_name(self) -> str:
        return self.value[1].value

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
        LOGGER.debug("Looking for resolver %s", function_name)
        for supported_resolver in SupportedResolver:
            if function_name == supported_resolver.get_function_name():
                return supported_resolver
        LOGGER.debug("Looking not found for resolver %s", function_name)
        return None
