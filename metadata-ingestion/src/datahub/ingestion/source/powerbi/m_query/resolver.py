import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast

from lark import Tree

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.m_query import native_sql_parser, tree_function
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataAccessFunctionDetail,
    IdentifierAccessor,
)
from datahub.ingestion.source.powerbi.proxy import PowerBiAPI

logger = logging.getLogger(__name__)


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
        powerbi_data_platform_name="PostgreSQL", datahub_data_platform_name="postgres"
    )

    ORACLE = DataPlatformPair(
        powerbi_data_platform_name="Oracle", datahub_data_platform_name="oracle"
    )

    SNOWFLAKE = DataPlatformPair(
        powerbi_data_platform_name="Snowflake", datahub_data_platform_name="snowflake"
    )

    MS_SQL = DataPlatformPair(
        powerbi_data_platform_name="Sql", datahub_data_platform_name="mssql"
    )


class AbstractTableFullNameCreator(ABC):
    @abstractmethod
    def get_full_table_names(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> List[str]:
        pass

    @abstractmethod
    def get_platform_pair(self) -> DataPlatformPair:
        pass


class AbstractDataAccessMQueryResolver(ABC):
    table: PowerBiAPI.Table
    parse_tree: Tree
    reporter: PowerBiDashboardSourceReport
    data_access_functions: List[str]

    def __init__(
        self,
        table: PowerBiAPI.Table,
        parse_tree: Tree,
        reporter: PowerBiDashboardSourceReport,
    ):
        self.table = table
        self.parse_tree = parse_tree
        self.reporter = reporter
        self.data_access_functions = SupportedResolver.get_function_names()

    @abstractmethod
    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        pass


class MQueryResolver(AbstractDataAccessMQueryResolver, ABC):
    @staticmethod
    def get_item_selector_tokens(
        expression_tree: Tree,
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:

        item_selector: Optional[Tree] = tree_function.first_item_selector_func(
            expression_tree
        )
        if item_selector is None:
            logger.debug("Item Selector not found in tree")
            logger.debug(expression_tree.pretty())
            return None, None

        identifier_tree: Optional[Tree] = tree_function.first_identifier_func(
            expression_tree
        )
        if identifier_tree is None:
            logger.debug("Identifier not found in tree")
            logger.debug(item_selector.pretty())
            return None, None

        # remove whitespaces and quotes from token
        tokens: List[str] = tree_function.strip_char_from_list(
            tree_function.remove_whitespaces_from_list(
                tree_function.token_values(cast(Tree, item_selector))
            ),
            '"',
        )
        identifier: List[str] = tree_function.token_values(
            cast(Tree, identifier_tree)
        )  # type :ignore

        # convert tokens to dict
        iterator = iter(tokens)

        return "".join(identifier), dict(zip(iterator, iterator))

    @staticmethod
    def get_argument_list(invoke_expression: Tree) -> Optional[Tree]:
        argument_list: Optional[Tree] = tree_function.first_arg_list_func(
            invoke_expression
        )
        if argument_list is None:
            logger.debug("First argument-list rule not found in input tree")
            return None

        return argument_list

    def _process_invoke_expression(
        self, invoke_expression: Tree
    ) -> Union[DataAccessFunctionDetail, List[str], None]:

        letter_tree: Tree = invoke_expression.children[0]
        data_access_func: str = tree_function.make_function_name(letter_tree)
        # The invoke function is either DataAccess function like PostgreSQL.Database(<argument-list>) or
        # some other function like Table.AddColumn or Table.Combine and so on
        if data_access_func in self.data_access_functions:
            arg_list: Optional[Tree] = MQueryResolver.get_argument_list(
                invoke_expression
            )
            if arg_list is None:
                self.reporter.report_warning(
                    f"{self.table.full_name}-arg-list",
                    f"Argument list not found for data-access-function {data_access_func}",
                )
                return None

            return DataAccessFunctionDetail(
                arg_list=arg_list,
                data_access_function_name=data_access_func,
                identifier_accessor=None,
            )

        # function is not data-access function, lets process function argument
        first_arg_tree: Optional[Tree] = tree_function.first_arg_list_func(
            invoke_expression
        )

        if first_arg_tree is None:
            logger.debug(
                "Function invocation without argument in expression = %s",
                invoke_expression.pretty(),
            )
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "Function invocation without argument",
            )
            return None

        first_argument: Tree = tree_function.flat_argument_list(first_arg_tree)[
            0
        ]  # take first argument only
        expression: Optional[Tree] = tree_function.first_list_expression_func(
            first_argument
        )

        logger.debug("Extracting token from tree %s", first_argument.pretty())
        if expression is None:
            expression = tree_function.first_type_expression_func(first_argument)
            if expression is None:
                logger.debug(
                    "Either list_expression or type_expression is not found = %s",
                    invoke_expression.pretty(),
                )
                self.reporter.report_warning(
                    f"{self.table.full_name}-variable-statement",
                    "Function argument expression is not supported",
                )
                return None

        tokens: List[str] = tree_function.remove_whitespaces_from_list(
            tree_function.token_values(expression)
        )

        logger.debug("Tokens in invoke expression are %s", tokens)
        return tokens

    def _process_item_selector_expression(
        self, rh_tree: Tree
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        new_identifier, key_vs_value = self.get_item_selector_tokens(  # type: ignore
            cast(Tree, tree_function.first_expression_func(rh_tree))
        )

        return new_identifier, key_vs_value

    @staticmethod
    def _create_or_update_identifier_accessor(
        identifier_accessor: Optional[IdentifierAccessor],
        new_identifier: str,
        key_vs_value: Dict[str, Any],
    ) -> IdentifierAccessor:

        # It is first identifier_accessor
        if identifier_accessor is None:
            return IdentifierAccessor(
                identifier=new_identifier, items=key_vs_value, next=None
            )

        new_identifier_accessor: IdentifierAccessor = IdentifierAccessor(
            identifier=new_identifier, items=key_vs_value, next=identifier_accessor
        )

        return new_identifier_accessor

    def create_data_access_functional_detail(
        self, identifier: str
    ) -> List[DataAccessFunctionDetail]:
        table_links: List[DataAccessFunctionDetail] = []

        def internal(
            current_identifier: str,
            identifier_accessor: Optional[IdentifierAccessor],
        ) -> None:
            """
            1) Find statement where identifier appear in the left-hand side i.e. identifier  = expression
            2) Check expression is function invocation i.e. invoke_expression or item_selector
            3) if it is function invocation and this function is not the data-access function then take first argument
               i.e. identifier and call the function recursively
            4) if it is item_selector then take identifier and key-value pair,
               add identifier and key-value pair in current_selector and call the function recursively
            5) This recursion will continue till we reach to data-access function and during recursion we will fill
               token_dict dictionary for all item_selector we find during traversal.

            :param current_identifier: variable to look for
            :param identifier_accessor:
            :return: None
            """
            # Grammar of variable_statement is <variable-name> = <expression>
            # Examples: Source = PostgreSql.Database(<arg-list>)
            #           public_order_date = Source{[Schema="public",Item="order_date"]}[Data]
            v_statement: Optional[Tree] = tree_function.get_variable_statement(
                self.parse_tree, current_identifier
            )
            if v_statement is None:
                self.reporter.report_warning(
                    f"{self.table.full_name}-variable-statement",
                    f"output variable ({current_identifier}) statement not found in table expression",
                )
                return None

            # Any expression after "=" sign of variable-statement
            rh_tree: Optional[Tree] = tree_function.first_expression_func(v_statement)
            if rh_tree is None:
                logger.debug("Expression tree not found")
                logger.debug(v_statement.pretty())
                return None

            invoke_expression: Optional[
                Tree
            ] = tree_function.first_invoke_expression_func(rh_tree)

            if invoke_expression is not None:
                result: Union[
                    DataAccessFunctionDetail, List[str], None
                ] = self._process_invoke_expression(invoke_expression)
                if result is None:
                    return None  # No need to process some un-expected grammar found while processing invoke_expression
                if isinstance(result, DataAccessFunctionDetail):
                    cast(
                        DataAccessFunctionDetail, result
                    ).identifier_accessor = identifier_accessor
                    table_links.append(result)  # Link of a table is completed
                    identifier_accessor = (
                        None  # reset the identifier_accessor for other table
                    )
                    return None
                # Process first argument of the function.
                # The first argument can be a single table argument or list of table.
                # For example Table.Combine({t1,t2},....), here first argument is list of table.
                # Table.AddColumn(t1,....), here first argument is single table.
                for token in cast(List[str], result):
                    internal(token, identifier_accessor)

            else:
                new_identifier, key_vs_value = self._process_item_selector_expression(
                    rh_tree
                )
                if new_identifier is None or key_vs_value is None:
                    logger.debug("Required information not found in rh_tree")
                    return None
                new_identifier_accessor: IdentifierAccessor = (
                    self._create_or_update_identifier_accessor(
                        identifier_accessor, new_identifier, key_vs_value
                    )
                )

                return internal(new_identifier, new_identifier_accessor)

        internal(identifier, None)

        return table_links

    def resolve_to_data_platform_table_list(self) -> List[DataPlatformTable]:
        data_platform_tables: List[DataPlatformTable] = []

        output_variable: Optional[str] = tree_function.get_output_variable(
            self.parse_tree
        )

        if output_variable is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-output-variable",
                "output-variable not found in table expression",
            )
            return data_platform_tables

        table_links: List[
            DataAccessFunctionDetail
        ] = self.create_data_access_functional_detail(output_variable)

        # Each item is data-access function
        for f_detail in table_links:
            supported_resolver = SupportedResolver.get_resolver(
                f_detail.data_access_function_name
            )
            if supported_resolver is None:
                logger.debug(
                    "Resolver not found for the data-access-function %s",
                    f_detail.data_access_function_name,
                )
                self.reporter.report_warning(
                    f"{self.table.full_name}-data-access-function",
                    f"Resolver not found for data-access-function = {f_detail.data_access_function_name}",
                )
                continue

            table_full_name_creator: AbstractTableFullNameCreator = (
                supported_resolver.get_table_full_name_creator()()
            )

            for table_full_name in table_full_name_creator.get_full_table_names(
                f_detail
            ):
                data_platform_tables.append(
                    DataPlatformTable(
                        name=table_full_name.split(".")[-1],
                        full_name=table_full_name,
                        data_platform_pair=table_full_name_creator.get_platform_pair(),
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

    def two_level_access_pattern(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> List[str]:
        full_table_names: List[str] = []

        logger.debug(
            "Processing PostgreSQL data-access function detail %s",
            data_access_func_detail,
        )
        arguments: List[str] = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                tree_function.token_values(data_access_func_detail.arg_list)
            ),
            char='"',
        )

        if len(arguments) != 2:
            logger.debug("Expected 2 arguments, but got {%s}", len(arguments))
            return full_table_names

        db_name: str = arguments[1]

        schema_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Schema"]

        table_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Item"]

        full_table_names.append(f"{db_name}.{schema_name}.{table_name}")

        logger.debug(
            "Platform(%s) full-table-names = %s",
            self.get_platform_pair().datahub_data_platform_name,
            full_table_names,
        )

        return full_table_names


class PostgresTableFullNameCreator(DefaultTwoStepDataAccessSources):
    def get_full_table_names(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> List[str]:
        return self.two_level_access_pattern(data_access_func_detail)

    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.POSTGRES_SQL.value


class MSSqlTableFullNameCreator(DefaultTwoStepDataAccessSources):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.MS_SQL.value

    def get_full_table_names(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> List[str]:
        full_table_names: List[str] = []
        arguments: List[str] = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                tree_function.token_values(data_access_func_detail.arg_list)
            ),
            char='"',
        )

        if len(arguments) == 2:
            # It is regular case of MS-SQL
            logger.debug("Handling with regular case")
            return self.two_level_access_pattern(data_access_func_detail)

        if len(arguments) >= 4 and arguments[2] != "Query":
            logger.debug("Unsupported case is found. Second index is not the Query")
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

        logger.debug("MS-SQL full-table-names %s", full_table_names)

        return full_table_names


class OracleTableFullNameCreator(AbstractTableFullNameCreator):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.ORACLE.value

    def _get_db_name(self, value: str) -> Optional[str]:
        error_message: str = f"The target argument ({value}) should in the format of <host-name>:<port>/<db-name>[.<domain>]"
        splitter_result: List[str] = value.split("/")
        if len(splitter_result) != 2:
            logger.debug(error_message)
            return None

        db_name = splitter_result[1].split(".")[0]

        return db_name

    def get_full_table_names(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> List[str]:
        full_table_names: List[str] = []

        logger.debug(
            "Processing Oracle data-access function detail %s", data_access_func_detail
        )

        arguments: List[str] = tree_function.remove_whitespaces_from_list(
            tree_function.token_values(data_access_func_detail.arg_list)
        )

        db_name: Optional[str] = self._get_db_name(arguments[0])
        if db_name is None:
            return full_table_names

        schema_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Schema"]

        table_name: str = cast(
            IdentifierAccessor,
            cast(IdentifierAccessor, data_access_func_detail.identifier_accessor).next,
        ).items["Name"]

        full_table_names.append(f"{db_name}.{schema_name}.{table_name}")

        return full_table_names


class SnowflakeTableFullNameCreator(AbstractTableFullNameCreator):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.SNOWFLAKE.value

    def get_full_table_names(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> List[str]:

        logger.debug("Processing Snowflake function detail %s", data_access_func_detail)
        # First is database name
        db_name: str = data_access_func_detail.identifier_accessor.items["Name"]  # type: ignore
        # Second is schema name
        schema_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor.next  # type: ignore
        ).items["Name"]
        # Third is table name
        table_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor.next.next  # type: ignore
        ).items["Name"]

        full_table_name: str = f"{db_name}.{schema_name}.{table_name}"

        logger.debug("Snowflake full-table-name %s", full_table_name)

        return [full_table_name]


class NativeQueryTableFullNameCreator(AbstractTableFullNameCreator):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.SNOWFLAKE.value

    def get_full_table_names(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> List[str]:
        full_table_names: List[str] = []
        t1: Tree = cast(
            Tree, tree_function.first_arg_list_func(data_access_func_detail.arg_list)
        )
        flat_argument_list: List[Tree] = tree_function.flat_argument_list(t1)

        if len(flat_argument_list) != 2:
            logger.debug(
                "Expecting 2 argument, actual argument count is %s",
                len(flat_argument_list),
            )
            logger.debug("Flat argument list = %s", flat_argument_list)
            return full_table_names

        data_access_tokens: List[str] = tree_function.remove_whitespaces_from_list(
            tree_function.token_values(flat_argument_list[0])
        )
        if (
            data_access_tokens[0]
            != SupportedDataPlatform.SNOWFLAKE.value.powerbi_data_platform_name
        ):
            logger.debug(
                "Provided native-query data-platform = %s", data_access_tokens[0]
            )
            logger.debug("Only Snowflake is supported in NativeQuery")
            return full_table_names

        # First argument is the query
        sql_query: str = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                tree_function.token_values(flat_argument_list[1])
            ),
            char='"',
        )[
            0
        ]  # Remove any whitespaces and double quotes character

        for table in native_sql_parser.get_tables(sql_query):
            if len(table.split(".")) != 3:
                logger.debug(
                    "Skipping table (%s) as it is not as per full_table_name format",
                    table,
                )
                continue

            full_table_names.append(table)

        return full_table_names


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
            functions.append(supported_resolver.get_function_name())

        return functions

    @staticmethod
    def get_resolver(function_name: str) -> Optional["SupportedResolver"]:
        logger.debug("Looking for resolver %s", function_name)
        for supported_resolver in SupportedResolver:
            if function_name == supported_resolver.get_function_name():
                return supported_resolver
        logger.debug("Looking not found for resolver %s", function_name)
        return None
