import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

from lark import Tree

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
)
from datahub.ingestion.source.powerbi.m_query import tree_function
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    TRACE_POWERBI_MQUERY_PARSER,
    DataAccessFunctionDetail,
    IdentifierAccessor,
    Lineage,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    AbstractLineage,
    SupportedPattern,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table

logger = logging.getLogger(__name__)


class AbstractDataAccessMQueryResolver(ABC):
    table: Table
    parse_tree: Tree
    parameters: Dict[str, str]
    reporter: PowerBiDashboardSourceReport
    data_access_functions: List[str]

    def __init__(
        self,
        table: Table,
        parse_tree: Tree,
        reporter: PowerBiDashboardSourceReport,
        parameters: Dict[str, str],
    ):
        self.table = table
        self.parse_tree = parse_tree
        self.reporter = reporter
        self.parameters = parameters
        self.data_access_functions = SupportedPattern.get_function_names()

    @abstractmethod
    def resolve_to_lineage(
        self,
        ctx: PipelineContext,
        config: PowerBiDashboardSourceConfig,
        platform_instance_resolver: AbstractDataPlatformInstanceResolver,
    ) -> List[Lineage]:
        pass


class MQueryResolver(AbstractDataAccessMQueryResolver, ABC):
    """
    This class parses the M-Query recursively to generate DataAccessFunctionDetail (see method create_data_access_functional_detail).

    This class has generic code to process M-Query tokens and create instance of DataAccessFunctionDetail.

    Once DataAccessFunctionDetail instance is initialized thereafter MQueryResolver generates the DataPlatformTable with the help of AbstractDataPlatformTableCreator
    (see method resolve_to_lineage).

    Classes which extended from AbstractDataPlatformTableCreator know how to convert generated DataAccessFunctionDetail instance
     to the respective DataPlatformTable instance as per dataplatform.

    """

    def get_item_selector_tokens(
        self,
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
                tree_function.token_values(item_selector, parameters=self.parameters)
            ),
        )
        identifier: List[str] = tree_function.token_values(
            identifier_tree, parameters={}
        )

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

    def take_first_argument(self, expression: Tree) -> Optional[Tree]:
        # function is not data-access function, lets process function argument
        first_arg_tree: Optional[Tree] = tree_function.first_arg_list_func(expression)

        if first_arg_tree is None:
            logger.debug(
                f"Function invocation without argument in expression = {expression.pretty()}"
            )
            self.reporter.report_warning(
                f"{self.table.full_name}-variable-statement",
                "Function invocation without argument",
            )
            return None
        return first_arg_tree

    def _process_invoke_expression(
        self, invoke_expression: Tree
    ) -> Union[DataAccessFunctionDetail, List[str], None]:
        letter_tree: Tree = invoke_expression.children[0]
        data_access_func: str = tree_function.make_function_name(letter_tree)
        # The invoke function is either DataAccess function like PostgreSQL.Database(<argument-list>) or
        # some other function like Table.AddColumn or Table.Combine and so on

        logger.debug(f"function-name: {data_access_func}")

        if data_access_func in self.data_access_functions:
            arg_list: Optional[Tree] = MQueryResolver.get_argument_list(
                invoke_expression
            )
            if arg_list is None:
                self.reporter.report_warning(
                    title="M-Query Resolver Error",
                    message="Unable to extract lineage from parsed M-Query expression (missing argument list)",
                    context=f"{self.table.full_name}: argument list not found for data-access-function {data_access_func}",
                )
                return None

            return DataAccessFunctionDetail(
                arg_list=arg_list,
                data_access_function_name=data_access_func,
                identifier_accessor=None,
            )

        first_arg_tree: Optional[Tree] = self.take_first_argument(invoke_expression)
        if first_arg_tree is None:
            return None

        flat_arg_list: List[Tree] = tree_function.flat_argument_list(first_arg_tree)
        if len(flat_arg_list) == 0:
            logger.debug("flat_arg_list is zero")
            return None

        first_argument: Tree = flat_arg_list[0]  # take first argument only

        # Detect nested function calls in the first argument
        # M-Query's data transformation pipeline:
        # 1. Functions typically operate on tables/columns
        # 2. First argument must be either:
        #    - A table variable name (referencing data source)
        #    - Another function that eventually leads to a table
        #
        # Example of nested functions:
        #   #"Split Column by Delimiter2" = Table.SplitColumn(
        #       Table.TransformColumnTypes(#"Removed Columns1", "KB")
        #   )
        #
        # In this example:
        # - The inner function Table.TransformColumnTypes takes #"Removed Columns1"
        #   (a table reference) as its first argument
        # - Its result is then passed as the first argument to Table.SplitColumn
        second_invoke_expression: Optional[Tree] = (
            tree_function.first_invoke_expression_func(first_argument)
        )
        if second_invoke_expression:
            # 1. The First argument is function call
            # 2. That function's first argument references next table variable
            first_arg_tree = self.take_first_argument(second_invoke_expression)
            if first_arg_tree is None:
                return None

            flat_arg_list = tree_function.flat_argument_list(first_arg_tree)
            if len(flat_arg_list) == 0:
                logger.debug("flat_arg_list is zero")
                return None

            first_argument = flat_arg_list[0]  # take first argument only

        expression: Optional[Tree] = tree_function.first_list_expression_func(
            first_argument
        )

        if TRACE_POWERBI_MQUERY_PARSER:
            logger.debug(f"Extracting token from tree {first_argument.pretty()}")
        else:
            logger.debug(f"Extracting token from tree {first_argument}")
        if expression is None:
            expression = tree_function.first_type_expression_func(first_argument)
            if expression is None:
                logger.debug(
                    f"Either list_expression or type_expression is not found = {invoke_expression.pretty()}"
                )
                self.reporter.report_warning(
                    title="M-Query Resolver Error",
                    message="Unable to extract lineage from parsed M-Query expression (function argument expression is not supported)",
                    context=f"{self.table.full_name}: function argument expression is not supported",
                )
                return None

        tokens: List[str] = tree_function.remove_whitespaces_from_list(
            tree_function.token_values(expression)
        )

        logger.debug(f"Tokens in invoke expression are {tokens}")
        return tokens

    def _process_item_selector_expression(
        self, rh_tree: Tree
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        first_expression: Optional[Tree] = tree_function.first_expression_func(rh_tree)
        assert first_expression is not None

        new_identifier, key_vs_value = self.get_item_selector_tokens(first_expression)
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
                    title="Unable to extract lineage from M-Query expression",
                    message="Lineage will be incomplete.",
                    context=f"table-full-name={self.table.full_name}, expression = {self.table.expression}, output-variable={current_identifier} not found in table expression",
                )
                return None

            # Any expression after "=" sign of variable-statement
            rh_tree: Optional[Tree] = tree_function.first_expression_func(v_statement)
            if rh_tree is None:
                logger.debug("Expression tree not found")
                logger.debug(v_statement.pretty())
                return None

            invoke_expression: Optional[Tree] = (
                tree_function.first_invoke_expression_func(rh_tree)
            )

            if invoke_expression is not None:
                result: Union[DataAccessFunctionDetail, List[str], None] = (
                    self._process_invoke_expression(invoke_expression)
                )
                if result is None:
                    return None  # No need to process some un-expected grammar found while processing invoke_expression
                if isinstance(result, DataAccessFunctionDetail):
                    result.identifier_accessor = identifier_accessor
                    table_links.append(result)  # Link of a table is completed
                    identifier_accessor = (
                        None  # reset the identifier_accessor for other table
                    )
                    return None
                # Process first argument of the function.
                # The first argument can be a single table argument or list of table.
                # For example Table.Combine({t1,t2},....), here first argument is list of table.
                # Table.AddColumn(t1,....), here first argument is single table.
                for token in result:
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

    def resolve_to_lineage(
        self,
        ctx: PipelineContext,
        config: PowerBiDashboardSourceConfig,
        platform_instance_resolver: AbstractDataPlatformInstanceResolver,
    ) -> List[Lineage]:
        lineage: List[Lineage] = []

        # Find out output variable as we are doing backtracking in M-Query
        output_variable: Optional[str] = tree_function.get_output_variable(
            self.parse_tree
        )

        if output_variable is None:
            self.reporter.report_warning(
                f"{self.table.full_name}-output-variable",
                "output-variable not found in table expression",
            )
            return lineage

        # Parse M-Query and use output_variable as root of tree and create instance of DataAccessFunctionDetail
        table_links: List[DataAccessFunctionDetail] = (
            self.create_data_access_functional_detail(output_variable)
        )

        # Each item is data-access function
        for f_detail in table_links:
            # Get & Check if we support data-access-function available in M-Query
            supported_resolver = SupportedPattern.get_pattern_handler(
                f_detail.data_access_function_name
            )
            if supported_resolver is None:
                logger.debug(
                    f"Resolver not found for the data-access-function {f_detail.data_access_function_name}"
                )
                self.reporter.report_warning(
                    f"{self.table.full_name}-data-access-function",
                    f"Resolver not found for data-access-function = {f_detail.data_access_function_name}",
                )
                continue

            # From supported_resolver enum get respective handler like AmazonRedshift or Snowflake or Oracle or NativeQuery and create instance of it
            # & also pass additional information that will be need to generate lineage
            pattern_handler: AbstractLineage = supported_resolver.handler()(
                ctx=ctx,
                table=self.table,
                config=config,
                reporter=self.reporter,
                platform_instance_resolver=platform_instance_resolver,
            )

            lineage.append(pattern_handler.create_lineage(f_detail))

        return lineage
