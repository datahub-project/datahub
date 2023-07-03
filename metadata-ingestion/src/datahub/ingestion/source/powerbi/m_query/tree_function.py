import logging
from functools import partial
from typing import Any, Dict, List, Optional, Union, cast

from lark import Token, Tree

from datahub.ingestion.source.powerbi.m_query.data_classes import (
    TRACE_POWERBI_MQUERY_PARSER,
)

logger = logging.getLogger(__name__)


def get_output_variable(root: Tree) -> Optional[str]:
    in_expression_tree: Optional[Tree] = get_first_rule(root, "in_expression")
    if in_expression_tree is None:
        return None
    # Get list of terminal value
    # Remove any whitespaces
    # Remove any spaces
    return "".join(
        strip_char_from_list(
            remove_whitespaces_from_list(token_values(in_expression_tree)), " "
        )
    )


def get_variable_statement(parse_tree: Tree, variable: str) -> Optional[Tree]:
    _filter = parse_tree.find_data("variable")
    # filter will return statement of the form <variable-name> = <expression>
    # We are searching for Tree where variable-name is matching with provided variable
    for tree in _filter:
        values: List[str] = token_values(tree.children[0])
        actual_value: str = "".join(strip_char_from_list(values, " "))
        if TRACE_POWERBI_MQUERY_PARSER:
            logger.debug(f"Actual Value = {actual_value}")
            logger.debug(f"Expected Value = {variable}")

        if actual_value.lower() == variable.lower():
            return tree

    logger.debug(f"Provided variable({variable}) not found in variable rule")

    return None


def get_first_rule(tree: Tree, rule: str) -> Optional[Tree]:
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


def token_values(tree: Tree, parameters: Dict[str, str] = {}) -> List[str]:
    """
    :param tree: Tree to traverse
    :param parameters: If parameters is not an empty dict, it will try to resolve identifier variable references
                       using the values in 'parameters'.
    :return: List of leaf token data
    """
    values: List[str] = []

    def internal(node: Union[Tree, Token]) -> None:
        if parameters and isinstance(node, Tree) and node.data == "identifier":
            # This is the case where they reference a variable using
            # the `#"Name of variable"` or `Variable` syntax. It can be
            # a quoted_identifier or a regular_identifier.

            ref = make_function_name(node)

            # For quoted_identifier, ref will have quotes around it.
            if ref.startswith('"') and ref[1:-1] in parameters:
                resolved = parameters[ref[1:-1]]
                values.append(resolved)
            elif ref in parameters:
                resolved = parameters[ref]
                values.append(resolved)
            else:
                # If we can't resolve, fall back to the name of the variable.
                logger.debug(f"Unable to resolve parameter reference to {ref}")
                values.append(ref)
        elif isinstance(node, Token):
            # This means we're probably looking at a literal.
            values.append(cast(Token, node).value)
            return
        else:
            for child in node.children:
                internal(child)

    internal(tree)

    return values


def remove_whitespaces_from_list(values: List[str]) -> List[str]:
    result: List[str] = []
    for item in values:
        if item.strip() not in ("", "\n", "\t"):
            result.append(item)

    return result


def strip_char_from_list(values: List[str], char: str = '"') -> List[str]:
    result: List[str] = []
    for item in values:
        result.append(item.strip(char))

    return result


def make_function_name(tree: Tree) -> str:
    values: List[str] = token_values(tree)
    return ".".join(values)


def get_all_function_name(tree: Tree) -> List[str]:
    """
    Returns all function name present in input tree
    :param tree: Input lexical tree
    :return: list of function name
    """
    functions: List[str] = []

    # List the all invoke_expression in the Tree
    _filter: Any = tree.find_data("invoke_expression")

    for node in _filter:
        if TRACE_POWERBI_MQUERY_PARSER:
            logger.debug(f"Tree = {node.pretty()}")
        primary_expression_node: Optional[Tree] = first_primary_expression_func(node)
        if primary_expression_node is None:
            continue

        identifier_node: Optional[Tree] = first_identifier_func(primary_expression_node)
        if identifier_node is None:
            continue

        functions.append(make_function_name(identifier_node))

    return functions


def flat_argument_list(tree: Tree) -> List[Tree]:
    values: List[Tree] = []

    for child in tree.children:
        if isinstance(child, Token):
            continue
        if isinstance(child, Tree) and (
            child.data == "argument_list" or child.data == "expression"
        ):
            values.append(child)

    return values


first_expression_func = partial(get_first_rule, rule="expression")
first_item_selector_func = partial(get_first_rule, rule="item_selector")
first_arg_list_func = partial(get_first_rule, rule="argument_list")
first_identifier_func = partial(get_first_rule, rule="identifier")
first_primary_expression_func = partial(get_first_rule, rule="primary_expression")
first_invoke_expression_func = partial(get_first_rule, rule="invoke_expression")
first_type_expression_func = partial(get_first_rule, rule="type_expression")
first_list_expression_func = partial(get_first_rule, rule="list_expression")
