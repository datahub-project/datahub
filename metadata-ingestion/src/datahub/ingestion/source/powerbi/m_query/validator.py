import logging
from typing import List, Optional, Tuple

from lark import Tree

from datahub.ingestion.source.powerbi.m_query import resolver, tree_function

logger = logging.getLogger(__name__)


def validate_parse_tree(
    tree: Tree, native_query_enabled: bool = True
) -> Tuple[bool, Optional[str]]:
    """
    :param tree: tree to validate as per functions supported by m_parser module
    :param native_query_enabled: Whether user want to extract lineage from native query
    :return: first argument is False if validation is failed and second argument would contain the error message.
             in-case of valid tree the first argument is True and second argument would be None.
    """
    functions: List[str] = tree_function.get_all_function_name(tree)
    if len(functions) == 0:
        return False, "Function calls not found"

    if native_query_enabled is False:
        if resolver.FunctionName.NATIVE_QUERY.value in functions:
            return False, "Lineage extraction from native query is disabled."

    return True, None
