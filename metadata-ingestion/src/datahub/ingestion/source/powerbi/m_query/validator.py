import logging
from typing import Optional, Tuple

from datahub.ingestion.source.powerbi.m_query import resolver

logger = logging.getLogger(__name__)


def validate_parse_tree(
    expression: str, native_query_enabled: bool = True
) -> Tuple[bool, Optional[str]]:
    """
    :param expression: M-Query expression to check if supported data-function is present in expression
    :param native_query_enabled: Whether user want to extract lineage from native query
    :return: True or False.
    """
    function_names = [fun.value for fun in resolver.FunctionName]
    if not any(fun in expression for fun in function_names):
        return False, "DataAccess function is not present in M-Query expression."

    if native_query_enabled is False:
        if resolver.FunctionName.NATIVE_QUERY.value in function_names:
            return (
                False,
                "Lineage extraction from native query is disabled. Enable native_query_parsing in recipe",
            )

    return True, None
