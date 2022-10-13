import logging
from typing import Any

import sqlparse

logger = logging.getLogger(__name__)


def format_sql_query(query: str, **options: Any) -> str:
    try:
        return sqlparse.format(query, **options)
    except Exception as e:
        logger.debug(f"Exception:{e} while formatting query '{query}'.")
        return query


def trim_query(
    query: str, budget_per_query: int, query_trimmer_string: str = " ..."
) -> str:
    trimmed_query = query
    if len(query) > budget_per_query:
        if budget_per_query - len(query_trimmer_string) > 0:
            end_index = budget_per_query - len(query_trimmer_string)
            trimmed_query = query[:end_index] + query_trimmer_string
        else:
            raise Exception(
                "Budget per query is too low. Please, decrease the number of top_n_queries."
            )
    return trimmed_query
