# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging
from typing import Any

import sqlparse

logger = logging.getLogger(__name__)

# TODO: The sql query formatting functionality is duplicated by the try_format_query method,
# which is powered by sqlglot instead of sqlparse.


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
