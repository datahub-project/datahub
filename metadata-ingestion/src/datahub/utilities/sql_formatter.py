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
