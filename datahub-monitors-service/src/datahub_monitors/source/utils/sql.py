import logging
from typing import Optional

from ..types import SourceOperationParams

logger = logging.getLogger(__name__)


def setup_high_watermark_field_value_query(
    column_name: str,
    operation_params: SourceOperationParams,
    filter_sql: str,
    previous_value: Optional[str],
) -> str:
    filter_sql_part = ""
    if filter_sql:
        if previous_value:
            filter_sql_part = f"AND {filter_sql}"
        else:
            filter_sql_part = f"WHERE {filter_sql}"

    get_value_query = f"""
        SELECT {column_name}
        FROM {operation_params.database}.{operation_params.schema}.{operation_params.table}
        {f"WHERE {column_name} >= '{previous_value}'" if previous_value else ''}
        {filter_sql_part}
        ORDER by {column_name} DESC
        LIMIT 1;
    """
    logger.debug(get_value_query)

    return get_value_query


def setup_high_watermark_row_count_query(
    column_name: str,
    operation_params: SourceOperationParams,
    filter_sql: str,
    current_field_value: str,
) -> str:
    get_count_query = f"""
        SELECT COUNT(*)
        FROM {operation_params.database}.{operation_params.schema}.{operation_params.table}
        WHERE {column_name} = '{current_field_value}'
        {f"AND {filter_sql}" if filter_sql else ''}
    """
    logger.debug(get_count_query)
    return get_count_query
