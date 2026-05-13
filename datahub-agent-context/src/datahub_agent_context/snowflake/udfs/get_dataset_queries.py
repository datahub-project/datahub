"""GET_DATASET_QUERIES UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_get_dataset_queries_udf() -> str:
    """Generate GET_DATASET_QUERIES UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.get_dataset_queries() to retrieve
    SQL queries associated with a dataset or column to understand usage patterns.

    Useful for understanding how data is used, common JOIN patterns, typical filters,
    and aggregation logic. Can filter by query source (MANUAL vs SYSTEM).

    Parameters:
        urn (STRING): Dataset URN
        column_name (STRING): Optional column name to filter queries (use NULL for all dataset queries)
        source (STRING): Filter by query origin - 'MANUAL', 'SYSTEM', or NULL for both
        count (NUMBER): Number of queries to return (default: 10)

    Returns:
        VARIANT: Dictionary with:
                - total: Total number of queries matching criteria
                - queries: Array of query objects with SQL statements and metadata
                - start: Starting offset
                - count: Number of results returned

    Examples:
        - Manual queries: GET_DATASET_QUERIES(urn, NULL, 'MANUAL', 10)
        - System queries: GET_DATASET_QUERIES(urn, NULL, 'SYSTEM', 20)
        - Column queries: GET_DATASET_QUERIES(urn, 'customer_id', 'MANUAL', 5)
    """
    function_body = """from datahub_agent_context.mcp_tools import get_dataset_queries
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return get_dataset_queries(
            urn=urn,
            column=column_name if column_name else None,
            source=source if source else None,
            count=int(count) if count else 10
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'urn': urn
    }"""

    return generate_python_udf_code(
        function_name="GET_DATASET_QUERIES",
        parameters=[
            ("urn", "STRING"),
            ("column_name", "STRING"),
            ("source", "STRING"),
            ("count", "NUMBER"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
