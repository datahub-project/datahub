"""GET_DATASET_ASSERTIONS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_get_dataset_assertions_udf() -> str:
    """Generate GET_DATASET_ASSERTIONS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.get_dataset_assertions() to retrieve
    data quality assertions for a dataset, with their latest run results.

    Parameters:
        urn (STRING): Dataset URN
        column_name (STRING): Optional column/field path to filter assertions by (use NULL for all)
        assertion_type (STRING): Optional type filter - 'FRESHNESS', 'VOLUME', 'FIELD', 'SQL',
                                 'DATASET', 'DATA_SCHEMA', 'CUSTOM', or NULL for all
        status (STRING): Optional status filter - 'PASSING', 'FAILING', 'ERROR', 'INIT', or NULL for all
        count (NUMBER): Number of assertions to return (default: 5, max: 20)
        run_events_count (NUMBER): Recent run events per assertion (default: 1, max: 10)

    Returns:
        VARIANT: Dictionary with:
                - success: Whether the request was successful
                - data: Contains start, count, total, and assertions list
                - message: Summary message

    Examples:
        - All assertions: GET_DATASET_ASSERTIONS(urn, NULL, NULL, NULL, 10, 1)
        - Column assertions: GET_DATASET_ASSERTIONS(urn, 'user_id', NULL, NULL, 10, 1)
        - Failing freshness: GET_DATASET_ASSERTIONS(urn, NULL, 'FRESHNESS', 'FAILING', 10, 1)
        - With run history: GET_DATASET_ASSERTIONS(urn, NULL, NULL, NULL, 5, 5)
    """
    function_body = """from datahub_agent_context.mcp_tools import get_dataset_assertions
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return get_dataset_assertions(
            urn=urn,
            column=column_name if column_name else None,
            assertion_type=assertion_type if assertion_type else None,
            status=status if status else None,
            count=int(count) if count else 5,
            run_events_count=int(run_events_count) if run_events_count else 1
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'urn': urn
    }"""

    return generate_python_udf_code(
        function_name="GET_DATASET_ASSERTIONS",
        parameters=[
            ("urn", "STRING"),
            ("column_name", "STRING"),
            ("assertion_type", "STRING"),
            ("status", "STRING"),
            ("count", "NUMBER"),
            ("run_events_count", "NUMBER"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
