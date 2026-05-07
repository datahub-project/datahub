"""GET_LINEAGE UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_get_lineage_udf() -> str:
    """Generate GET_LINEAGE UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.get_lineage() to get upstream
    or downstream lineage for any entity from Snowflake.

    Parameters:
        urn (STRING): Entity URN
        column_name (STRING): Optional column name for column-level lineage (use NULL for entity-level)
        upstream (NUMBER): 1 for upstream lineage, 0 for downstream lineage
        max_hops (NUMBER): Maximum number of hops (1-3+, default: 1)
        max_results (NUMBER): Maximum number of results to return (default: 30)

    Returns:
        VARIANT: Dictionary with upstreams or downstreams field containing lineage entities,
                facets, and metadata. For column-level lineage, includes lineageColumns showing
                which columns have relationships.
    """
    function_body = """from datahub_agent_context.mcp_tools import get_lineage
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return get_lineage(
            urn=urn,
            column=column_name if column_name else None,
            upstream=bool(upstream),
            max_hops=int(max_hops) if max_hops else 1,
            max_results=int(max_results) if max_results else 30
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'urn': urn
    }"""

    return generate_python_udf_code(
        function_name="GET_LINEAGE",
        parameters=[
            ("urn", "STRING"),
            ("column_name", "STRING"),
            ("upstream", "NUMBER"),
            ("max_hops", "NUMBER"),
            ("max_results", "NUMBER"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
