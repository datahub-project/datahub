"""GET_ME UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_get_me_udf() -> str:
    """Generate GET_ME UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.get_me() to get information
    about the currently authenticated user.

    Returns user profile information, platform privileges, group memberships,
    and user settings.

    Parameters:
        None

    Returns:
        VARIANT: Dictionary with:
                - success: Boolean indicating if operation succeeded
                - data: User information including corpUser details
                - message: Success or error message

    Example:
        - Get current user: SELECT GET_ME()
    """
    function_body = """from datahub_agent_context.mcp_tools import get_me
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return get_me()

except Exception as e:
    return {
        'success': False,
        'error': str(e)
    }"""

    return generate_python_udf_code(
        function_name="GET_ME",
        parameters=[],
        return_type="VARIANT",
        function_body=function_body,
    )
