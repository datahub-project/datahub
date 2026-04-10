"""ASK_DATAHUB_CHAT UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_ask_datahub_chat_udf() -> str:
    """Generate ASK_DATAHUB_CHAT UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.ask_datahub_chat() to send
    questions to the DataHub AI assistant from Snowflake. Blocks until the
    agent responds. Each call creates a new conversation.

    Cloud-only: requires a DataHub Cloud instance.

    Parameters:
        message (STRING): The question to ask the AI agent.

    Returns:
        VARIANT: Dictionary with conversation_urn and response.

    Examples:
        - ASK_DATAHUB_CHAT('What are the most queried datasets?')
        - ASK_DATAHUB_CHAT('Show me the schema of the orders table')
    """
    function_body = """from datahub_agent_context.mcp_tools import ask_datahub_chat
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return ask_datahub_chat(
            message=message,
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'message': message
    }"""

    return generate_python_udf_code(
        function_name="ASK_DATAHUB_CHAT",
        parameters=[("message", "STRING")],
        return_type="VARIANT",
        function_body=function_body,
    )
