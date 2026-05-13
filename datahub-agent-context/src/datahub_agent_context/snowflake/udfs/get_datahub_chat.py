"""GET_DATAHUB_CHAT UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_get_datahub_chat_udf() -> str:
    """Generate GET_DATAHUB_CHAT UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.get_datahub_chat() to
    retrieve messages and status from an Ask DataHub conversation.

    Cloud-only: requires a DataHub Cloud instance.

    Parameters:
        conversation_urn (STRING): URN of the conversation to retrieve.
        message_limit (NUMBER): Maximum number of messages to return. Default 10.
        offset (NUMBER): Number of most-recent messages to skip. Default 0.

    Returns:
        VARIANT: Dictionary with conversation_urn, status, messages, and response.

    Examples:
        - Get latest: GET_DATAHUB_CHAT('urn:li:dataHubAiConversation:abc', 10, 0)
        - Page older: GET_DATAHUB_CHAT('urn:li:dataHubAiConversation:abc', 10, 10)
    """
    function_body = """from datahub_agent_context.mcp_tools import get_datahub_chat
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return get_datahub_chat(
            conversation_urn=conversation_urn,
            message_limit=int(message_limit),
            offset=int(offset),
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'conversation_urn': conversation_urn
    }"""

    return generate_python_udf_code(
        function_name="GET_DATAHUB_CHAT",
        parameters=[
            ("conversation_urn", "STRING"),
            ("message_limit", "NUMBER"),
            ("offset", "NUMBER"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
