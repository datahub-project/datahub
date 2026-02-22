"""Get authenticated user information tool for DataHub MCP server."""

import logging
from typing import Any

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql

logger = logging.getLogger(__name__)


def get_me() -> dict[str, Any]:
    """Get information about the currently authenticated user.

    This tool fetches detailed information about the authenticated user including:
    - User profile information (username, email, full name, etc.)
    - Platform privileges (what the user can do in DataHub)
    - Group memberships
    - User settings and preferences

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - data: User information including corpUser and platformPrivileges
        - message: Success or error message

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = get_me()
    """
    graph = get_graph()
    # GraphQL query to get authenticated user information
    query = """
        query getMe {
            me {
                corpUser {
                    type
                    urn
                    username
                    info {
                        active
                        displayName
                        title
                        firstName
                        lastName
                        fullName
                        email
                    }
                    editableProperties {
                        displayName
                        title
                        pictureLink
                        teams
                        skills
                    }
                    groups: relationships(
                        input: { types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: OUTGOING, start: 0, count: 50 }
                    ) {
                        relationships {
                            entity {
                                ... on CorpGroup {
                                    urn
                                    name
                                    properties {
                                        displayName
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    """

    try:
        result = execute_graphql(
            graph,
            query=query,
            variables={},
            operation_name="getMe",
        )

        me_data = result.get("me")
        if me_data:
            return {
                "success": True,
                "data": me_data,
                "message": "Successfully retrieved authenticated user information",
            }
        else:
            raise RuntimeError("No authenticated user found")

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error retrieving user information: {str(e)}") from e
