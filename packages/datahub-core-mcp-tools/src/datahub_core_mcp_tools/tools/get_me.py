"""The get_me tool: who the caller is in DataHub."""

from typing import Any

from fastmcp.exceptions import ToolError
from loguru import logger

from .. import graphql_helpers
from ..version_requirements import min_version, read_only

GET_ME_QUERY = """
query getMe {
    me {
        corpUser {
            type
            urn
            username
            exists
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

# GMS answers `me` for the actor it authenticated. With metadata-service auth off it
# accepts any token, or none, and answers for an actor that may have no user entity.
NONEXISTENT_USER_WARNING = (
    "DataHub returned a user that does not exist ({user}). Check the auth "
    "settings and METADATA_SERVICE_AUTH_ENABLED; the supplied token might be invalid."
)


@read_only
@min_version(cloud="0.3.16", oss="1.4.0")
def get_me() -> dict[str, Any]:
    """Get information about the currently authenticated user.

    This tool fetches detailed information about the authenticated user including:
    - User profile information (username, email, full name, etc.)
    - Group memberships

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - data: User information under me.corpUser
        - message: Success message, or a warning when DataHub reports the user
          does not exist

    Example:
        # Get current user information
        get_me()
    """
    client = graphql_helpers.get_datahub_client()

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=GET_ME_QUERY,
            variables={},
            operation_name="getMe",
        )
    except Exception as e:
        raise ToolError(f"Error retrieving user information: {e}") from e

    me_data = result.get("me") if isinstance(result, dict) else None
    if not me_data:
        raise ToolError("No authenticated user found")

    message = "Successfully retrieved authenticated user information"
    corp_user = me_data.get("corpUser") or {}
    if corp_user.get("exists") is False:
        message = NONEXISTENT_USER_WARNING.format(
            user=corp_user.get("urn") or corp_user.get("username") or "unknown"
        )
        logger.warning(message)

    return {"success": True, "data": me_data, "message": message}
