import logging
from typing import Dict, List, Optional, Tuple

import slack_bolt
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import CorpUserUrn

from datahub_integrations.app import DATAHUB_FRONTEND_URL, graph
from datahub_integrations.graphql.subscription import GET_SUBSCRIPTION_QUERY

logger = logging.getLogger(__name__)


class UserResolutionResult:
    """Result of attempting to resolve a Slack user to a DataHub user.

    This is used across different Slack features (Ask DataHub mentions, incident resolution, etc.)
    to determine whether a user has connected their account and what action to take.
    """

    def __init__(
        self,
        user_urn: Optional[str] = None,
        requires_connection: bool = False,
    ):
        self.user_urn = user_urn
        self.requires_connection = requires_connection

    @property
    def is_resolved(self) -> bool:
        """User was successfully resolved to a DataHub user."""
        return self.user_urn is not None

    @property
    def should_prompt_connection(self) -> bool:
        """OAuth is required but user hasn't connected their account."""
        return self.requires_connection and self.user_urn is None


def resolve_slack_user_to_datahub(
    slack_user_id: str,
    require_oauth_binding: bool,
) -> UserResolutionResult:
    """
    Resolve a Slack user to a DataHub user.

    This is the common entry point for user resolution logic across all Slack features.

    Args:
        slack_user_id: The Slack user ID (e.g., "U12345678")
        require_oauth_binding: Whether OAuth binding is required (from feature flag)

    Returns:
        UserResolutionResult with:
        - user_urn: The resolved DataHub user URN (if found)
        - requires_connection: True if OAuth is required but user not connected
    """
    if not require_oauth_binding:
        logger.debug(
            f"OAuth binding not required. Processing request for {slack_user_id} "
            "with system credentials."
        )
        return UserResolutionResult(user_urn=None, requires_connection=False)

    datahub_user_urn = get_datahub_user_by_slack_id(slack_user_id)

    if datahub_user_urn is None:
        logger.debug(
            f"User {slack_user_id} has not connected their Slack account. "
            "OAuth binding is required."
        )
        return UserResolutionResult(user_urn=None, requires_connection=True)

    logger.debug(
        f"User {slack_user_id} resolved to {datahub_user_urn} via OAuth binding"
    )
    return UserResolutionResult(user_urn=datahub_user_urn, requires_connection=True)


def build_connect_account_blocks(
    slack_user_id: str,
    action_description: str = "use this feature",
) -> Tuple[str, List[Dict]]:
    """
    Build Slack message blocks prompting the user to connect their Slack account to DataHub.

    This is shown when require_slack_oauth_binding is TRUE and the user
    hasn't connected their account yet.

    Args:
        slack_user_id: The Slack user ID to mention
        action_description: Description of what action requires the connection
            (e.g., "use Ask DataHub", "manage incidents")

    Returns:
        tuple[str, List[dict]]: (plain text fallback, rich Slack message blocks)
    """
    connect_url = (
        f"{DATAHUB_FRONTEND_URL}/settings/personal-notifications?connect_slack=true"
    )

    text = f"Hi <@{slack_user_id}>! To {action_description}, you need to connect your DataHub account to Slack first."

    blocks: List[Dict] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text,
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": ":link: Connect To DataHub",
                        "emoji": True,
                    },
                    "url": connect_url,
                    "style": "primary",
                    "action_id": "external_redirect",
                }
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "_This is a one-time setup that takes less than a minute._",
                }
            ],
        },
    ]

    return text, blocks


def build_authorization_error_blocks(
    slack_user_id: str,
    action_description: str = "perform this action",
) -> Tuple[str, List[Dict]]:
    """
    Build Slack message blocks informing the user they don't have permission.

    This is shown when a user attempts an action they're not authorized to perform
    (e.g., resolving/reopening an incident without the required privileges).

    Args:
        slack_user_id: The Slack user ID to mention
        action_description: Description of what action was attempted
            (e.g., "resolve this incident", "reopen this incident")

    Returns:
        tuple[str, List[dict]]: (plain text fallback, rich Slack message blocks)
    """
    text = (
        f"Sorry <@{slack_user_id}>, you don't have permission to {action_description}. "
        "Please request access from your DataHub administrator."
    )

    blocks: List[Dict] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":no_entry: {text}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "_This action requires specific privileges in DataHub._",
                }
            ],
        },
    ]

    return text, blocks


def graph_as_user(impersonation_urn: str) -> DataHubGraph:
    """
    Create a DataHub graph client that impersonates a specific user.

    This allows operations to be performed with the user's permissions,
    which is important for authorization and audit trails.

    Args:
        impersonation_urn: The URN of the DataHub user to impersonate

    Returns:
        DataHubGraph client configured with impersonation header
    """
    return DataHubGraph(
        graph.config.model_copy(
            update={"extra_headers": {"X-DataHub-Impersonated-Urn": impersonation_urn}},
            deep=True,
        )
    )


def graph_as_system() -> DataHubGraph:
    """
    Get the system-level DataHub graph client (no impersonation).

    Returns:
        DataHubGraph client with system-level permissions
    """
    return graph


def _get_slack_email_from_user_id(app: slack_bolt.App, user_id: str) -> Optional[str]:
    """
    Get email address for a Slack user.

    Separated for testability.

    Args:
        app: Slack app instance
        user_id: Slack user ID

    Returns:
        Email address if found, None otherwise
    """
    try:
        user_response = app.client.users_info(user=user_id)
        if user_response.get("ok"):
            return user_response["user"]["profile"].get("email")
        return None
    except Exception as e:
        logger.warning(f"Could not get email for Slack user {user_id}: {e}")
        return None


def _search_datahub_users_by_email(
    email: str, graph_client: Optional[DataHubGraph] = None
) -> List[str]:
    """
    Search for DataHub users by email address.

    Separated for testability.

    Args:
        email: Email address to search for
        graph_client: Optional DataHubGraph client (defaults to system graph)

    Returns:
        List of matching DataHub user URNs
    """
    client = graph_client or graph

    users = list(
        client.get_urns_by_filter(
            entity_types=[CorpUserUrn.ENTITY_TYPE],
            extraFilters=[
                {
                    "field": "email",
                    "value": email,
                    "condition": "EQUAL",
                }
            ],
        )
    )

    return users


def _search_datahub_users_by_slack_id(
    slack_user_id: str, graph_client: Optional[DataHubGraph] = None
) -> List[str]:
    """
    Search for DataHub users by Slack user ID.

    Separated for testability.

    Args:
        slack_user_id: Slack user ID (e.g., "U12345678")
        graph_client: Optional DataHubGraph client (defaults to system graph)

    Returns:
        List of matching DataHub user URNs
    """
    client = graph_client or graph

    # skip_cache=True bypasses GMS's CachingEntitySearchService so that
    # freshly-bound Slack users are found immediately after OAuth completion
    # rather than remaining invisible until the cache entry expires.
    users = list(
        client.get_urns_by_filter(
            entity_types=[CorpUserUrn.ENTITY_TYPE],
            extraFilters=[
                {
                    "field": "slackUserId",
                    "value": slack_user_id,
                    "condition": "EQUAL",
                }
            ],
            skip_cache=True,
        )
    )

    return users


def get_datahub_user_by_slack_id(slack_user_id: str) -> Optional[str]:
    """
    Look up a DataHub user URN by their Slack user ID.

    This uses the searchable slackUserId field that is populated when users
    bind their Slack account via OAuth in personal notification settings.

    Args:
        slack_user_id: Slack user ID (e.g., "U12345678")

    Returns:
        DataHub user URN if found, None otherwise
    """
    logger.debug(f"Looking up DataHub user by Slack ID: {slack_user_id}")

    try:
        users = _search_datahub_users_by_slack_id(slack_user_id)

        if users:
            user_urn = users[0]
            logger.debug(f"Found DataHub user {user_urn} for Slack ID {slack_user_id}")
            return user_urn
        else:
            logger.debug(f"No DataHub user found for Slack ID {slack_user_id}")
            return None
    except Exception as e:
        logger.error(f"Error looking up user by Slack ID {slack_user_id}: {e}")
        return None


def get_user_information(
    app: slack_bolt.App, user_id: str, require_oauth_binding: bool = False
) -> Tuple[str, Optional[str], List[str]]:
    """
    Get DataHub user information by Slack user ID.

    Behavior depends on require_oauth_binding flag:
    - If True: Only OAuth-bound users are resolved (strict mode)
    - If False: Try OAuth first, fall back to email lookup (migration mode)

    Args:
        app: Slack app instance
        user_id: Slack user ID
        require_oauth_binding: If True, only return OAuth-bound users

    Returns:
        Tuple of (email, user_urn, all_matching_user_urns)
    """
    logger.debug(
        f"[get_user_information] Starting lookup for Slack user_id={user_id}, "
        f"require_oauth_binding={require_oauth_binding}"
    )

    # First try OAuth-based lookup (preferred)
    user_urn_via_slack_id = get_datahub_user_by_slack_id(user_id)
    if user_urn_via_slack_id:
        email = _get_slack_email_from_user_id(app, user_id) or ""
        logger.debug(
            f"[get_user_information] Resolved via OAuth binding: "
            f"slack_user_id={user_id}, user_urn={user_urn_via_slack_id}"
        )
        return email, user_urn_via_slack_id, [user_urn_via_slack_id]

    # If OAuth binding is required, don't fall back to email lookup
    if require_oauth_binding:
        logger.debug(
            f"[get_user_information] OAuth binding required but not found for {user_id}"
        )
        return "", None, []

    # Fallback to email-based lookup (legacy method - only in migration mode)
    logger.debug(f"[get_user_information] Falling back to email lookup for {user_id}")

    fallback_email = _get_slack_email_from_user_id(app, user_id) or ""
    if not fallback_email:
        logger.warning(
            f"[get_user_information] Could not get email for Slack user {user_id}"
        )
        return "", None, []

    users = _search_datahub_users_by_email(fallback_email)
    logger.debug(
        f"[get_user_information] Email search for {user_id}: found {len(users)} users"
    )

    if not users:
        return fallback_email, None, []
    else:
        return fallback_email, users[0], users


def get_datahub_user(
    app: slack_bolt.App, user_id: str, require_oauth_binding: bool = False
) -> Optional[str]:
    """
    Get DataHub user URN for a Slack user.

    Args:
        app: Slack app instance
        user_id: Slack user ID
        require_oauth_binding: If True, only return OAuth-bound users

    Returns:
        DataHub user URN if found, None otherwise
    """
    _, user_urn, _ = get_user_information(app, user_id, require_oauth_binding)
    return user_urn


def get_subscription_urn(entity_urn: str, user_urn: str) -> Optional[str]:
    """
    Get subscription URN for a user and entity.

    Args:
        entity_urn: DataHub entity URN
        user_urn: DataHub user URN

    Returns:
        Subscription URN if found, None otherwise
    """
    impersonation_graph = graph_as_user(user_urn)
    response = impersonation_graph.execute_graphql(
        GET_SUBSCRIPTION_QUERY, {"input": {"entityUrn": entity_urn}}
    )
    logger.debug(f"get_subscription: {response}")
    return (response["getSubscription"].get("subscription") or {}).get(
        "subscriptionUrn"
    )
