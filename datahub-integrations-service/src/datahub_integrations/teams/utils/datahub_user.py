"""
Teams user mapping utilities for DataHub integration.

This module provides functions to map Teams user IDs to DataHub user URNs
and handle user impersonation for GraphQL operations.
"""

import asyncio
import concurrent.futures
import logging
from typing import List, Optional, Tuple

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import CorpUserUrn

from datahub_integrations.app import graph
from datahub_integrations.teams.graph_api import GraphApiClient, GraphApiUser
from datahub_integrations.teams.teams import TeamsActivity


def _run_async_graph_api_lookup(
    graph_client: GraphApiClient, azure_user_id: str
) -> Optional[GraphApiUser]:
    """
    Run async Graph API lookup in a separate thread with proper event loop management.

    Args:
        graph_client: GraphApiClient instance
        azure_user_id: Azure AD user ID to lookup

    Returns:
        GraphApiUser object if found, None otherwise
    """

    def run_async_lookup() -> Optional[GraphApiUser]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(graph_client.get_user_by_id(azure_user_id))
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_async_lookup)
        return future.result(timeout=10)


def _get_graph_api_user_by_id(azure_user_id: str) -> Optional[GraphApiUser]:
    """
    Helper function to get Graph API user by Azure AD user ID.

    Args:
        azure_user_id: Azure AD user ID

    Returns:
        GraphApiUser object if found, None otherwise
    """
    try:
        # Use the Teams configuration system instead of environment variables directly
        from datahub_integrations.teams.config import teams_config
        from datahub_integrations.teams.graph_api import GraphApiClient

        # Get Teams configuration
        config = teams_config.get_config()
        if not config.app_details:
            logger.warning("Teams app details not configured. Cannot resolve user.")
            return None

        # Create Graph API client
        graph_client = GraphApiClient(config)

        # Run async Graph API call in sync context
        return _run_async_graph_api_lookup(graph_client, azure_user_id)

    except Exception as e:
        logger.error(f"Error getting Graph API user by ID {azure_user_id}: {e}")
        return None


logger = logging.getLogger(__name__)


def graph_as_user(impersonation_urn: str) -> DataHubGraph:
    """
    Create a DataHub graph client with user impersonation.

    Args:
        impersonation_urn: DataHub user URN to impersonate

    Returns:
        DataHubGraph client with impersonation headers
    """
    return DataHubGraph(
        graph.config.model_copy(
            update={"extra_headers": {"X-DataHub-Impersonated-Urn": impersonation_urn}},
            deep=True,
        )
    )


def graph_as_system() -> DataHubGraph:
    """
    Create a DataHub graph client with system user (no impersonation).

    Returns:
        DataHubGraph client with system user
    """
    return graph


def get_user_information_from_azure_id(
    azure_user_id: str,
) -> Tuple[str, Optional[str], List[str]]:
    """
    Map an Azure AD user ID to DataHub user information.

    This function first tries to search DataHub directly by azureUserId (if searchable),
    then falls back to the email-based approach.

    Args:
        azure_user_id: Azure AD user ID (e.g., "12345678-1234-1234-1234-123456789abc")

    Returns:
        Tuple of (email, user_urn, user_urns) where:
        - email: The user's email (extracted from Azure AD ID or "unknown")
        - user_urn: The first matching DataHub user URN (or None)
        - user_urns: List of all matching DataHub user URNs
    """
    try:
        # First, try to search DataHub directly by azureUserId (if searchable)
        azure_result = get_user_information_from_azure_user_id(azure_user_id)
        if azure_result[1] is not None:  # Found a user
            logger.info(
                f"Found DataHub user by azureUserId: {azure_user_id} -> {azure_result[1]}"
            )
            return azure_result

        # Fallback: Use Graph API to get user email from Azure AD user ID
        graph_user = _get_graph_api_user_by_id(azure_user_id)

        if graph_user and graph_user.mail:
            email = graph_user.mail
            logger.info(f"Resolved Azure AD user {azure_user_id} -> {email}")

            # Search DataHub for users with this email
            email_result = get_user_information_from_email(email)

            # If no DataHub user found with this email, return None
            if email_result[1] is None:  # No user_urn found
                logger.warning(f"No DataHub user found for email {email}")
                return email, None, []

            return email_result
        else:
            logger.warning(
                f"Could not resolve email for Azure AD user {azure_user_id}. Using system user."
            )
            return "unknown", None, []

    except Exception as e:
        logger.error(
            f"Error mapping Azure AD user {azure_user_id} to DataHub user: {e}"
        )
        return "unknown", None, []


def get_user_information_from_teams_id(
    teams_user_id: str,
) -> Tuple[str, Optional[str], List[str]]:
    """
    Map a Teams user ID to DataHub user information.

    This function uses the same hardcoded Azure AD user ID approach as incident actions
    to ensure consistent user impersonation across all Teams interactions.

    Args:
        teams_user_id: Teams user ID (currently unused, kept for compatibility)

    Returns:
        Tuple of (email, user_urn, user_urns) where:
        - email: The user's email
        - user_urn: The first matching DataHub user URN (or None)
        - user_urns: List of all matching DataHub user URNs
    """
    # This function is deprecated - use get_user_information_from_teams_activity instead
    # which properly extracts the Azure AD user ID from the Teams activity
    logger.warning(
        f"get_user_information_from_teams_id is deprecated. "
        f"Use get_user_information_from_teams_activity instead for Teams user {teams_user_id}"
    )

    # Return None values to indicate this function should not be used
    return "unknown", None, []


def get_user_information_from_teams_activity(
    activity: "TeamsActivity",
) -> Tuple[str, Optional[str], List[str]]:
    """
    Map a Teams activity to DataHub user information by extracting Azure AD user ID.

    This function properly extracts the Azure AD user ID from the Teams activity
    and maps it to the corresponding DataHub user.

    Args:
        activity: Teams Bot Framework activity containing user information

    Returns:
        Tuple of (email, user_urn, user_urns) where:
        - email: The user's email
        - user_urn: The first matching DataHub user URN (or None)
        - user_urns: List of all matching DataHub user URNs
    """
    try:
        # Extract Azure AD user ID from the activity
        azure_user_id = None

        if hasattr(activity, "from_property") and activity.from_property:
            # Try to get aadObjectId from the from_property
            if (
                hasattr(activity.from_property, "aad_object_id")
                and activity.from_property.aad_object_id
            ):
                azure_user_id = activity.from_property.aad_object_id
                logger.info(
                    f"Found Azure AD user ID in from_property.aad_object_id: {azure_user_id}"
                )
            # Try additional_properties as fallback
            elif (
                hasattr(activity.from_property, "additional_properties")
                and activity.from_property.additional_properties
            ):
                additional_props = activity.from_property.additional_properties
                if "aadObjectId" in additional_props:
                    azure_user_id = additional_props["aadObjectId"]
                    logger.info(
                        f"Found Azure AD user ID in additional_properties: {azure_user_id}"
                    )

            if not azure_user_id:
                logger.error(
                    f"Could not extract Azure AD user ID from Teams activity. "
                    f"Activity from_property: {activity.from_property if hasattr(activity, 'from_property') else 'None'}"
                )
                # Return None values to indicate user mapping failed
                return "unknown", None, []

        # Map Azure AD user ID to DataHub user
        if azure_user_id:
            return get_user_information_from_azure_id(azure_user_id)
        else:
            return "unknown", None, []

    except Exception as e:
        logger.error(f"Error extracting user information from Teams activity: {e}")
        # Return None values to indicate user mapping failed
        return "unknown", None, []


def get_datahub_user_from_teams_id(teams_user_id: str) -> Optional[str]:
    """
    Get DataHub user URN from Teams user ID.

    Args:
        teams_user_id: Teams user ID

    Returns:
        DataHub user URN or None if not found
    """
    _, user_urn, _ = get_user_information_from_teams_id(teams_user_id)
    return user_urn


def get_user_information_from_azure_user_id(
    azure_user_id: str,
) -> Tuple[str, Optional[str], List[str]]:
    """
    Map an Azure AD user ID to DataHub user information by searching the azureUserId field.

    This function searches DataHub users by their Teams settings azureUserId field,
    which should be searchable if the @Searchable annotation is properly configured.

    Args:
        azure_user_id: Azure AD user ID (e.g., "12345678-1234-1234-1234-123456789abc")

    Returns:
        Tuple of (email, user_urn, user_urns) where:
        - email: The user's email (from Graph API or "unknown")
        - user_urn: The first matching DataHub user URN (or None)
        - user_urns: List of all matching DataHub user URNs
    """
    try:
        logger.debug(f"Searching DataHub users by azureUserId: {azure_user_id}")

        # Use the existing graph instance to search by azureUserId field
        # This works because we added @Searchable annotation to the azureUserId field
        users = list(
            graph.get_urns_by_filter(
                entity_types=[CorpUserUrn.ENTITY_TYPE],
                extraFilters=[
                    {
                        "field": "azureUserId",
                        "value": azure_user_id,
                        "condition": "EQUAL",
                    }
                ],
            )
        )

        if not users:
            logger.debug(f"No DataHub users found with azureUserId: {azure_user_id}")
            return "unknown", None, []
        else:
            logger.debug(
                f"Found {len(users)} DataHub users with azureUserId: {azure_user_id}"
            )

            # Get the email for the first user (we'll use Graph API for consistency)
            try:
                graph_user = _get_graph_api_user_by_id(azure_user_id)
                email = graph_user.mail if graph_user and graph_user.mail else "unknown"
            except Exception as e:
                logger.debug(
                    f"Could not get email for azureUserId {azure_user_id}: {e}"
                )
                email = "unknown"

            return email, users[0], users

    except Exception as e:
        logger.error(
            f"Error searching DataHub users by azureUserId {azure_user_id}: {e}"
        )
        return "unknown", None, []


def get_user_information_from_email(email: str) -> Tuple[str, Optional[str], List[str]]:
    """
    Map an email address to DataHub user information.

    This function searches for users by email using the search SDK.
    It's used as a fallback when direct azureUserId search is not available.

    Args:
        email: User's email address

    Returns:
        Tuple of (email, user_urn, user_urns) where:
        - email: The provided email
        - user_urn: The first matching DataHub user URN (or None)
        - user_urns: List of all matching DataHub user URNs
    """
    try:
        logger.debug(f"Searching DataHub users by email: {email}")

        # Use the existing graph instance to search by email field
        # Note: This assumes the email field is searchable in the search index
        users = list(
            graph.get_urns_by_filter(
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

        if not users:
            logger.debug(f"No DataHub users found with email: {email}")
            return email, None, []
        else:
            logger.debug(f"Found {len(users)} DataHub users with email: {email}")
            return email, users[0], users

    except Exception as e:
        logger.error(f"Error searching DataHub users by email {email}: {e}")
        return email, None, []


def get_datahub_user_from_email(email: str) -> Optional[str]:
    """
    Get DataHub user URN from email address.

    Args:
        email: User's email address

    Returns:
        DataHub user URN or None if not found
    """
    _, user_urn, _ = get_user_information_from_email(email)
    return user_urn
