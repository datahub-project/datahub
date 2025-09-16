import os
from typing import Dict, List, Optional, Union

from cachetools import TTLCache
from loguru import logger

from datahub_integrations.identity.identity_provider import Group, User
from datahub_integrations.notifications.constants import MAX_ACTOR_TAGS
from datahub_integrations.teams.config import TeamsConnection
from datahub_integrations.teams.graph_api import GraphApiClient, GraphApiUser

# TTL Configuration for Teams user cache
TEAMS_USER_CACHE_TTL_SECONDS_ENV_VAR = (
    "INTEGRATION_SERVICE_TEAMS_USER_CACHE_TTL_SECONDS"
)
TEAMS_USER_CACHE_TTL_SECONDS_VALUE: int = int(
    os.getenv(TEAMS_USER_CACHE_TTL_SECONDS_ENV_VAR) or "300"  # 5 minutes default
)
TEAMS_USER_CACHE_MAX_SIZE = 1000

# TTL-based cache for email to Teams user ID mapping
# This prevents unbounded growth and automatically expires entries after TTL
email_to_teams_user: TTLCache[str, GraphApiUser] = TTLCache(
    maxsize=TEAMS_USER_CACHE_MAX_SIZE, ttl=TEAMS_USER_CACHE_TTL_SECONDS_VALUE
)


async def create_actors_tag_string_async(
    actors: List[Union[User, Group]], teams_config: Optional[TeamsConnection] = None
) -> str:
    """
    Create a Teams mention string for a list of actors (users and groups) - async version.

    Args:
        actors: List of User and Group objects
        teams_config: Teams configuration for Graph API access

    Returns:
        Comma-separated string of actor mentions
    """
    name_string = ""
    num_actors = len(actors)

    displayed_actors_count = min(MAX_ACTOR_TAGS, num_actors)

    for index in range(displayed_actors_count):
        actor = actors[index]
        if isinstance(actor, Group):
            # Handle Group Name Resolution
            actor_name_string = create_group_tag_string(actor)
        else:
            # Default to User Tag, then Name Resolution
            actor_name_string = await create_user_tag_string_async(actor, teams_config)

        if actor_name_string:
            if index > 0:
                name_string += ", "
            name_string += actor_name_string

    # Add "and X others" if there are more actors than we displayed
    if num_actors > displayed_actors_count:
        additional_actors_count = num_actors - displayed_actors_count
        name_string += f" and {additional_actors_count} others"

    return name_string


def create_actors_tag_string(
    actors: List[Union[User, Group]], teams_config: Optional[TeamsConnection] = None
) -> str:
    """
    Create a Teams mention string for a list of actors (users and groups) - sync version.

    Args:
        actors: List of User and Group objects
        teams_config: Teams configuration for Graph API access

    Returns:
        Comma-separated string of actor mentions
    """
    name_string = ""
    num_actors = len(actors)

    displayed_actors_count = min(MAX_ACTOR_TAGS, num_actors)

    for index in range(displayed_actors_count):
        actor = actors[index]
        if isinstance(actor, Group):
            # Handle Group Name Resolution
            actor_name_string = create_group_tag_string(actor)
        else:
            # Default to User Tag, then Name Resolution
            actor_name_string = create_user_tag_string(actor, teams_config)

        if actor_name_string:
            if index > 0:
                name_string += ", "
            name_string += actor_name_string

    # Add "and X others" if there are more actors than we displayed
    if num_actors > displayed_actors_count:
        additional_actors_count = num_actors - displayed_actors_count
        name_string += f" and {additional_actors_count} others"

    return name_string


async def create_user_tag_string_async(
    user: User, teams_config: Optional[TeamsConnection] = None
) -> Optional[str]:
    """
    Create a display name string for a user (async version).

    Args:
        user: User object to create display name for
        teams_config: Teams configuration for Graph API access (unused, kept for compatibility)

    Returns:
        User display name
    """
    fallback_display_name = user.get_resolved_display_name()

    if user.email:
        try:
            teams_user = await get_teams_user_from_email_async(user.email, teams_config)
            if teams_user:
                # Use the Teams display name if available, otherwise fallback
                display_name = teams_user.displayName or fallback_display_name
                return display_name
            else:
                logger.debug(
                    f"No Teams user found for email {user.email}. Using fallback name."
                )
        except Exception as e:
            logger.debug(
                f"Failed to resolve user with email {user.email} to Teams user: {e}. Using fallback name."
            )
    else:
        logger.debug("No email found for user. Using fallback name.")

    # Return the fallback display name
    return fallback_display_name


def create_user_tag_string(
    user: User, teams_config: Optional[TeamsConnection] = None
) -> Optional[str]:
    """
    Create a display name string for a user (sync version).

    Args:
        user: User object to create display name for
        teams_config: Teams configuration for Graph API access (unused, kept for compatibility)

    Returns:
        User display name
    """
    fallback_display_name = user.get_resolved_display_name()

    if user.email:
        try:
            teams_user = get_teams_user_from_email(user.email, teams_config)
            if teams_user:
                # Use the Teams display name if available, otherwise fallback
                display_name = teams_user.displayName or fallback_display_name
                return display_name
            else:
                logger.debug(
                    f"No Teams user found for email {user.email}. Using fallback name."
                )
        except Exception as e:
            logger.debug(
                f"Failed to resolve user with email {user.email} to Teams user: {e}. Using fallback name."
            )
    else:
        logger.debug("No email found for user. Using fallback name.")

    # Return the fallback display name
    return fallback_display_name


def create_group_tag_string(group: Group) -> Optional[str]:
    """Create a Teams mention string for a group."""
    fallback_display_name = group.get_resolved_display_name()

    # For Teams groups, we would need to:
    # 1. Look up the group in Teams/Azure AD
    # 2. Get the Teams channel or group ID
    # 3. Use appropriate mention format

    # For now, return the display name without tagging since Teams group mentions
    # are more complex and require specific permissions and setup
    return fallback_display_name


async def get_teams_user_from_email_async(
    email: str, teams_config: Optional[TeamsConnection] = None
) -> Optional[GraphApiUser]:
    """
    Look up a Teams user by email address using Microsoft Graph API (async version).

    Args:
        email: User's email address
        teams_config: Teams configuration for Graph API access

    Returns:
        GraphApiUser object if found, None otherwise
    """
    if email in email_to_teams_user:
        return email_to_teams_user[email]

    if not teams_config:
        logger.warning("Teams configuration not provided for user lookup")
        return None

    try:
        # Create Graph API client
        graph_client = GraphApiClient(teams_config)

        # Use Graph API to look up user by email
        teams_user = await graph_client.get_user_by_email(email)

        if teams_user:
            # Cache the result
            email_to_teams_user[email] = teams_user
            logger.debug(
                f"Cached Teams user for email {email}: {teams_user.displayName}"
            )
            return teams_user
        else:
            logger.debug(f"No Teams user found for email: {email}")
            return None

    except Exception as e:
        logger.error(f"Failed to lookup Teams user by email {email}: {e}")
        return None


def get_teams_user_from_email(
    email: str, teams_config: Optional[TeamsConnection] = None
) -> Optional[GraphApiUser]:
    """
    Look up a Teams user by email address using Microsoft Graph API (sync version).

    This version uses a thread pool to run async Graph API calls from sync contexts.
    For better performance in async contexts, use the async version.

    Args:
        email: User's email address
        teams_config: Teams configuration for Graph API access

    Returns:
        GraphApiUser object if found, None otherwise
    """
    if email in email_to_teams_user:
        return email_to_teams_user[email]

    if not teams_config:
        logger.warning("Teams configuration not provided for user lookup")
        return None

    try:
        # Use Graph API to look up user by email
        import asyncio
        import concurrent.futures

        # Create Graph API client
        graph_client = GraphApiClient(teams_config)

        # Always use thread pool approach to avoid async/sync context issues
        def run_async_lookup() -> Optional[GraphApiUser]:
            # Create a new event loop in this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(graph_client.get_user_by_email(email))
            finally:
                loop.close()

        # Run the async lookup in a separate thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_async_lookup)
            teams_user = future.result(timeout=10)  # 10 second timeout

        if teams_user:
            # Cache the result
            email_to_teams_user[email] = teams_user
            logger.debug(
                f"Cached Teams user for email {email}: {teams_user.displayName}"
            )
            return teams_user
        else:
            logger.debug(f"No Teams user found for email: {email}")
            return None

    except Exception as e:
        logger.error(f"Failed to lookup Teams user by email {email}: {e}")
        return None


async def extract_owners_strings_async(
    owner_urns: List[str],
    downstream_owner_urns: List[str],
    actors: Dict[str, Union[User, Group]],
    teams_config: Optional[TeamsConnection] = None,
) -> tuple[str, str]:
    """
    Extract owner strings for Teams notifications (async version).

    Args:
        owner_urns: List of owner URNs
        downstream_owner_urns: List of downstream owner URNs
        actors: Dictionary mapping URNs to User/Group objects
        teams_config: Teams configuration for Graph API access

    Returns:
        Tuple of (owners_str, downstream_owners_str)
    """
    # Get owners
    owners_str = await create_actors_tag_string_async(
        [
            actor
            for actor in (actors.get(urn) for urn in owner_urns if urn in actors)
            if actor is not None
        ],
        teams_config,
    )

    # Deduplicate downstream owners while preserving order
    seen = set()
    deduplicated_downstream_owners = []
    for urn in downstream_owner_urns:
        actor = actors.get(urn)
        if actor is not None and urn not in seen:
            seen.add(urn)
            deduplicated_downstream_owners.append(actor)

    downstream_owners_str = await create_actors_tag_string_async(
        deduplicated_downstream_owners, teams_config
    )
    return owners_str, downstream_owners_str


def extract_owners_strings(
    owner_urns: List[str],
    downstream_owner_urns: List[str],
    actors: Dict[str, Union[User, Group]],
    teams_config: Optional[TeamsConnection] = None,
) -> tuple[str, str]:
    """
    Extract owner strings for Teams notifications (sync version).

    Args:
        owner_urns: List of owner URNs
        downstream_owner_urns: List of downstream owner URNs
        actors: Dictionary mapping URNs to User/Group objects
        teams_config: Teams configuration for Graph API access

    Returns:
        Tuple of (owners_str, downstream_owners_str)
    """
    # Get owners
    owners_str = create_actors_tag_string(
        [
            actor
            for actor in (actors.get(urn) for urn in owner_urns if urn in actors)
            if actor is not None
        ],
        teams_config,
    )

    # Deduplicate downstream owners while preserving order
    seen = set()
    deduplicated_downstream_owners = []
    for urn in downstream_owner_urns:
        actor = actors.get(urn)
        if actor is not None and urn not in seen:
            seen.add(urn)
            deduplicated_downstream_owners.append(actor)

    downstream_owners_str = create_actors_tag_string(
        deduplicated_downstream_owners, teams_config
    )
    return owners_str, downstream_owners_str
