from typing import Any, Dict, List, Optional, Union

from loguru import logger

from datahub_integrations.identity.identity_provider import Group, User
from datahub_integrations.notifications.constants import MAX_ACTOR_TAGS

# Cache for email to Teams user ID mapping
email_to_teams_user: Dict[str, Dict[str, Any]] = {}


def create_actors_tag_string(actors: List[Union[User, Group]]) -> str:
    """Create a Teams mention string for a list of actors (users and groups)."""
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
            actor_name_string = create_user_tag_string(actor)

        if actor_name_string:
            if index > 0:
                name_string += ", "
            name_string += actor_name_string

    # Add "and X others" if there are more actors than we displayed
    if num_actors > displayed_actors_count:
        additional_actors_count = num_actors - displayed_actors_count
        name_string += f" and {additional_actors_count} others"

    return name_string


def create_user_tag_string(user: User) -> Optional[str]:
    """Create a Teams mention string for a user."""
    fallback_display_name = user.get_resolved_display_name()

    # For Teams, we would need to resolve email to Teams user ID
    # This is a simplified implementation - in reality you'd need to:
    # 1. Use Microsoft Graph API to look up user by email
    # 2. Get their Teams user ID
    # 3. Use the Teams mention format: <at>DisplayName</at>

    if user.email:
        try:
            teams_user = get_teams_user_from_email(user.email)
            if teams_user:
                # Teams mention format is different from Slack
                # Format: <at>DisplayName</at> (in actual Teams cards)
                # For now, we'll use a placeholder format
                display_name = teams_user.get("displayName") or fallback_display_name
                return f"@{display_name}"
            else:
                logger.warning(
                    f"Skipping tagging user with email {user.email}. No corresponding Teams user found."
                )
        except Exception as e:
            logger.warning(
                f"Failed to resolve user with email {user.email} to Teams user: {e}. Using fallback name."
            )
    else:
        logger.warning(
            "Failed to resolve user to Teams user by email. No email found for user!"
        )

    # If we cannot resolve a proper tag, fallback to an untagged name
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


def get_teams_user_from_email(email: str) -> Optional[Dict[str, Any]]:
    """Look up a Teams user by email address."""
    if email in email_to_teams_user:
        return email_to_teams_user[email]

    # This is a placeholder implementation
    # In reality, you would:
    # 1. Use Microsoft Graph API to search for user by email
    # 2. Make a request to https://graph.microsoft.com/v1.0/users/{email}
    # 3. Extract user info and Teams-specific IDs
    # 4. Cache the result

    try:
        # Placeholder - in real implementation would call Graph API
        # For now, just return a basic user object based on email
        teams_user = {
            "id": f"teams_user_{email}",
            "displayName": email.split("@")[0].replace(".", " ").title(),
            "email": email,
        }

        # Cache the result
        email_to_teams_user[email] = teams_user
        return teams_user
    except Exception as e:
        logger.error(f"Failed to lookup Teams user by email {email}: {e}")
        return None


def extract_owners_strings(
    owner_urns: List[str],
    downstream_owner_urns: List[str],
    actors: Dict[str, Union[User, Group]],
) -> tuple[str, str]:
    """Extract owner strings for Teams notifications."""
    # Get owners
    owners_str = create_actors_tag_string(
        [
            actor
            for actor in (actors.get(urn) for urn in owner_urns if urn in actors)
            if actor is not None
        ]
    )

    # Deduplicate downstream owners while preserving order
    seen = set()
    deduplicated_downstream_owners = []
    for urn in downstream_owner_urns:
        actor = actors.get(urn)
        if actor is not None and urn not in seen:
            seen.add(urn)
            deduplicated_downstream_owners.append(actor)

    downstream_owners_str = create_actors_tag_string(deduplicated_downstream_owners)
    return owners_str, downstream_owners_str
