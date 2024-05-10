import json
from typing import Any, Dict, List, Optional, Union

from datahub.metadata.schema_classes import NotificationRequestClass
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from datahub_integrations.identity.identity_provider import (
    Group,
    IdentityProvider,
    User,
)

# Global Cache Storing Email Address to Slack Handle to reduce Queries to Slack.
email_to_slack_user: Dict[str, Any] = {}


def build_new_incident_message(
    request: NotificationRequestClass,
    identity_provider: IdentityProvider,
    slack_client: WebClient,
    base_url: str,
) -> str:

    request_parameters = request.message.parameters or {}
    owner_urns = deserialize_json_list(
        request_parameters.get("owners", "[]"),
    )
    downstream_owner_urns = deserialize_json_list(
        request_parameters.get("downstreamOwners", "[]")
    )

    all_actors = set(owner_urns + downstream_owner_urns)
    actors: Dict[str, Any] = {}
    try:
        actors = identity_provider.batch_get_actors(all_actors)
    except Exception:
        logger.exception(
            "Failed to resolve actors from identity provider. Skipping tagging them in Slack broadcast."
        )

    url = base_url + (
        request_parameters.get("entityPath") or ""
    )  # Ensure a string is returned even if the value is None
    entity_name = request_parameters.get("entityName", "")
    title = request_parameters.get("incidentTitle", "None")
    description = request_parameters.get("incidentDescription", "None")
    actor_name = (
        get_user_name(request_parameters.get("actorUrn", ""), identity_provider)
        if "actorUrn" in request_parameters
        else None
    )

    owners_str = create_actors_tag_string(
        slack_client,
        [actors[actor_urn] for actor_urn in actors if actor_urn in owner_urns],
    )
    downstream_owners_str = create_actors_tag_string(
        slack_client,
        [
            actors[actor_urn]
            for actor_urn in actors
            if actor_urn in downstream_owner_urns
        ],
    )

    return (
        f":warning: *New Incident Raised* \n\n"
        f"A new incident has been raised on asset <{url}|{entity_name}>{' by *%s*' % actor_name if actor_name else ''}.\n\n"
        f"*Incident Name*: {title}\n"
        f"*Incident Description*: {description}\n\n"
        f"*Asset Owners*: {owners_str if owners_str else 'None'}\n"
        f"*Downstream Asset Owners*: {downstream_owners_str if downstream_owners_str else 'None'}"
    )


def build_incident_status_change_message(
    request: NotificationRequestClass,
    identity_provider: IdentityProvider,
    slack_client: WebClient,
    base_url: str,
) -> str:
    request_parameters = request.message.parameters or {}
    owner_urns = deserialize_json_list(
        request_parameters.get("owners", "[]"),
    )
    downstream_owner_urns = deserialize_json_list(
        request_parameters.get("downstreamOwners", "[]")
    )

    all_actors = set(owner_urns + downstream_owner_urns)
    actors: Dict[str, Any] = {}
    try:
        actors = identity_provider.batch_get_actors(all_actors)
    except Exception:
        logger.exception(
            "Failed to resolve actors from identity provider. Skipping adding them to notification."
        )

    url = base_url + request_parameters.get("entityPath", "")
    entity_name = request_parameters.get("entityName", "")
    message = request_parameters.get("message", "None")
    title = request_parameters.get("incidentTitle", "None")
    description = request_parameters.get("incidentDescription", "None")
    prev_status = request_parameters.get("prevStatus", "")
    new_status = request_parameters.get("newStatus", "")
    actor_name = (
        get_user_name(request_parameters.get("actorUrn", ""), identity_provider)
        if "actorUrn" in request_parameters
        else None
    )

    owners_str = create_actors_tag_string(
        slack_client,
        [actors[actor_urn] for actor_urn in actors if actor_urn in owner_urns],
    )
    downstream_owners_str = create_actors_tag_string(
        slack_client,
        [
            actors[actor_urn]
            for actor_urn in actors
            if actor_urn in downstream_owner_urns
        ],
    )

    icon = ":white_check_mark:" if new_status == "RESOLVED" else ":warning:"
    return (
        f"{icon} *Incident Status Changed*\n\n"
        f"The status of incident *{title if title else 'None'}* on asset <{url}|{entity_name}> "
        f"has changed from *{prev_status}* to *{new_status}*{' by *%s*' % actor_name if actor_name else ''}.\n\n"
        f"*Message*: {message}\n\n"
        f"*Incident Name*: {title if title else 'None'}\n"
        f"*Incident Description*: {description if description else 'None'}\n\n"
        f"*Asset Owners*: {owners_str if owners_str else 'None'}\n"
        f"*Downstream Asset Owners*: {downstream_owners_str if downstream_owners_str else 'None'}"
    )


def create_actors_tag_string(
    slack_client: WebClient, actors: List[Union[User, Group]]
) -> str:
    name_string = ""
    for index, actor in enumerate(actors):
        if isinstance(actor, Group):
            # Handle Group Name Resolution
            actor_name_string = create_group_tag_string(actor)
        else:
            # Default to User Tag, then Name Resolution
            actor_name_string = create_user_tag_string(slack_client, actor)

        if actor_name_string:
            name_string += actor_name_string
            if index < len(actors) - 1:
                name_string += ", "

    return name_string.strip()


def create_user_tag_string(slack_client: WebClient, user: User) -> Optional[str]:
    fallback_display_name = user.get_resolved_display_name()

    if user.slack:
        return f"<@{user.slack}>"

    if user.email:
        try:
            slack_user = get_slack_user_from_email(slack_client, user.email)
            if slack_user:
                return f"<@{slack_user.get('id')}>"
            else:
                logger.warning(
                    f"Skipping tagging user with email {user.email} to tag string. No corresponding Slack user found."
                )
        except Exception:
            logger.exception(
                f"Caught exception while attempting to resolve user with email {user.email} to Slack user. Skipping adding user to tag string."
            )
    else:
        logger.error(
            "Failed to resolve user email to Slack user by email. No email found for user!"
        )
    # If we cannot resolve a proper tag, fallback to an untagged name!
    return fallback_display_name


def create_group_tag_string(group: Group) -> Optional[str]:
    fallback_display_name = group.get_resolved_display_name()
    if group.slack:
        return f"<@{group.slack}>"
    # If we cannot resolve a proper tag, fallback to an untagged group name!
    return fallback_display_name


def get_slack_user_from_email(
    slack_client: WebClient, email: str
) -> Optional[Dict[str, Any]]:
    if email in email_to_slack_user:
        return email_to_slack_user[email]
    else:
        try:
            response = get_slack_user_lookup_response_from_email(slack_client, email)
            if response["ok"]:
                slack_user = response["user"]
                email_to_slack_user[email] = slack_user  # Store in cache
                return slack_user
            else:
                logger.error(
                    f"Received API error while attempting to resolve a Slack user with email {email}. Error: {response['error']}"
                )
        except Exception:
            logger.exception(
                "Caught exception while attempting to lookup Slack user by email"
            )
    return None


def get_slack_user_lookup_response_from_email(
    slack_client: WebClient, email: str
) -> dict:
    try:
        response = slack_client.users_lookupByEmail(email=email)
        return response.data  # type: ignore
    except SlackApiError as e:
        error_message = e.response["error"]
        logger.error(f"Error retrieving Slack user by email: {error_message}")
        return {"ok": False, "error": error_message}


def get_user_name(
    user_urn_str: str, identity_provider: IdentityProvider
) -> Optional[str]:
    try:
        user = identity_provider.get_user(user_urn_str)
        return user.get_resolved_display_name() if user else None
    except Exception as e:
        raise RuntimeError(f"Invalid actor urn {user_urn_str} provided: {e}")


def deserialize_json_list(serialized_list: str) -> List[str]:
    try:
        # Deserialize the JSON-formatted string into a Python list
        deserialized_list = json.loads(serialized_list)
        return deserialized_list
    except json.JSONDecodeError:
        # Handle JSON decoding errors
        logger.exception("Error decoding JSON template parameter")
        return []
