import dataclasses
import json
from typing import Any, Dict, List, Optional, Tuple, Union

from datahub.metadata.schema_classes import NotificationRequestClass
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from datahub_integrations.identity.identity_provider import (
    Group,
    IdentityProvider,
    User,
)
from datahub_integrations.notifications.constants import (
    ACTIVE_INCIDENT_COLOR,
    DATAHUB_SYSTEM_ACTOR,
    INCIDENT_ADVANCED_ACTIONS_ENABLED,
    INCIDENT_STATUS_ACTIVE,
    INCIDENT_STATUS_RESOLVED,
    MAX_ACTOR_TAGS,
    RESOLVED_INCIDENT_COLOR,
)
from datahub_integrations.slack.context import IncidentContext, IncidentSelectOption

# Global Cache Storing Email Address to Slack Handle to reduce Queries to Slack.
email_to_slack_user: Dict[str, Any] = {}


priority_select_options: List[Dict[str, Any]] = [
    {"text": {"type": "plain_text", "text": "Critical"}, "value": "CRITICAL"},
    {"text": {"type": "plain_text", "text": "High"}, "value": "HIGH"},
    {"text": {"type": "plain_text", "text": "Medium"}, "value": "MEDIUM"},
    {"text": {"type": "plain_text", "text": "Low"}, "value": "LOW"},
]

stage_select_options: List[Dict[str, Any]] = [
    {
        "text": {"type": "plain_text", "text": "No Action Required"},
        "value": "NO_ACTION_REQUIRED",
    },
    {"text": {"type": "plain_text", "text": "Triage"}, "value": "TRIAGE"},
    {"text": {"type": "plain_text", "text": "Investigation"}, "value": "INVESTIGATION"},
    {
        "text": {"type": "plain_text", "text": "Work in Progress"},
        "value": "WORK_IN_PROGRESS",
    },
    {"text": {"type": "plain_text", "text": "Fixed"}, "value": "FIXED"},
]


def get_stage_select_options(context: IncidentContext) -> List[Any]:
    # Inject the Incident Context into each object, required to wire data back from Slack.
    final_options = []
    for option in stage_select_options:
        option_value = IncidentSelectOption(option["value"], context)
        final_option = {
            "text": option["text"],
            "value": json.dumps(dataclasses.asdict(option_value)),
        }
        final_options.append(final_option)
    return final_options


def get_priority_select_options(context: IncidentContext) -> List[Any]:
    # Inject the Incident Context into each object, required to wire data back from Slack.
    final_options = []
    for option in priority_select_options:
        option_value = IncidentSelectOption(option["value"], context)
        final_option = {
            "text": option["text"],
            "value": json.dumps(dataclasses.asdict(option_value)),
        }
        final_options.append(final_option)
    return final_options


def map_priority(raw_priority: Any | None) -> Optional[str]:
    if raw_priority is None:
        return None
    priority_mapping = {"0": "CRITICAL", "1": "HIGH", "2": "MEDIUM", "3": "LOW"}
    priority = priority_mapping.get(raw_priority)
    if priority is None:
        logger.warning(
            f"Found unrecognized incident priority {raw_priority}. Displaying NONE!"
        )
    return priority


def get_initial_priority_option(
    raw_priority: Any | None, context: IncidentContext
) -> Optional[Any]:
    final_options = get_priority_select_options(context)
    mapped_priority = map_priority(raw_priority)
    for index, option in enumerate(priority_select_options):
        if option["value"] == mapped_priority:
            return final_options[index]
    return None


def get_initial_stage_option(
    raw_stage: Any | None, context: IncidentContext
) -> Optional[Any]:
    final_options = get_stage_select_options(context)
    for index, option in enumerate(stage_select_options):
        if option["value"] == raw_stage:
            return final_options[index]
    return None


def map_incident_type(raw_type: str) -> str:
    type_mapping = {
        "FRESHNESS": "Freshness",
        "OPERATIONAL": "Operational",
        "DATA_SCHEMA": "Schema",
        "VOLUME": "Volume",
        "FIELD": "Column",
        "SQL": "Custom SQL",
    }
    return type_mapping.get(
        raw_type, raw_type
    )  # Return original if not found (may be custom)


def map_assertion_type(raw_type: Any | None) -> Any | None:
    type_mapping = {
        "FRESHNESS": "Freshness",
        "DATA_SCHEMA": "Schema",
        "VOLUME": "Volume",
        "FIELD": "Column",
        "SQL": "Custom SQL",
    }
    return (
        type_mapping.get(raw_type) if raw_type else raw_type
    )  # Return original if not found


def create_incident_attachment(
    title: Any | None,
    description: Any | None,
    owners_str: str,
    downstream_owners_str: str,
    maybe_downstream_asset_count: Any | None,
    final_type: Any | None,
    initial_priority_option: Optional[Dict[str, Any]],
    initial_stage_option: Optional[Dict[str, Any]],
    action_buttons: List[Dict[str, Any]],
    color: str,
    context: IncidentContext,
) -> List[Dict[str, Any]]:
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*"}},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Reason*\n{description}"},
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Asset Owners*\n{owners_str or 'None'}",
                },
                {"type": "mrkdwn", "text": f"*Category*\n`{final_type}`"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Impacted Owners*\n{downstream_owners_str or 'None'}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Impacted Asset Count*\n{maybe_downstream_asset_count or 'None'}",
                },
            ],
        },
        {"type": "actions", "elements": action_buttons},
    ]

    if INCIDENT_ADVANCED_ACTIONS_ENABLED == "true":
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "static_select",
                        "placeholder": {"type": "plain_text", "text": "Priority"},
                        "action_id": "select_incident_priority",
                        "options": get_priority_select_options(context),
                        **(
                            {"initial_option": initial_priority_option}
                            if initial_priority_option
                            else {}
                        ),
                    },
                    {
                        "type": "static_select",
                        "placeholder": {"type": "plain_text", "text": "Stage"},
                        "action_id": "select_incident_stage",
                        "options": get_stage_select_options(context),
                        **(
                            {"initial_option": initial_stage_option}
                            if initial_stage_option
                            else {}
                        ),
                    },
                ],
            }
        )

    return [{"color": color, "blocks": blocks}]


def get_owner_tag_strings(
    request_parameters: Dict[str, Any],
    identity_provider: IdentityProvider,
    slack_client: WebClient,
) -> Tuple[str, str]:
    owner_urns = deserialize_json_list(request_parameters.get("owners", "[]"))
    downstream_owner_urns = deserialize_json_list(
        request_parameters.get("downstreamOwners", "[]")
    )
    all_actors = set(owner_urns + downstream_owner_urns)
    try:
        actors = identity_provider.batch_get_actors(all_actors)
    except Exception:
        logger.exception("Failed to resolve actors from identity provider.")
        actors = {}
    owners_str = create_actors_tag_string(
        slack_client,
        [
            actor
            for actor in (actors.get(urn) for urn in owner_urns if urn in actors)
            if actor is not None
        ],
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
        slack_client, deduplicated_downstream_owners
    )
    return owners_str, downstream_owners_str


def extract_incident_details(
    request_parameters: Dict[str, Any],
    base_url: str,
    identity_provider: IdentityProvider,
) -> Dict[str, Any]:
    # Ensuring default values for potentially missing parameters to prevent type errors
    url = base_url + (request_parameters.get("entityPath") or "")
    # URL encode the path if necessary (not shown here for brevity)

    assertion_urn = request_parameters.get("assertionUrn")
    assertion_url = (
        f"{url}/Validation/Assertions?assertion_urn={assertion_urn}"
        if assertion_urn
        else None
    )

    actor_urn = request_parameters.get("actorUrn", "")
    actor_name = (
        get_user_name(actor_urn, identity_provider)
        if actor_urn and actor_urn != DATAHUB_SYSTEM_ACTOR
        else None
    )

    incident_details = {
        "urn": request_parameters.get(
            "incidentUrn", ""
        ),  # Default to empty string if None
        "title": request_parameters.get("incidentTitle", "None"),
        "description": request_parameters.get("incidentDescription", "None"),
        "type": map_incident_type(request_parameters.get("incidentType", "None")),
        "url": f"{url}/Incidents",
        "entity_name": request_parameters.get("entityName"),
        "entity_platform": request_parameters.get("entityPlatform"),
        "entity_type": request_parameters.get("entityType"),
        "assertion_description": request_parameters.get("assertionDescription"),
        "assertion_type": map_assertion_type(request_parameters.get("assertionType")),
        "assertion_url": assertion_url,
        "is_smart_assertion": request_parameters.get("assertionSourceType")
        == "INFERRED",
        "is_assertion_incident": bool(
            assertion_urn and request_parameters.get("assertionDescription")
        ),
        "downstream_asset_count": request_parameters.get("downstreamAssetCount"),
        "priority": request_parameters.get("incidentPriority"),
        "stage": request_parameters.get("incidentStage"),
        "actor_name": actor_name,
        "prev_status": request_parameters.get("prevStatus"),
        "new_status": request_parameters.get("newStatus"),
        "message": request_parameters.get("message", "None"),
    }

    assertion_prefix = (
        "Smart"
        if incident_details.get("is_smart_assertion")
        else incident_details.get("assertion_type") or None
    )

    final_incident_title = (
        f"{assertion_prefix + ' ' if assertion_prefix else ''}Assertion <{incident_details.get('assertion_url')}|{incident_details.get('assertion_description')}> has failed"
        if incident_details["is_smart_assertion"]
        or (
            incident_details["assertion_url"]
            and incident_details["assertion_description"]
        )
        else incident_details["title"]
    )
    incident_details["title"] = final_incident_title

    return incident_details


def build_incident_message(
    request: NotificationRequestClass,
    identity_provider: IdentityProvider,
    slack_client: WebClient,
    base_url: str,
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Extract parameters or set defaults if none provided
    request_parameters = request.message.parameters or {}

    # Get owners and downstream owners' strings from their URNs
    owners_str, downstream_owners_str = get_owner_tag_strings(
        request_parameters, identity_provider, slack_client
    )

    # Extract incident details
    incident_details = extract_incident_details(
        request_parameters, base_url, identity_provider
    )

    # Ensure URN is a string, even if None (fallback to empty string)
    urn = incident_details.get("urn", "")
    stage = incident_details.get("stage")
    status = incident_details.get("new_status")

    incident_context = IncidentContext(urn=urn, stage=stage)

    # Get initial priority and stage options, handling potentially missing data
    initial_priority_option = get_initial_priority_option(
        incident_details.get("priority"),
        incident_context,
    )
    initial_stage_option = get_initial_stage_option(
        incident_details.get("stage"),
        incident_context,
    )

    # Mark as resolved button
    mark_as_resolved = {
        "type": "button",
        "text": {"type": "plain_text", "text": "Mark as Resolved"},
        "style": "primary",
        "value": json.dumps(dataclasses.asdict(incident_context)),
        "action_id": "resolve_incident",
    }

    # Mark as reopened button
    mark_as_reopened = {
        "type": "button",
        "text": {"type": "plain_text", "text": "Reopen Incident"},
        "style": "primary",
        "value": json.dumps(dataclasses.asdict(incident_context)),
        "action_id": "reopen_incident",
    }

    # Define action buttons for the Slack message
    action_buttons: List[Dict[str, Any]] = [
        mark_as_resolved if status == "ACTIVE" else mark_as_reopened,
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "View Details"},
            "url": incident_details.get("url"),
            "action_id": "external_redirect",
        },
    ]

    status_color = (
        ACTIVE_INCIDENT_COLOR if status == "ACTIVE" else RESOLVED_INCIDENT_COLOR
    )

    # Create incident attachment for the Slack message
    attachments = create_incident_attachment(
        title=incident_details.get("title"),
        description=incident_details.get("description"),
        owners_str=owners_str,
        downstream_owners_str=downstream_owners_str,
        maybe_downstream_asset_count=incident_details.get("downstream_asset_count"),
        final_type=incident_details.get("type"),  # Default to empty string if None
        initial_priority_option=initial_priority_option,
        initial_stage_option=initial_stage_option,
        action_buttons=action_buttons,
        color=status_color,
        context=incident_context,
    )

    # Prepare the textual content and structured blocks for the Slack message
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":warning: *New Data Incident*\nAn incident has been raised on *{incident_details.get('entity_platform', '') + ' ' if incident_details.get('entity_platform') else ''}{incident_details.get('entity_type', '') + ' ' if incident_details.get('entity_type') else ''}<{incident_details.get('url')}|{incident_details.get('entity_name')}>*{' by *' + incident_details.get('actor_name', '') + '*' if incident_details.get('actor_name') else ''}.",
            },
        },
    ]

    text = (
        f":warning: *New Data Incident* \n\n"
        f"An incident has been raised on asset <{incident_details.get('url')}|{incident_details.get('entity_name')}>{' by *' + incident_details.get('actor_name', '') + '*' if incident_details.get('actor_name') else ''}.\n\n"
        f"*Incident Name*: {incident_details.get('title')}\n"
        f"*Incident Description*: {incident_details.get('description')}\n\n"
        f"*Asset Owners*: {owners_str if owners_str else 'None'}\n"
        f"*Impacted Asset Owners*: {downstream_owners_str if downstream_owners_str else 'None'}"
    )

    return (text, blocks, attachments)


def build_incident_status_change_message(
    request: NotificationRequestClass,
    identity_provider: IdentityProvider,
    slack_client: WebClient,
    base_url: str,
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Safe extraction of parameters from the possibly None `parameters` field
    request_parameters = request.message.parameters or {}

    # Safely obtain the new status, with a default to None if not specified
    new_status = request_parameters.get("newStatus")

    # Handle different incident statuses robustly with explicit checks
    if new_status == INCIDENT_STATUS_RESOLVED:
        return build_incident_resolved_message(
            request, identity_provider, slack_client, base_url
        )
    elif new_status == INCIDENT_STATUS_ACTIVE:
        return build_incident_reopened_message(
            request, identity_provider, slack_client, base_url
        )
    else:
        # This handles cases where new_status is neither RESOLVED nor ACTIVE, including None
        raise ValueError(f"Unrecognized incident status {new_status} provided!")


def build_incident_resolved_message(
    request: NotificationRequestClass,
    identity_provider: IdentityProvider,
    slack_client: WebClient,
    base_url: str,
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Extract parameters safely or default to an empty dictionary
    request_parameters = request.message.parameters or {}

    # Get the tag strings for owners and downstream owners
    owners_str, downstream_owners_str = get_owner_tag_strings(
        request_parameters, identity_provider, slack_client
    )

    # Extract detailed incident data, considering handling of potential None values
    incident_details = extract_incident_details(
        request_parameters, base_url, identity_provider
    )

    # Ensure the urn is a string to avoid any type errors, default to an empty string if None
    urn = incident_details.get("urn", "")
    stage = incident_details.get("stage")
    incident_context = IncidentContext(urn=urn, stage=stage)

    # Get the initial options for priority and stage, handling None safely
    initial_priority_option = get_initial_priority_option(
        incident_details.get("priority"),
        incident_context,
    )
    initial_stage_option = get_initial_stage_option(
        incident_details.get("stage"),
        incident_context,
    )

    # Define action buttons for the Slack message
    action_buttons: List[Dict[str, Any]] = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Reopen Incident"},
            "style": "primary",
            "value": json.dumps(dataclasses.asdict(incident_context)),
            "action_id": "reopen_incident",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "View Details"},
            "url": incident_details.get("url"),
            "action_id": "external_redirect",
        },
    ]

    # Create the incident attachment, handling type safely
    attachments = create_incident_attachment(
        title=incident_details.get("title"),
        description=incident_details.get("description"),
        owners_str=owners_str,
        downstream_owners_str=downstream_owners_str,
        maybe_downstream_asset_count=incident_details.get("downstream_asset_count"),
        final_type=incident_details.get("type"),
        initial_priority_option=initial_priority_option,
        initial_stage_option=initial_stage_option,
        action_buttons=action_buttons,
        color=RESOLVED_INCIDENT_COLOR,
        context=incident_context,
    )

    # Compose the message summary with safe checks
    summary = f":white_check_mark: *Data Incident Resolved*\nAn incident has been resolved for *{incident_details.get('entity_platform', '') + ' ' if incident_details.get('entity_platform') else ''}{incident_details.get('entity_type', '') + ' ' if incident_details.get('entity_type') else ''}<{incident_details.get('url')}|{incident_details.get('entity_name')}>*{' by *' + incident_details.get('actor_name', '') + '*' if incident_details.get('actor_name') else ''}."

    # Optionally append resolution note if provided
    has_message = (
        incident_details.get("message") and incident_details.get("message") != ""
    )

    # Structured block for Slack layout
    blocks: List[Dict[str, Any]] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": summary,
            },
        },
    ]

    if has_message:
        blocks.append(
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f">>> *Note*\n{incident_details.get('message')}",
                    },
                ],
            }
        )

        blocks.append({"type": "divider"})

    # Plain text for notifications or logs
    text = (
        f":white_check_mark: *Data Incident Resolved*\n\n"
        f"Incident *{incident_details.get('title') if incident_details.get('title') else 'None'}* on asset <{incident_details.get('url')}|{incident_details.get('entity_name')}> has been resolved{' by *' + incident_details.get('actor_name', '') + '*' if incident_details.get('actor_name') else ''}.\n\n"
        f"*Note*: {incident_details.get('message')}\n\n"
        f"*Asset Owners*: {owners_str if owners_str else 'None'}\n"
        f"*Impacted Asset Owners*: {downstream_owners_str if downstream_owners_str else 'None'}"
    )

    return (text, blocks, attachments)


def build_incident_reopened_message(
    request: NotificationRequestClass,
    identity_provider: IdentityProvider,
    slack_client: WebClient,
    base_url: str,
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Extract parameters safely or default to an empty dictionary if None provided
    request_parameters = request.message.parameters or {}

    # Retrieve the owners and downstream owners' strings from their URNs
    owners_str, downstream_owners_str = get_owner_tag_strings(
        request_parameters, identity_provider, slack_client
    )

    # Extract incident details with appropriate handling for potential None values
    incident_details = extract_incident_details(
        request_parameters, base_url, identity_provider
    )

    # Ensure the urn is a string and not None, defaulting to an empty string if necessary
    urn = incident_details.get("urn", "")
    stage = incident_details.get("stage")
    incident_context = IncidentContext(urn=urn, stage=stage)

    # Obtain the initial options for priority and stage, defaulting to None safely
    initial_priority_option = get_initial_priority_option(
        incident_details.get("priority"),
        incident_context,
    )
    initial_stage_option = get_initial_stage_option(
        incident_details.get("stage"),
        incident_context,
    )

    # Define the action buttons for the Slack message
    action_buttons: List[Dict[str, Any]] = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Resolve Incident"},
            "style": "primary",
            "value": json.dumps(dataclasses.asdict(incident_context)),
            "action_id": "resolve_incident",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "View Details"},
            "url": incident_details.get("url"),
            "action_id": "external_redirect",
        },
    ]

    # Create the incident attachment with careful handling of type consistency
    attachments = create_incident_attachment(
        title=incident_details.get("title"),
        description=incident_details.get("description"),
        owners_str=owners_str,
        downstream_owners_str=downstream_owners_str,
        maybe_downstream_asset_count=incident_details.get("downstream_asset_count"),
        final_type=incident_details.get("type"),
        initial_priority_option=initial_priority_option,
        initial_stage_option=initial_stage_option,
        action_buttons=action_buttons,
        color=ACTIVE_INCIDENT_COLOR,
        context=incident_context,
    )

    # Compose the textual content and structured blocks for the Slack message
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":warning: *Data Incident Reopened*\nAn incident has been reopened for *{incident_details.get('entity_platform', '') + ' ' if incident_details.get('entity_platform') else ''}{incident_details.get('entity_type', '') + ' ' if incident_details.get('entity_type') else ''}<{incident_details.get('url')}|{incident_details.get('entity_name')}>*{' by *' + incident_details.get('actor_name', '') + '*' if incident_details.get('actor_name') else ''}.",
            },
        },
    ]

    text = (
        f":warning: *Data Incident Reopened*\n\n"
        f"Incident *{incident_details.get('title') if incident_details.get('title') else 'None'}* on asset <{incident_details.get('url')}|{incident_details.get('entity_name')}> has been reopened{' by *' + incident_details.get('actor_name', '') + '*' if incident_details.get('actor_name') else ''}.\n\n"
        f"*Asset Owners*: {owners_str if owners_str else 'None'}\n"
        f"*Impacted Asset Owners*: {downstream_owners_str if downstream_owners_str else 'None'}"
    )

    return (text, blocks, attachments)


def build_compliance_form_publish_parameters(
    request: NotificationRequestClass, base_url: str
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    if request.message.parameters is None:
        raise ValueError(
            "Parameters are required for compliance form publish notifications."
        )

    form_name = request.message.parameters.get("formName", "")
    form_details = request.message.parameters.get("formDetails", None)

    # Prepare the textual content and structured blocks for the Slack message
    blocks: List[Dict[str, Any]] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Initiative:*\n{form_name}",
            },
        },
    ]

    if form_details is not None:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Details:*\n{form_details}",
                },
            }
        )

    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "primary",
                    "text": {"type": "plain_text", "text": "Complete Tasks"},
                    "url": f"{base_url}/requests/requests",
                    "action_id": "external_redirect",
                },
            ],
        },
    )

    text = f":memo: Action Required: You have new data compliance tasks to complete for initiative: {form_name}"

    return (text, [], [{"color": "#533FD1", "blocks": blocks}])


def create_actors_tag_string(
    slack_client: WebClient, actors: List[Union[User, Group]]
) -> str:
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
            actor_name_string = create_user_tag_string(slack_client, actor)

        if actor_name_string:
            if index > 0:
                name_string += ", "
            name_string += actor_name_string

    if num_actors > MAX_ACTOR_TAGS:
        more_count = num_actors - MAX_ACTOR_TAGS
        name_string += f", + {more_count} more"

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
        raise RuntimeError(f"Invalid actor urn {user_urn_str} provided: {e}") from e


def deserialize_json_list(serialized_list: str) -> List[str]:
    try:
        # Deserialize the JSON-formatted string into a Python list
        deserialized_list = json.loads(serialized_list)
        return deserialized_list
    except json.JSONDecodeError:
        # Handle JSON decoding errors
        logger.exception("Error decoding JSON template parameter")
        return []
