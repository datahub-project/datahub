from datetime import datetime
from typing import Dict, Optional
from urllib.parse import quote

from datahub.metadata.schema_classes import (
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)

from datahub_integrations.notifications.constants import NON_INGESTION_RUN_ID


def build_new_incident_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError("Parameters are required for new incident notifications.")

    actor_name = get_actor_name(request.message.parameters.get("actorName"))
    entity_path = request.message.parameters.get("entityPath")
    entity_url = f"{base_url}{entity_path}"
    details_url = f"{base_url}{entity_path}/Incidents"
    entity_name = request.message.parameters.get("entityName", "")
    entity_platform = request.message.parameters.get("entityPlatform", "")
    entity_type = request.message.parameters.get("entityType", "")
    title = request.message.parameters.get("incidentTitle")
    description = request.message.parameters.get("incidentDescription")

    entity_title = build_entity_title(entity_platform, entity_type, entity_name)

    subject = (
        f"A new incident has been raised on {entity_title}"
        f"{' by ' + actor_name if actor_name else ''}."
    )

    incident_message = (
        f"A new incident has been raised on <b>{entity_title}</b>"
        f"{' by ' + actor_name if actor_name else ''}."
    )

    details_message = f"<br><br><b>Incident Name</b>: {title if title else 'None'}<br><b>Incident Description</b>: {description if description else 'None'}<br><br>"

    message = incident_message + details_message

    return {
        "subject": subject,
        "message": message,
        "entityName": entity_name,
        "detailsUrl": details_url,
        "entityUrl": entity_url,
        "baseUrl": base_url,
    }


def build_incident_status_change_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError(
            "Parameters are required for incident status change notifications."
        )

    actor_name = get_actor_name(request.message.parameters.get("actorName"))
    entity_path = request.message.parameters.get("entityPath")
    entity_platform = request.message.parameters.get("entityPlatform", "")
    entity_type = request.message.parameters.get("entityType", "")
    entity_url = f"{base_url}{entity_path}"
    details_url = f"{base_url}{entity_path}/Incidents"
    entity_name = request.message.parameters.get("entityName", "")
    title = request.message.parameters.get("incidentTitle")
    description = request.message.parameters.get("incidentDescription")
    prev_status = request.message.parameters.get("prevStatus")
    new_status = request.message.parameters.get("newStatus")

    entity_title = build_entity_title(entity_platform, entity_type, entity_name)

    subject = (
        f"The status of incident on {entity_title} has been changed from {prev_status} to {new_status}"
        f"{' by ' + actor_name if actor_name else ''}."
    )

    incident_message = (
        f"The status of incident on <b>{entity_title}</b> has been changed from {prev_status} to {new_status}"
        f"{' by ' + actor_name if actor_name else ''}."
    )

    details_message = f"<br><br><b>Incident Name</b>: {title if title else 'None'}<br><b>Incident Description</b>: {description if description else 'None'}<br><br>"

    message = incident_message + details_message

    return {
        "subject": subject,
        "message": message,
        "entityName": entity_name,
        "detailsUrl": details_url,
        "entityUrl": entity_url,
        "baseUrl": base_url,
    }


def build_new_proposal_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError("Parameters are required for new proposal notifications.")

    # Assuming get_user_name is a function you have defined to extract the user name from actorUrn
    actor_name = get_actor_name(request.message.parameters.get("actorName"))
    entity_name = request.message.parameters.get("entityName", "")
    entity_path = request.message.parameters.get("entityPath")
    entity_url = f"{base_url}{entity_path}"
    entity_platform = request.message.parameters.get("entityPlatform", "")
    entity_type = request.message.parameters.get("entityType", "")
    modifier_type = (request.message.parameters.get("modifierType") or "").capitalize()
    modifier_name = request.message.parameters.get("modifierName", "")

    maybe_sub_resource_type = request.message.parameters.get("subResourceType")
    maybe_sub_resource = request.message.parameters.get("subResource")

    subject = None
    message = None

    entity_title = build_entity_title(entity_platform, entity_type, entity_name)

    if maybe_sub_resource is not None and maybe_sub_resource_type is not None:
        # Examples:
        # - Tag PII has been proposed for column foo of SampleKafkaDataset by John Joyce.
        # - Glossary term FOOBAR has been proposed for column bar of SampleKafkaDataset.
        subject = (
            f"{modifier_type} {modifier_name} has been proposed for column {maybe_sub_resource} of {entity_title}"
            f"{' by ' + actor_name if actor_name else ''}."
        )
        message = (
            f"{modifier_type} <b>{modifier_name}</b> has been proposed for column <b>{maybe_sub_resource}</b> of <b>{entity_title}</b>"
            f"{' by ' + actor_name if actor_name else ''}."
        )
    else:
        # Examples:
        # - Tag PII has been proposed for SampleKafkaDataset by John Joyce.
        # - Glossary Term TERM has been proposed proposed for SampleKafkaDataset by John Joyce.
        subject = (
            f"{modifier_type} {modifier_name} has been proposed for {entity_title}"
            f"{' by ' + actor_name if actor_name else ''}."
        )
        message = (
            f"{modifier_type} <b>{modifier_name}</b> has been proposed for <b>{entity_title}</b>"
            f"{' by ' + actor_name if actor_name else ''}."
        )

    return {
        "subject": subject,
        "message": message,
        "entityName": entity_name,
        "detailsUrl": entity_url,
        "entityUrl": entity_url,
        "baseUrl": base_url,
    }


def build_proposal_status_change_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError(
            "Parameters are required for proposal status change notifications."
        )

    actor_name = get_actor_name(request.message.parameters.get("actorName"))
    entity_name = request.message.parameters.get("entityName", "")
    entity_path = request.message.parameters.get("entityPath")
    entity_platform = request.message.parameters.get("entityPlatform", "")
    entity_type = request.message.parameters.get("entityType", "")
    entity_url = f"{base_url}{entity_path}"
    request.message.parameters.get("entityType")
    modifier_type = request.message.parameters.get("modifierType")
    modifier_name = request.message.parameters.get("modifierName")
    operation = request.message.parameters.get("operation")
    action = request.message.parameters.get("action")

    maybe_sub_resource_type = request.message.parameters.get("subResourceType")
    maybe_sub_resource = request.message.parameters.get("subResource")

    subject = None
    message = None

    entity_title = build_entity_title(entity_platform, entity_type, entity_name)

    # Examples:
    # - Proposal to add tag PII for column foo of Dataset SampleKafkaDataset has been approved by John Joyce.
    # - Proposal to remove glossary term FOOBAR for column foo of SampleKafkaDataset has been denied by John Joyce.
    if maybe_sub_resource is not None and maybe_sub_resource_type is not None:
        subject = (
            f"Proposal to {operation} {modifier_type} {modifier_name} for column {maybe_sub_resource} of {entity_title} has been {action}"
            f"{' by ' + actor_name if actor_name else ''}."
        )
        message = (
            f"Proposal to {operation} {modifier_type} <b>{modifier_name}</b> for column <b>{maybe_sub_resource}</b> of <b>{entity_title}</b> has been {action}"
            f"{' by ' + actor_name if actor_name else ''}."
        )
    else:
        # Examples:
        # - Proposal to add tag PII for SampleKafkaDataset has been approved by John Joyce.
        # - Proposal to remove glossary term FOOBAR for SampleKafkaDataset has been denied by John Joyce.
        subject = (
            f"Proposal to {operation} {modifier_type} {modifier_name} for {entity_title} has been {action}"
            f"{' by ' + actor_name if actor_name else ''}."
        )
        message = (
            f"Proposal to {operation} {modifier_type} <b>{modifier_name}</b> for <b>{entity_title}</b> has been {action}"
            f"{' by ' + actor_name if actor_name else ''}."
        )

    return {
        "subject": subject,
        "message": message,
        "entityName": entity_name,
        "detailsUrl": entity_url,
        "entityUrl": entity_url,
        "baseUrl": base_url,
    }


def build_assertion_status_change_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError(
            "Parameters are required for assertion status change notifications."
        )

    assertion_urn = request.message.parameters.get("assertionUrn")
    assertion_type = request.message.parameters.get("assertionType")
    entity_name = request.message.parameters.get("entityName", "")
    entity_path = request.message.parameters.get("entityPath")
    entity_platform = request.message.parameters.get("entityPlatform", "")
    entity_type = request.message.parameters.get("entityType", "")
    entity_url = f"{base_url}{entity_path}"
    results_url = f"{base_url}{entity_path}/Validation/Assertions?assertion_urn={quote(assertion_urn) if assertion_urn else ''}"

    result = request.message.parameters.get("result")
    description = request.message.parameters.get("description")
    maybe_external_url = request.message.parameters.get("externalUrl", None)
    maybe_source_type = request.message.parameters.get("sourceType", None)

    # Replace 'INFERRED' with the actual value you use to represent inferred assertions
    assertion_type_text = (
        "Smart Assertion"
        if maybe_source_type == "INFERRED"
        else f"{get_assertion_type_name(assertion_type)} Assertion"
    )
    result_string = get_assertion_result_string(result)

    entity_title = build_entity_title(entity_platform, entity_type, entity_name)

    subject = f"{assertion_type_text} has {result_string} for {entity_title}!"

    # Example output:
    # Column Assertion 'column x must not be null' has failed for Dataset SampleHiveDataset!
    message = f"{assertion_type_text} '{description}' has <b>{result_string}</b> for <b>{entity_title}</b>!"

    return {
        "subject": subject,
        "message": message,
        "entityName": entity_name,
        "detailsUrl": maybe_external_url if maybe_external_url else results_url,
        "entityUrl": entity_url,
        "baseUrl": base_url,
    }


def build_entity_change_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError("Parameters are required for entity change notifications.")

    if request.message.parameters.get("modifierType") == "deprecation":
        return build_entity_deprecation_parameters(request, base_url)

    actor_name = get_actor_name(request.message.parameters.get("actorName"))
    entity_name = request.message.parameters.get("entityName", "")
    entity_platform = request.message.parameters.get("entityPlatform", "")
    entity_type = request.message.parameters.get("entityType", "")
    entity_path = request.message.parameters.get("entityPath")
    entity_url = f"{base_url}{entity_path}"
    operation = request.message.parameters.get("operation")
    modifier_type = (request.message.parameters.get("modifierType") or "").capitalize()
    modifier_string = build_modifier_string(request.message.parameters)
    modified_count_str: Optional[str] = (
        request.message.parameters.get("modifierCount")
        if request.message.parameters.get("modifierCount")
        else None
    )
    modifier_count: int = int(modified_count_str) if modified_count_str else 1
    is_ingestion_notif = is_ingestion_notification(request)
    skip_actor = (
        modifier_type.lower() == "column(s)"
    )  # Currently we do not show actor for column changes.

    entity_title = build_entity_title(entity_platform, entity_type, entity_name)

    actor_context = (
        get_actor_suffix(actor_name, is_ingestion_notif, entity_platform)
        if not skip_actor
        else ""
    )

    if modifier_count > 1:
        subject = (
            f"{modifier_count} {modifier_type.lower()} have been {operation} for {entity_title}"
            f"{actor_context}"
        )
    else:
        subject = (
            f"{modifier_type} {modifier_string} {'have' if modifier_count > 1 else 'has'} been {operation} for {entity_title}"
            f"{actor_context}"
        )
    message = (
        f"{modifier_type} <b>{modifier_string}</b> {'have' if modifier_count > 1 else 'has'} been {operation} for <b>{entity_title}</b>"
        f"{actor_context}."
    )

    return {
        "subject": subject,
        "message": message,
        "entityName": entity_name,
        "detailsUrl": entity_url,
        "entityUrl": entity_url,
        "baseUrl": base_url,
    }


def build_entity_deprecation_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError(
            "Parameters are required for entity deprecation notifications."
        )

    actor_name = get_actor_name(request.message.parameters.get("actorName"))
    entity_name = request.message.parameters.get("entityName", "")
    entity_platform = request.message.parameters.get("entityPlatform", "")
    entity_type = request.message.parameters.get("entityType", "")
    entity_path = request.message.parameters.get("entityPath")
    entity_url = f"{base_url}{entity_path}"

    operation = request.message.parameters.get("operation")

    # Note that usually deprecation wont come from ingestion, so this should be false.
    is_ingestion_notif = is_ingestion_notification(request)
    entity_title = build_entity_title(entity_platform, entity_type, entity_name)
    actor_context = get_actor_suffix(actor_name, is_ingestion_notif, entity_platform)

    subject = f"{entity_title} has been {operation}{actor_context}"
    message = f"<b>{entity_title}</b> has been <b>{operation}</b>{actor_context}."

    message_with_context = add_context_to_entity_change_message(
        message, request.message.template, request.message.parameters
    )

    return {
        "subject": subject,
        "message": message_with_context,
        "entityName": entity_name,
        "detailsUrl": entity_url,
        "entityUrl": entity_url,
        "baseUrl": base_url,
    }


def build_ingestion_run_change_parameters(
    request: NotificationRequestClass, base_url: str
) -> Dict[str, str | None]:
    if request.message.parameters is None:
        raise ValueError(
            "Parameters are required for ingestion run change notifications."
        )

    source_name = request.message.parameters.get("sourceName")
    source_type = request.message.parameters.get("sourceType")
    status_text = request.message.parameters.get("statusText")
    ingestion_url = f"{base_url}/ingestion"

    # Example outputs:
    # - Ingestion source my-ingestion-source of type kafka has failed!
    # - Ingestion source my-ingestion-source of type bigquery-usage has completed!
    # - Ingestion source my-ingestion-source of type looker has been cancelled!
    # - Ingestion source my-ingestion-source of type okta has timed out!
    # - Ingestion source my-ingestion-source of type snowflake has started!
    subject = f"Ingestion source {source_name} of type {source_type} has {status_text}."
    message = f"Ingestion source <b>{source_name}</b> of type {source_type} has <b>{status_text}</b>."

    return {
        "subject": subject,
        "message": message,
        "detailsUrl": ingestion_url,
        "baseUrl": base_url,
    }


def get_assertion_type_name(assertion_type: str | None) -> str:
    """
    Returns a string representing the type name of an assertion based on the assertion type.

    :param assertion_type: The type of the assertion.
    :return: A string representing the assertion type name.
    """
    if assertion_type == "DATASET":
        return "External"
    elif assertion_type == "FRESHNESS":
        return "Freshness"
    elif assertion_type == "VOLUME":
        return "Volume"
    elif assertion_type == "FIELD":
        return "Column"
    elif assertion_type == "SQL":
        return "Custom SQL"
    else:
        # Unrecognized type. Prefix text.
        return ""


def get_assertion_result_string(result: str | None) -> str:
    """
    Returns a string representing the result of an assertion based on the assertion result.

    :param result: The result of the assertion.
    :return: A string representing the assertion result.
    """
    if result == "SUCCESS":
        return "passed"
    elif result == "FAILURE":
        return "failed"
    elif result == "ERROR":
        return "completed with errors"
    else:
        # Unrecognized type.
        return "completed"


def get_actor_name(actor_name: Optional[str]) -> Optional[str]:
    if actor_name == "__datahub_system":
        return "DataHub System"
    return actor_name


def is_ingestion_notification(notificationRequest: NotificationRequestClass) -> bool:
    if notificationRequest.context is None:
        return False
    run_id = notificationRequest.context.runId
    return run_id is not None and run_id != NON_INGESTION_RUN_ID


def get_actor_suffix(
    actor_name: Optional[str],
    is_ingestion_notif: bool,
    platform_name: Optional[str],
) -> str:
    if is_ingestion_notif:
        platform_suffix = platform_name or "source"
        return " during sync" + f" with {platform_suffix}"
    return f" by {actor_name}" if actor_name else ""


def build_modifier_string(params: Dict[str, str]) -> str:
    """
    Handle multiple modifiers and build a string representing these modifiers.

    :param params: A dictionary containing the modifier parameters.
    :return: A formatted string representing the entity change modifiers.
    """
    modified_count_str: Optional[str] = (
        params.get("modifierCount") if params.get("modifierCount") else None
    )
    modifier_count: Optional[int] = (
        int(modified_count_str) if modified_count_str else None
    )
    if modifier_count is not None and modifier_count > 0:
        # There are modifiers.
        builder: list = [""]
        for i in range(min(modifier_count, 3)):
            # For each modifier, add it to a list
            modifier_name = params.get(f"modifier{i}Name")
            # modifier_path: str = params.get(f"modifier{i}Path")
            # modifier_url: str = f"{base_url}{modifier_path}"
            builder.append(f"{modifier_name}")
            if i < modifier_count - 1:
                builder.append(", ")
        if modifier_count > 3:
            # Then add + x more at the end. By default, only the first 3 are shown.
            builder.append(f"+ {modifier_count - 3} more")
        return "".join(builder)
    return ""


def add_context_to_entity_change_message(
    message: str,
    template: str | NotificationTemplateTypeClass,
    parameters: Dict[str, str],
) -> str:
    """
    Adjust the entity change message based on the template and parameters.

    :param message: The original message.
    :param template: The template used for the message.
    :param parameters: The parameters used for the message.
    :return: The adjusted message.
    """
    if template == NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE:
        modifier_type = parameters.get("modifierType", None)

        # We can extend with additional context for other types here.
        if modifier_type == "deprecation":
            return add_context_to_deprecation_message(message, parameters)

    return message


def add_context_to_deprecation_message(message: str, parameters: Dict[str, str]) -> str:
    """
    Adjust the entity deprecation message based on the template and parameters.
    This appends the note and deprecation timestamp to the message for deprecation.

    :param message: The original message.
    :param parameters: The parameters used for the message.
    :return: The adjusted message.
    """
    maybe_note = parameters.get("note", None)
    maybe_timestamp = parameters.get("timestamp", None)

    if maybe_note is not None or maybe_timestamp is not None:
        return (
            message
            + f"<br><br><b>Note</b>: {maybe_note or 'Not provided'}<br><b>Deprecation Date</b>: {timestamp_to_date(maybe_timestamp) if maybe_timestamp is not None else 'Not provided'}<br><br>"
        )
    return message


def timestamp_to_date(timestamp: str) -> str:
    """
    Convert a stringified timestamp to a human-readable string.

    :param timestamp: The timestamp to convert.
    :return: The date string.
    """
    # Convert the stringified timestamp to an integer
    timestamp_int = int(timestamp) // 1000  # Convert from milliseconds to seconds
    # Convert the integer timestamp to a datetime object
    date_object = datetime.utcfromtimestamp(timestamp_int)
    # Format the datetime object as a string
    return date_object.strftime("%B %d, %Y")


def build_entity_title(
    entity_platform: Optional[str], entity_type: str, entity_name: str
) -> str:
    """
    Build the entity title based on the entity platform, type, and name.

    :param entity_platform: The platform of the entity.
    :param entity_type: The type of the entity.
    :param entity_name: The name of the entity.
    :return: The entity title.
    """
    platform_prefix = entity_platform + " " if entity_platform is not None else ""
    return f"{platform_prefix}{entity_type} {entity_name}"
