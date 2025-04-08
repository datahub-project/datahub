import datetime
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub.utilities.urns.urn import Urn
from datahub_actions.utils.name_resolver import (
    get_entity_name_from_urn,
    get_entity_qualifier_from_urn,
)

logger = logging.getLogger(__name__)


def pretty_any_text(text: str, channel: str) -> str:
    if text.startswith("http:"):
        return make_url_with_title(text, text, channel)
    else:
        return text


def make_url_with_title(title: str, url: str, channel: str) -> str:
    if channel == "slack":
        # slack uses mrkdwn format
        return f"<{url}|{title}>"
    else:
        return f"[{title}]({url})"


def make_bold(text: str, channel: str) -> str:
    if not text:
        return text
    if channel == "slack":
        return f"*{text}*"
    else:
        return f"**{text}**"


@dataclass
class StructuredMessage:
    title: str
    properties: Dict[str, str]
    text: Optional[str]


def get_welcome_message(datahub_home_url: str) -> StructuredMessage:
    hostname = "unknown-host"
    try:
        import os

        hostname = os.uname()[1]
    except Exception as e:
        logger.warning(f"Failed to acquire hostname with {e}")
        pass

    current_time: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_timezone: str = str(datetime.datetime.now().astimezone().tzinfo)
    return StructuredMessage(
        title="DataHub Bot 🌟",
        properties={
            "Home 🏠": datahub_home_url,
            "Host 🖥️": hostname,
            "Started ⏱️": f"{current_time} ({current_timezone})",
        },
        text=f"👀 I'll be watching for interesting events on DataHub at <{datahub_home_url}> and keep you updated when anything changes. ⚡",
    )


def get_message_from_entity_change_event(
    event: EntityChangeEvent,
    datahub_base_url: str,
    datahub_graph: Optional[DataHubGraph],
    channel: str,
) -> Optional[str]:
    datahub_base_url = datahub_base_url.rstrip("/")
    actor_name = get_entity_name_from_urn(event.auditStamp.actor, datahub_graph)
    operation = {
        "ADD": "added",
        "UPDATE": "updated",
        "MODIFY": "updated",
        "REMOVE": "removed",
        "CREATE": "created",
        "REINSTATE": "reinstated",
    }.get(event.operation, event.operation.lower())
    if event.modifier:
        try:
            modifier_name = get_entity_name_from_urn(event.modifier, datahub_graph)
        except Exception:
            modifier_name = ""
    else:
        modifier_name = ""
    category = event.category.lower()

    entity_specialized_type = get_entity_qualifier_from_urn(
        event.entityUrn, datahub_graph
    )
    entity_name = get_entity_name_from_urn(event.entityUrn, datahub_graph)
    # special handling for entity type schemaField
    if event.entityType == "schemaField":
        schema_field_urn = Urn.from_string(event.entityUrn)
        parent_entity_urn = Urn.from_string(schema_field_urn.get_entity_id()[0])
        parent_entity_name = get_entity_name_from_urn(
            str(parent_entity_urn), datahub_graph
        )
        parent_specialized_type = get_entity_qualifier_from_urn(
            str(parent_entity_urn), datahub_graph
        )
        parent_entity_url = f"{datahub_base_url}/{parent_entity_urn.get_type()}/{parent_entity_urn}/Schema?schemaFilter={entity_name}"
        entity_message_trailer = f"{entity_name} of {parent_specialized_type} {make_url_with_title(title=parent_entity_name, url=parent_entity_url, channel=channel)}"
    elif event.entityType == "dataFlow":
        entity_url = f"{datahub_base_url}/pipelines/{event.entityUrn}"
        entity_message_trailer = make_url_with_title(
            title=entity_name, url=entity_url, channel=channel
        )
    elif event.entityType == "dataJob":
        entity_url = f"{datahub_base_url}/tasks/{event.entityUrn}"
        entity_message_trailer = make_url_with_title(
            title=entity_name, url=entity_url, channel=channel
        )
    else:
        entity_url = f"{datahub_base_url}/{event.entityType}/{event.entityUrn}"
        entity_message_trailer = make_url_with_title(
            title=entity_name, url=entity_url, channel=channel
        )

    if category == "lifecycle":
        message = f">✏️ {make_bold(actor_name, channel)} has {operation} {entity_specialized_type} {entity_message_trailer}."
    elif category == "technical_schema":
        if event.modifier and event.modifier.startswith("urn:li:schemaField"):
            message = f">✏️ {make_bold(actor_name, channel)} has {operation} field {make_bold(modifier_name, channel)} in schema for {entity_specialized_type} {entity_message_trailer}."
        else:
            message = f">✏️ {make_bold(actor_name, channel)} has {operation} {make_bold(modifier_name, channel)} schema for {entity_specialized_type} {entity_message_trailer}."
    else:
        message = f">✏️ {make_bold(actor_name, channel)} has {operation} {category} {make_bold(modifier_name, channel)} for {entity_specialized_type} {entity_message_trailer}."
    return message
