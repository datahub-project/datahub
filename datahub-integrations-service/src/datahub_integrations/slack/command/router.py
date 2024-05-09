import logging

from datahub.ingestion.graph.client import DataHubGraph
from slack_bolt import Ack, App, Respond

from datahub_integrations.slack.command.ask import handle_ask_command
from datahub_integrations.slack.command.get import handle_get_command
from datahub_integrations.slack.command.help import handle_help_command
from datahub_integrations.slack.command.search import search
from datahub_integrations.slack.context import SearchContext
from datahub_integrations.slack.utils.datahub_user import get_datahub_user

logger = logging.getLogger(__name__)

COMMAND_TEXT_FIELD_NAME = "text"


def handle_command(
    app: App, graph: DataHubGraph, ack: Ack, respond: Respond, command: dict
) -> None:
    ack()
    logger.debug(f"command: {command}")
    channel_name = command.get("channel_name") or ""
    user_urn = get_datahub_user(app, command["user_id"])
    text = (command.get(COMMAND_TEXT_FIELD_NAME) or "").strip()

    # Command Routing Layer
    if text.startswith("search"):
        # If search is explicitly mentioned in the command, then it is a search command
        query = text[6:].strip()
        context = SearchContext(query=query, page=0, filters={})
        return search(graph, ack, respond, channel_name, user_urn, context)
    elif text.startswith("get"):
        # If get is explicitly mentioned in the command, then it is an entity get command
        entity_urn = text[3:].strip()
        return handle_get_command(graph, ack, respond, entity_urn)
    elif text.startswith("ask"):
        # If ask is explicitly mentioned in the command, then it is an ask command
        question = text[3:].strip()
        return handle_ask_command(graph, ack, respond, question)
    elif text.startswith("help"):
        # If help is specified, provide a helpful usage message.
        return handle_help_command(graph, ack, respond)
    else:
        # If no command is specified, then it is a search command
        context = SearchContext(query=text, page=0, filters={})
        return search(graph, ack, respond, channel_name, user_urn, context)
