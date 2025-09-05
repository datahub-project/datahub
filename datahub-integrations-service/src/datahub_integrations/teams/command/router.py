import logging

# Import TeamsActivity from teams module for proper typing
# We import it here only for type checking to avoid circular imports
from typing import TYPE_CHECKING, Optional

from datahub_integrations.app import graph
from datahub_integrations.teams.command.ask import handle_ask_command_teams
from datahub_integrations.teams.command.get import handle_get_command_teams
from datahub_integrations.teams.command.help import handle_help_command_teams
from datahub_integrations.teams.command.search import handle_search_command_teams
from datahub_integrations.teams.config import TeamsConnection
from datahub_integrations.teams.context import SearchContext

if TYPE_CHECKING:
    from datahub_integrations.teams.teams import TeamsActivity

logger = logging.getLogger(__name__)


async def route_teams_command(command_text: str, user_urn: Optional[str]) -> dict:
    """Route Teams commands to appropriate handlers."""

    text = command_text.strip()
    text_lower = text.lower()

    # Command Routing Layer
    if text_lower.startswith("search"):
        # If search is explicitly mentioned in the command, then it is a search command
        query = text[6:].strip()
        context = SearchContext(query=query, page=0, filters={})
        return await handle_search_command_teams(graph, context, user_urn)
    elif text_lower.startswith("get"):
        # If get is explicitly mentioned in the command, then it is an entity get command
        entity_urn = text[3:].strip()
        return await handle_get_command_teams(graph, entity_urn)
    elif text_lower.startswith("ask"):
        # If ask is explicitly mentioned in the command, then it is an ask command
        question = text[3:].strip()
        return await handle_ask_command_teams(graph, question, user_urn)
    elif text_lower.startswith("help"):
        # If help is specified, provide a helpful usage message.
        return await handle_help_command_teams(graph, user_urn)
    else:
        # If no command is specified, then it is a search command
        context = SearchContext(query=text, page=0, filters={})
        return await handle_search_command_teams(graph, context, user_urn)


async def handle_teams_command_from_router(
    activity: "TeamsActivity", config: TeamsConnection
) -> dict:
    """Handle Teams command from webhook."""

    try:
        # Extract user information
        # user_id = activity.from_.get("id") if activity.from_ else None
        # You would implement proper user mapping here
        user_urn = None  # get_datahub_user_teams(user_id) if user_id else None

        # Parse command
        command_text = (activity.text or "").replace("/datahub", "").strip()

        # Route to appropriate handler
        response = await route_teams_command(command_text, user_urn)

        return response

    except Exception as e:
        logger.error(f"Error handling Teams command: {e}")
        return {
            "type": "message",
            "text": "Sorry, I encountered an error processing your command.",
        }
