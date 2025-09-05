from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.teams.cards.adaptive_cards import create_teams_help_card


async def handle_help_command_teams(
    graph: DataHubGraph, user_urn: Optional[str] = None
) -> dict:
    """Handle help command for Teams."""

    card = create_teams_help_card()

    return {"type": "message", "attachments": [card]}
