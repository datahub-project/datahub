import logging

from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.graphql.slack import SLACK_GET_ENTITY_QUERY
from datahub_integrations.teams.render.render_entity import render_entity_card

logger = logging.getLogger(__name__)


async def handle_get_command_teams(graph: DataHubGraph, entity_urn: str) -> dict:
    """Handle get command for Teams."""

    try:
        # Fetch the Entity From DataHub
        variables = {"urn": entity_urn}
        data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)

        logger.debug(f"GraphQL response for {entity_urn}: {data}")

        raw_entity = data.get("entity")
        if not raw_entity:
            return {"type": "message", "text": f"Entity not found: {entity_urn}"}

        # Render entity card for Teams
        card = render_entity_card(raw_entity)

        return {"type": "message", "attachments": [card]}

    except Exception as e:
        logger.error(f"Error fetching entity {entity_urn}: {e}", exc_info=True)
        return {
            "type": "message",
            "text": "Sorry, I encountered an error fetching the entity. Please check the URN and try again.",
        }
