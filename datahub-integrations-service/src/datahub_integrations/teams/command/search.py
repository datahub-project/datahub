import logging
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.graphql.slack import SLACK_SEARCH_QUERY
from datahub_integrations.teams.context import SearchContext
from datahub_integrations.teams.render.render_search import render_search_teams

RESULT_COUNT = 3
ENTITY_TYPES = [
    "DATASET",
    "CHART",
    "DASHBOARD",
    "DATA_JOB",
    "DATA_FLOW",
    "CONTAINER",
    "DOMAIN",
    "DATA_PRODUCT",
    "GLOSSARY_TERM",
]

logger = logging.getLogger(__name__)


async def handle_search_command_teams(
    graph: DataHubGraph, context: SearchContext, user_urn: Optional[str]
) -> dict:
    """Handle search command for Teams."""

    start = context.page * RESULT_COUNT
    count = RESULT_COUNT

    variables = {
        "input": {
            "query": context.query,
            "start": start,
            "count": count,
            "types": ENTITY_TYPES,
            "orFilters": [{"and": []}],  # Simplified for now
        }
    }

    try:
        data = graph.execute_graphql(SLACK_SEARCH_QUERY, variables=variables)
        logger.debug(f"search results: {data}")

        # Check if we have valid search results
        search_results = data.get("searchAcrossEntities") if data else None
        if not search_results:
            logger.warning(f"No search results returned for query: {context.query}")
            return {
                "type": "message",
                "text": f"No results found for '{context.query}'",
            }

        # Render search results for Teams
        return render_search_teams(search_results, context)

    except Exception as e:
        logger.error(f"Error executing search: {e}", exc_info=True)
        return {
            "type": "message",
            "text": f"Sorry, I encountered an error searching for '{context.query}'. Please try again.",
        }
