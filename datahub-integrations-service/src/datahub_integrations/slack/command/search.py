import logging
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from slack_bolt import Ack, Respond

from datahub_integrations.graphql.slack import SLACK_SEARCH_QUERY
from datahub_integrations.slack.context import SearchContext
from datahub_integrations.slack.render.render_filter import get_graphql_filters
from datahub_integrations.slack.render.render_search import render_search
from datahub_integrations.slack.utils.datahub_user import get_subscription_urn

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


def search(
    graph: DataHubGraph,
    ack: Ack,
    respond: Respond,
    channel_name: str,
    user_urn: Optional[str],
    context: SearchContext,
) -> None:
    start = context.page * RESULT_COUNT
    count = RESULT_COUNT

    variables = {
        "input": {
            "query": context.query,
            "start": start,
            "count": count,
            "types": ENTITY_TYPES,
            "orFilters": [{"and": list(get_graphql_filters(context))}],
        }
    }
    data = graph.execute_graphql(SLACK_SEARCH_QUERY, variables=variables)
    logger.debug(f"search: {data}")

    # TODO: Ideally, add subscription status to the search results
    search_urns = [
        entity["entity"]["urn"]
        for entity in data["searchAcrossEntities"]["searchResults"]
    ]
    try:
        subscriptions = (
            {urn: get_subscription_urn(urn, user_urn) for urn in search_urns}
            if user_urn
            else None
        )
    except Exception as e:
        logger.error(f"Error fetching subscriptions: {e}", exc_info=True)
        subscriptions = None

    respond(
        **render_search(data["searchAcrossEntities"], context, subscriptions),
        replace_original=True,
    )
