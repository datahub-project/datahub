"""
Feature flag utilities for Slack integration.

Fetches feature flags from GMS (GraphQL) and caches them with a TTL.
The single source of truth is the GMS ``application.yaml`` configuration;
the integrations service does **not** support an environment-variable override
to avoid inconsistencies between the Java and Python layers.
"""

import threading

from cachetools import TTLCache, cached
from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from datahub_integrations.app import graph

# Re-fetch the feature flag from GMS every 5 minutes.
_FEATURE_FLAG_TTL_SECONDS = 300
_feature_flag_cache: TTLCache = TTLCache(maxsize=1, ttl=_FEATURE_FLAG_TTL_SECONDS)
_feature_flag_lock = threading.Lock()

# GraphQL query to fetch Slack-related feature flags
GET_SLACK_FEATURE_FLAGS_QUERY = """
query GetSlackFeatureFlags {
    appConfig {
        featureFlags {
            requireSlackOAuthBinding
        }
    }
}
"""


def _fetch_feature_flag_from_gms(graph_client: DataHubGraph) -> bool:
    """
    Fetch the requireSlackOAuthBinding feature flag from GMS via GraphQL.

    This is separated for testability.

    Args:
        graph_client: DataHubGraph client to use for the query

    Returns:
        bool: Value of the feature flag from GMS

    Raises:
        Exception: If the GraphQL query fails
    """
    logger.debug("Fetching requireSlackOAuthBinding from GMS")
    result = graph_client.execute_graphql(GET_SLACK_FEATURE_FLAGS_QUERY)

    require_oauth = (
        result.get("appConfig", {})
        .get("featureFlags", {})
        .get("requireSlackOAuthBinding", False)
    )

    logger.debug(f"Fetched requireSlackOAuthBinding from GMS: {require_oauth}")
    return require_oauth


@cached(cache=_feature_flag_cache, lock=_feature_flag_lock)
def get_require_slack_oauth_binding() -> bool:
    """
    Get the requireSlackOAuthBinding feature flag from GMS.

    This flag controls whether Slack OAuth binding is required for personal notifications:
    - True: Only OAuth-bound users are resolved (strict mode)
    - False: Try OAuth first, fall back to email lookup (migration mode)

    The value is cached with a 5-minute TTL so that config changes in GMS
    are picked up without requiring an integrations-service restart.

    Returns:
        bool: True if OAuth binding is required, False otherwise
    """
    try:
        return _fetch_feature_flag_from_gms(graph)
    except Exception as e:
        logger.warning(
            f"Failed to fetch requireSlackOAuthBinding from GMS: {e}. "
            "Defaulting to False (migration mode)."
        )
        return False


def clear_feature_flag_cache() -> None:
    """
    Clear the cached feature flag value.

    Useful for testing or if you need to force a re-fetch from GMS.
    """
    _feature_flag_cache.clear()
    logger.debug("Cleared requireSlackOAuthBinding cache")
