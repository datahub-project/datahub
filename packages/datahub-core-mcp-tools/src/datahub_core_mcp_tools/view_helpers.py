"""Resolve the default DataHub view a search applies.

Separated from view_preference.py to avoid circular imports.
"""

from typing import Hashable, Optional

import cachetools
from cachetools.keys import hashkey
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from . import graphql_helpers

DISABLE_DEFAULT_VIEW = get_boolean_env_variable(
    "DATAHUB_MCP_DISABLE_DEFAULT_VIEW", default=False
)
VIEW_CACHE_TTL_SECONDS = 300

if not DISABLE_DEFAULT_VIEW:
    logger.info("Default view application ENABLED (cache TTL: 5 minutes)")
else:
    logger.info("Default view application DISABLED")


def _caller_key(graph: DataHubGraph) -> Hashable:
    """Cache by the server and token a graph calls with, not by the graph object.

    A multi-user host builds a new client for every request, so a cache keyed on the
    graph object would never hit, and would keep every recent client alive. Graphs
    that authenticate another way (a token provider) are keyed by the object itself.
    """
    config = getattr(graph, "config", None)
    server = getattr(config, "server", None)
    token = getattr(config, "token", None)
    if isinstance(server, str) and (
        isinstance(token, str) or getattr(config, "auth", None) is None
    ):
        return hashkey(server, token)
    return hashkey(graph)


_global_view_cache: cachetools.TTLCache = cachetools.TTLCache(
    maxsize=64, ttl=VIEW_CACHE_TTL_SECONDS
)


@cachetools.cached(cache=_global_view_cache, key=_caller_key)
def fetch_global_default_view(graph: DataHubGraph) -> Optional[str]:
    """The organization's default view URN, or None when disabled or not set.

    Cached for VIEW_CACHE_TTL_SECONDS seconds.
    """
    if DISABLE_DEFAULT_VIEW:
        return None

    query = """
    query getGlobalViewsSettings {
        globalViewsSettings {
            defaultView
        }
    }
    """

    result = graphql_helpers.execute_graphql(graph, query=query)
    settings = result.get("globalViewsSettings")
    if settings:
        view_urn = settings.get("defaultView")
        if view_urn:
            logger.debug("Fetched global default view: {}", view_urn)
            return view_urn
    logger.debug("No global default view configured")
    return None


_user_view_cache: cachetools.TTLCache = cachetools.TTLCache(
    maxsize=64, ttl=VIEW_CACHE_TTL_SECONDS
)


@cachetools.cached(cache=_user_view_cache, key=_caller_key)
def fetch_user_default_view(graph: DataHubGraph) -> Optional[str]:
    """The calling user's personal default view URN.

    Uses the ``me`` query, so the result depends on the user behind ``graph``.
    Returns None if disabled, if the user has no default view, or if the settings
    cannot be read. Cached per caller for VIEW_CACHE_TTL_SECONDS seconds.
    """
    if DISABLE_DEFAULT_VIEW:
        return None

    query = """
    query getUserDefaultView {
        me {
            corpUser {
                settings {
                    views {
                        defaultView {
                            urn
                        }
                    }
                }
            }
        }
    }
    """

    try:
        result = graphql_helpers.execute_graphql(graph, query=query)
    except Exception:
        logger.opt(exception=True).warning(
            "Failed to fetch user default view, skipping"
        )
        return None

    me = result.get("me") or {}
    corp_user = me.get("corpUser") or {}
    settings = corp_user.get("settings") or {}
    views = settings.get("views") or {}
    default_view = views.get("defaultView") or {}
    urn = default_view.get("urn")
    if urn:
        logger.debug("Fetched user default view: {}", urn)
    else:
        logger.debug("No user default view configured")
    return urn
