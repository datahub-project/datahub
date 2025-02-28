from typing import Any, Optional

import cachetools
import cachetools.keys
from datahub.ingestion.graph.client import DataHubGraph

CACHEABLE_METHODS = [
    "get_entity_raw",
    "get_entity_semityped",
    "get_aspect",
    # The variables dictionary is not hashable, so we can't cache this for now.
    # Eventually we should turn it into a frozendict and hash that.
    # "execute_graphql",
]


class _CachedGraph:
    # This class will proxy most method calls to the underlying graph.
    # Certain methods will have their results cached.

    def __init__(self, graph: DataHubGraph, cache: cachetools.Cache):
        self._graph = graph
        self._cache = cache

        self._cached_methods = {
            method: cachetools.cached(cache=cache, key=cachetools.keys.hashkey)(
                getattr(graph, method)
            )
            for method in CACHEABLE_METHODS
        }

    def __getattribute__(self, name: str) -> Any:
        cached_methods = super().__getattribute__("_cached_methods")
        if name in cached_methods:
            return cached_methods[name]
        graph = super().__getattribute__("_graph")
        return getattr(graph, name)


def make_cached_graph(
    graph: DataHubGraph, ttl_sec: Optional[float] = None
) -> DataHubGraph:
    if isinstance(graph, _CachedGraph):
        return graph  # type: ignore

    cache: cachetools.Cache
    if ttl_sec:
        cache = cachetools.TTLCache(ttl=ttl_sec, maxsize=1000)
    else:
        cache = cachetools.Cache(maxsize=1000)
    return _CachedGraph(graph=graph, cache=cache)  # type: ignore
