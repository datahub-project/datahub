import functools
import logging
from typing import TYPE_CHECKING, Optional

from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.urn_aliases.resolver import UrnAliasResolver

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

_DATASET_ENTITY_TYPE = "dataset"

_BATCH_SIZE = 1000

_PROGRESS_EVERY = 5000


@functools.lru_cache(maxsize=None)
def provide_urn_alias_resolver(
    graph: "DataHubGraph",
    platform: str,
    platform_instance: Optional[str],
    env: str,
) -> UrnAliasResolver:
    """A graph-less resolver over one bulk-loaded region of DataHub, cached per region.

    Raises when the scroll fails part way: a key holds every casing of a name, so a partial
    row is a hit with an incomplete list, which heals a reference to the wrong entity. The
    caller owns the report, so the cause reaches it; a raise is not memoised either, so a
    later region is not disabled by one flaky search.

    Scrolls URNs alone; schemas are a separate concern with a separate loader.
    """
    scope = f"platform={platform}, platform_instance={platform_instance}, env={env}"
    logger.info(f"Loading URN aliases for {scope}; this may take a while...")
    resolver = UrnAliasResolver()
    count = 0
    try:
        with PerfTimer() as timer:
            for urn in graph.get_urns_by_filter(
                entity_types=[_DATASET_ENTITY_TYPE],
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                batch_size=_BATCH_SIZE,
            ):
                resolver.add(urn)
                count += 1
                if count % _PROGRESS_EVERY == 0:
                    logger.debug(
                        f"Loaded {count} URNs for {scope} in "
                        f"{timer.elapsed_seconds()} seconds"
                    )
            logger.info(
                f"Loaded {count} URNs for {scope} in {timer.elapsed_seconds()} seconds"
            )
    except Exception:
        logger.debug(
            f"Failed to load URN aliases for {scope} after {count} URNs.",
            exc_info=True,
        )
        resolver.close()
        raise
    return resolver


@functools.lru_cache(maxsize=None)
def graph_urn_alias_resolver(graph: "DataHubGraph") -> UrnAliasResolver:
    """Asks `graph` about one name at a time, for references no bulk load held. Shared, so
    one question is not paid for twice."""
    return UrnAliasResolver(graph)
