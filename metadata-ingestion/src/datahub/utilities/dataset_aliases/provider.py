import functools
import logging
from typing import TYPE_CHECKING, Optional

from datahub.utilities.dataset_aliases.resolver import UrnAliasResolver
from datahub.utilities.perf_timer import PerfTimer

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
    batch_size: int = _BATCH_SIZE,
) -> Optional[UrnAliasResolver]:
    """A resolver over one bulk-loaded region of DataHub's catalog, cached per region.

    None, not a half-filled resolver, when the scroll fails: a key holds every casing of a
    name, so a partial one answers a later reference with the wrong entity.

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
                batch_size=batch_size,
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
        logger.warning(
            f"Failed to load URN aliases for {scope} after {count} URNs; references "
            "there will be resolved one at a time instead.",
            exc_info=True,
        )
        resolver.close()
        return None

    if count == 0:
        # An instance filter matches the `dataPlatformInstance` aspect, which a connector
        # may never emit even with the instance in the URN.
        logger.warning(
            f"Loaded 0 URNs for {scope}. If this platform instance does hold datasets, "
            "its connector likely does not emit the dataPlatformInstance aspect the "
            "filter matches; drop platform_instance to load it."
        )
    return resolver


@functools.lru_cache(maxsize=None)
def graph_urn_alias_resolver(graph: "DataHubGraph") -> UrnAliasResolver:
    """The resolver that asks `graph` about one name at a time, for references outside
    every bulk-loaded region. Shared, so a question one consumer paid for is not asked
    twice."""
    return UrnAliasResolver(graph)
