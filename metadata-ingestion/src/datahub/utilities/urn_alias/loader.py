import functools
import logging
from typing import TYPE_CHECKING, Optional

from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.urn_alias.index import CatalogSlice, UrnAliasIndex

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

_DATASET_ENTITY_TYPE = "dataset"

# Large because a page carries URNs only — no aspects to fetch, unlike the schema scroll.
_BATCH_SIZE = 5000

_PROGRESS_EVERY = 10_000


@functools.lru_cache(maxsize=None)
def load_urn_alias_index(
    graph: "DataHubGraph",
    platform: str,
    platform_instance: Optional[str],
    env: str,
    batch_size: int = _BATCH_SIZE,
) -> Optional[UrnAliasIndex]:
    """Scroll one region of DataHub's catalog into an index, cached per (graph, region).

    None, not a partial index, when the scroll fails: a key holds every casing of a name,
    so a half-filled one reads as settled and resolves references to the wrong entity.
    """
    catalog_slice = CatalogSlice(
        platform=platform, platform_instance=platform_instance, env=env
    )
    index = UrnAliasIndex(catalog_slice)
    scope = f"platform={platform}, platform_instance={platform_instance}, env={env}"
    logger.info(f"Loading URN alias index for {scope}; this may take a while...")
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
                index.add(urn)
                count += 1
                if count % _PROGRESS_EVERY == 0:
                    logger.debug(
                        f"Indexed {count} URNs for {scope} in "
                        f"{timer.elapsed_seconds()} seconds"
                    )
            logger.info(
                f"Indexed {count} URNs for {scope} in {timer.elapsed_seconds()} seconds"
            )
    except Exception:
        logger.warning(
            f"Failed to load the URN alias index for {scope} after {count} URNs; "
            "references there will be resolved one at a time instead.",
            exc_info=True,
        )
        index.close()
        return None

    if count == 0:
        # An instance filter matches the `dataPlatformInstance` aspect, which a connector
        # may never emit even with the instance in the URN.
        logger.warning(
            f"URN alias index for {scope} is empty. If this platform instance does hold "
            "datasets, its connector likely does not emit the dataPlatformInstance "
            "aspect the filter matches; drop platform_instance to index it."
        )
    return index
