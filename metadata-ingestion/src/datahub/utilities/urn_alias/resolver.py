import functools
import logging
from typing import TYPE_CHECKING, List, Optional

from datahub.metadata.urns import DatasetUrn
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.urn_alias.remote import search_aliases
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

_TABLE = "urn_aliases"
_DATASET_ENTITY_TYPE = "dataset"

# Sized to the distinct upstreams a source references rather than to the catalog: a BI
# tool names the same warehouse table across many charts, so the repeats answer from
# memory.
_CACHE_MAX_SIZE = 10_000

# Large because a page carries URNs only — no aspects to fetch, unlike the schema scroll.
_BATCH_SIZE = 5000

_PROGRESS_EVERY = 10_000


def lowercased_urn(urn: str) -> Optional[str]:
    """`urn` with the dataset name lowercased, or None for a non-dataset URN."""
    # Mirrors AliasesUtils.lowercaseDatasetUrn, the value GMS indexes in `aliases`.
    try:
        dataset = DatasetUrn.from_string(urn)
    except InvalidUrnError:
        return None
    return str(
        DatasetUrn(
            platform=dataset.platform, name=dataset.name.lower(), env=dataset.env
        )
    )


def _has_lowercased_name(urn: str) -> bool:
    """Whether `urn`'s dataset name is already all lowercase."""
    # The name only: a URN's scaffolding is mixed case whatever the entity is called.
    try:
        name = DatasetUrn.from_string(urn).name
    except InvalidUrnError:
        return False
    return name == name.lower()


class UrnAliasResolver:
    """Resolves a dataset URN to the URN DataHub stores for it, ignoring case.

    Keyed by the lowercased URN, the value GMS indexes as a dataset's alias, so one row
    answers for every casing of a name.

    With `graph`, an unknown name is fetched. Without it the resolver answers from what it
    holds alone, so a miss is an absence — which is only true of one filled by a bulk load
    that ran to completion, and is why `provide_urn_alias_resolver` returns None otherwise.
    """

    def __init__(self, graph: "Optional[DataHubGraph]" = None) -> None:
        # Backed by sqlite: a bulk load holds a whole platform's URNs for the pipeline's
        # lifetime, roughly 500 bytes per dataset on disk.
        self._graph = graph
        self._urns_by_key: FileBackedDict[List[str]] = FileBackedDict(
            tablename=_TABLE, cache_max_size=_CACHE_MAX_SIZE
        )

    def add(self, urn: str) -> None:
        """Record `urn` as stored, from a scroll that enumerated it."""
        key = lowercased_urn(urn)
        if key is None:
            return
        entry = self._urns_by_key.get(key)
        if entry is None:
            self._urns_by_key[key] = [urn]
        elif urn not in entry:
            # Reassigned rather than appended in place, so the store persists it.
            self._urns_by_key[key] = entry + [urn]

    def find_match(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case. Raises if a fetch fails."""
        key = lowercased_urn(urn)
        if key is None:
            # Not a dataset, so it has no casing to reconcile.
            return []
        entry = self._urns_by_key.get(key)
        if entry is None:
            if self._graph is None:
                return []
            # The search is exhaustive, so `[]` is a genuine absence and is recorded as
            # one — the same name is not asked about twice.
            entry = search_aliases(self._graph, key)
            self._urns_by_key[key] = entry
        return entry

    def resolve(self, urn: str, prefer_lowercased: bool = False) -> Optional[str]:
        """The dataset URN DataHub stores for `urn`, or None without a single match.

        `prefer_lowercased` settles a collision between casings on the lowercase-named URN.
        """
        matches = self.find_match(urn)
        if len(matches) == 1:
            return matches[0]
        if urn in matches:
            return urn
        if prefer_lowercased:
            return next((m for m in matches if _has_lowercased_name(m)), None)
        return None

    def close(self) -> None:
        self._urns_by_key.close()


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
