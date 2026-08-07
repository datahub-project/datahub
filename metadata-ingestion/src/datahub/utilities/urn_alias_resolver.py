import logging
from typing import TYPE_CHECKING, Dict, List, Optional

from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# The `aliases` aspect's search field, an exact-match keyword. Filter values are OR'd, so
# one query can carry a whole batch of keys.
_LOWERCASED_URN_FIELD = "lowercasedUrn"

_DATASET_ENTITY_TYPE = "dataset"

# Whether a bulk load should fill its URN index. Off by default: the index holds a whole
# platform's URNs in memory. Set once per ingestion, before any source exists — see
# PipelineContext.
_LOAD_URN_ALIASES = False


def set_urn_alias_loading(value: bool) -> None:
    global _LOAD_URN_ALIASES
    _LOAD_URN_ALIASES = value


def urn_alias_loading_enabled() -> bool:
    return _LOAD_URN_ALIASES


def _alias_lookup_key(urn: str) -> Optional[str]:
    """The value GMS indexes in ``aliases.lowercasedUrn`` for `urn`, or None for a non-dataset.

    Mirrors ``AliasesUtils.lowercaseDatasetUrn``: only the dataset name is lowercased, the
    platform and environment are left alone.
    """
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
    # The name only: a URN's scaffolding (`dataPlatform`, the env) is mixed case whatever
    # the entity is called, so `urn == urn.lower()` is never true.
    try:
        name = DatasetUrn.from_string(urn).name
    except InvalidUrnError:
        return False
    return name == name.lower()


class UrnAliasCache:
    """Stores dataset URNs by their alias lookup key, for case-insensitive lookup.

    `add` and `get` derive the key from a plain URN; a non-dataset URN has no key and is
    ignored.
    """

    def __init__(self) -> None:
        # A list per key: two datasets differing only by case can both exist.
        self._urns_by_key: Dict[str, List[str]] = {}
        self._count = 0

    def add(self, urn: str) -> None:
        """Add `urn` to those stored for its key, leaving any others in place."""
        key = _alias_lookup_key(urn)
        if key is None:
            return
        entry = self._urns_by_key.setdefault(key, [])
        if urn in entry:
            return
        entry.append(urn)
        self._count += 1

    def get(self, urn: str) -> Optional[List[str]]:
        """URNs stored for `urn` ignoring case. None means unknown, `[]` means known absent."""
        key = _alias_lookup_key(urn)
        if key is None:
            return None
        entry = self._urns_by_key.get(key)
        # Copied so callers cannot mutate the store.
        return list(entry) if entry is not None else None

    def replace(self, key: str, urns: List[str]) -> None:
        """Make `urns` the complete set stored for `key`; an empty list records known absent."""
        previous = self._urns_by_key.get(key)
        if previous is not None:
            self._count -= len(previous)
        deduped = list(dict.fromkeys(urns))
        self._urns_by_key[key] = deduped
        self._count += len(deduped)

    def count(self) -> int:
        return self._count


class UrnAliasResolver:
    """Resolves a dataset URN to the dataset URNs DataHub stores for it, ignoring case.

    Answers from a local index, filled by a bulk load up front or, given a `graph`, by an
    on-demand lookup.
    """

    def __init__(self, graph: Optional["DataHubGraph"] = None) -> None:
        self._cache = UrnAliasCache()
        self._graph = graph

    def add(self, urn: str) -> None:
        self._cache.add(urn)

    def find_matches(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case."""
        self.prefetch([urn])
        return self._cache.get(urn) or []

    def prefetch(self, urns: List[str]) -> None:
        """Fill the index for every URN it doesn't already answer, in one query."""
        unknown = [urn for urn in urns if self._cache.get(urn) is None]
        if unknown:
            self._query_matches(unknown)

    def _query_matches(self, urns: List[str]) -> None:
        """Query DataHub for all of `urns` at once and cache the answers."""
        if self._graph is None:
            return
        # One bucket per key: URNs differing only in the casing of the name share a key,
        # so one query value and one cache write covers all of them.
        matches_by_key: Dict[str, List[str]] = {}
        for urn in urns:
            key = _alias_lookup_key(urn)
            if key is not None:
                matches_by_key.setdefault(key, [])
        if not matches_by_key:
            return

        try:
            stored_urns = list(
                self._graph.get_urns_by_filter(
                    entity_types=[_DATASET_ENTITY_TYPE],
                    extraFilters=[
                        SearchFilterRule(
                            field=_LOWERCASED_URN_FIELD,
                            condition="EQUAL",
                            values=list(matches_by_key.keys()),
                        ).to_raw()
                    ],
                )
            )
        except Exception as e:
            # Not cached: a failure stored as "known absent" would decline every later
            # reference to the same entity for the rest of the run.
            logger.warning(
                f"URN alias lookup failed for {len(matches_by_key)} key(s): {e}",
                exc_info=True,
            )
            return

        for stored_urn in stored_urns:
            key = _alias_lookup_key(stored_urn)
            if key in matches_by_key:
                matches_by_key[key].append(stored_urn)
        for key, matches in matches_by_key.items():
            self._cache.replace(key, matches)

    def resolve(self, urn: str, prefer_lowercased: bool = False) -> Optional[str]:
        """The dataset URN DataHub stores for `urn`, or None without a single match.

        `prefer_lowercased` settles a collision between casings on the lowercase-named URN.
        """
        matches = self.find_matches(urn)
        if len(matches) == 1:
            return matches[0]
        if urn in matches:
            return urn
        if prefer_lowercased:
            return next((m for m in matches if _has_lowercased_name(m)), None)
        return None

    def cached_urn_count(self) -> int:
        return self._cache.count()
