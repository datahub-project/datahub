import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Set

from datahub.ingestion.graph.filters import RawSearchFilter, SearchFilterRule
from datahub.metadata.schema_classes import DatasetKeyClass
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# The `aliases` aspect's search field. Filter values are OR'd, so one query carries a batch.
_LOWERCASED_URN_FIELD = "lowercasedUrn"

# Searched alongside it: a dataset written before the `aliases` aspect existed carries no
# alias until the backfill reaches it, and is findable only under its own urn.
_URN_FIELD = "urn"

_DATASET_ENTITY_TYPE = "dataset"

# A table per lookup, not the default name: these share a sqlite file with the schema
# cache, and the two lookups key their rows differently — the index by lowercased URN,
# the probe by exact URN. A persisted file written by one must not be read by the other.
_ALIAS_INDEX_TABLE = "urn_aliases"
_CASING_PROBE_TABLE = "urn_casing_probe"

# Sized to the distinct upstreams a source references rather than to the catalog: a BI
# tool names the same warehouse table across many charts, so the repeats answer from
# memory. Costs a few MB, well above the 900 a schema cache is tuned for.
_ALIAS_CACHE_MAX_SIZE = 10_000

# Off by default: a bulk load writes a whole platform's URNs to disk. Set once per
# ingestion, before any source exists — see PipelineContext.
_LOAD_URN_ALIASES = False


def set_urn_alias_loading(value: bool) -> None:
    global _LOAD_URN_ALIASES
    _LOAD_URN_ALIASES = value


def urn_alias_loading_enabled() -> bool:
    return _LOAD_URN_ALIASES


def _lowercased_urn(urn: str) -> Optional[str]:
    """`urn` with the dataset name lowercased, or None for a non-dataset URN.

    Mirrors ``AliasesUtils.lowercaseDatasetUrn``, the value GMS indexes in ``aliases``.
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


def _instance_kept_candidate(urn: str, platform_instance: str) -> Optional[str]:
    """`urn` with the table name lowercased but the platform instance left alone.

    Mirrors ``SchemaResolver.get_urn_for_table``'s `mixed` casing, which some connectors
    produce. None when `urn` does not carry `platform_instance`.
    """
    try:
        dataset = DatasetUrn.from_string(urn)
    except InvalidUrnError:
        return None
    prefix = f"{platform_instance}."
    name = dataset.name
    # Matched case-insensitively: a reference may spell the instance any way.
    if not name.lower().startswith(prefix.lower()):
        return None
    instance, rest = name[: len(prefix) - 1], name[len(prefix) :]
    return str(
        DatasetUrn(
            platform=dataset.platform,
            name=f"{instance}.{rest.lower()}",
            env=dataset.env,
        )
    )


def _casing_candidates(urn: str, platform_instance: Optional[str]) -> List[str]:
    """The casings of `urn` worth probing for when the server has no `aliases` aspect."""
    candidates = [urn]
    extras = [_lowercased_urn(urn)]
    # Without an instance there is nothing to leave alone, and `resolve_table` likewise
    # drops its `mixed` candidate as a duplicate of the lowercased one.
    if platform_instance:
        extras.append(_instance_kept_candidate(urn, platform_instance))
    for candidate in extras:
        if candidate is not None and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _has_lowercased_name(urn: str) -> bool:
    """Whether `urn`'s dataset name is already all lowercase."""
    # The name only: a URN's scaffolding is mixed case whatever the entity is called.
    try:
        name = DatasetUrn.from_string(urn).name
    except InvalidUrnError:
        return False
    return name == name.lower()


class UrnAliasCache:
    """Stores the dataset URNs a key resolves to. The key is supplied by the caller.

    Backed by sqlite: a bulk load holds a whole platform's URNs for the pipeline's
    lifetime, roughly 500 bytes per dataset on the heap. `tablename` keeps callers that
    key their rows differently from reading each other's.
    """

    def __init__(
        self, tablename: str, shared_connection: Optional[ConnectionWrapper] = None
    ) -> None:
        # A list per key: two datasets differing only by case can both exist.
        self._urns_by_key: FileBackedDict[List[str]] = FileBackedDict(
            shared_connection=shared_connection,
            tablename=tablename,
            cache_max_size=_ALIAS_CACHE_MAX_SIZE,
        )

    def add(self, key: str, urn: str) -> None:
        """Add `urn` under `key`, keeping any URNs already there."""
        entry = self._urns_by_key.get(key)
        if entry is None:
            self._urns_by_key[key] = [urn]
            return
        if urn in entry:
            return
        # Reassigned rather than appended in place, so the store persists it.
        self._urns_by_key[key] = entry + [urn]

    def get(self, key: str) -> Optional[List[str]]:
        """URNs stored under `key`. None means unknown, `[]` means known absent."""
        entry = self._urns_by_key.get(key)
        # Copied so callers cannot mutate the store.
        return list(entry) if entry is not None else None

    def replace(self, key: str, urns: List[str]) -> None:
        """Make `urns` the complete set stored under `key`; `[]` records known absent."""
        self._urns_by_key[key] = list(dict.fromkeys(urns))

    def close(self) -> None:
        self._urns_by_key.close()


class _Lookup(Protocol):
    """How a reference is matched against the dataset URNs DataHub stores.

    Given a cache, a lookup answers from what it has already learned and `prefetch` fills
    it a batch at a time. Without one it keeps nothing, so every `matches` queries.
    """

    def add(self, urn: str) -> None:
        """Record `urn` as stored, from a bulk load."""

    def prefetch(self, urns: List[str]) -> None:
        """Learn what is not already known about `urns`, in one query."""

    def matches(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case."""


class _AliasIndexLookup:
    """Matches via the `aliases.lowercasedUrn` index: one search returns every stored casing,
    including casings nothing could have guessed.

    The search matches `urn` as well as the alias field: a dataset predating the `aliases`
    aspect carries no alias until the backfill reaches it, so it is findable only under its urn.
    """

    def __init__(
        self, graph: Optional["DataHubGraph"], cache: Optional[UrnAliasCache]
    ) -> None:
        self._graph = graph
        self._cache = cache

    def add(self, urn: str) -> None:
        if self._cache is None:
            return
        key = _lowercased_urn(urn)
        if key is not None:
            self._cache.add(key, urn)

    def matches(self, urn: str) -> List[str]:
        key = _lowercased_urn(urn)
        if key is None:
            return []
        if self._cache is not None:
            return self._cache.get(key) or []
        # Nothing held, so this reference pays for its own search.
        return self._search([key]).get(key, [])

    def prefetch(self, urns: List[str]) -> None:
        cache = self._cache
        if cache is None:
            # Nothing to fill; matches() searches per reference.
            return
        # One key covers every casing of a name.
        seen: Set[str] = set()
        keys: List[str] = []
        for urn in urns:
            key = _lowercased_urn(urn)
            if key is None or key in seen or cache.get(key) is not None:
                continue
            seen.add(key)
            keys.append(key)
        if not keys:
            return

        for key, matches in self._search(keys).items():
            cache.replace(key, matches)

    def _search(self, keys: List[str]) -> Dict[str, List[str]]:
        """Stored URNs for each of `keys`, from one search, keyed by the key they matched.

        Empty on failure, never a partial answer: a failure recorded as "absent" would
        decline every later reference to the same entity for the rest of the run.
        """
        graph = self._graph
        if graph is None:
            return {}
        or_filters: RawSearchFilter = [
            {
                "and": [
                    SearchFilterRule(
                        field=field, condition="EQUAL", values=keys
                    ).to_raw()
                ]
            }
            for field in (_LOWERCASED_URN_FIELD, _URN_FIELD)
        ]
        try:
            stored_urns = list(
                graph.get_urns_by_filter(
                    entity_types=[_DATASET_ENTITY_TYPE],
                    extra_or_filters=or_filters,
                )
            )
        except Exception as e:
            logger.warning(
                f"URN alias lookup failed for {len(keys)} key(s): {e}", exc_info=True
            )
            return {}

        matches_by_key: Dict[str, List[str]] = {key: [] for key in keys}
        # Deduped: a scroll that repeated a urn would otherwise read as a casing collision.
        for stored_urn in dict.fromkeys(stored_urns):
            key = _lowercased_urn(stored_urn)
            if key in matches_by_key:
                matches_by_key[key].append(stored_urn)
        return matches_by_key


class _CasingProbeLookup:
    """Matches by guessing casings and checking whether each exact URN exists.

    The fallback with no `aliases` aspect, so only guessable casings are findable. Existence
    is keyed by the exact URN, so two references never inherit each other's answer.
    """

    def __init__(
        self,
        graph: Optional["DataHubGraph"],
        platform_instance: Optional[str],
        cache: Optional[UrnAliasCache],
    ) -> None:
        self._graph = graph
        self._platform_instance = platform_instance
        # Keyed by the exact URN: `[urn]` where it exists, `[]` where it does not.
        self._cache = cache

    def add(self, urn: str) -> None:
        if self._cache is None:
            return
        # Only proves existence: absence is never recorded, so unknown casings still probe.
        self._cache.add(urn, urn)

    def matches(self, urn: str) -> List[str]:
        candidates = _casing_candidates(urn, self._platform_instance)
        cache = self._cache
        if cache is not None:
            return [candidate for candidate in candidates if cache.get(candidate)]
        # Nothing held, so this reference pays for its own probe. A non-dataset URN has no
        # casing to guess and must not be probed as a dataset, as in prefetch.
        if _lowercased_urn(urn) is None:
            return []
        existing = self._probe(candidates)
        if existing is None:
            return []
        return [candidate for candidate in candidates if candidate in existing]

    def prefetch(self, urns: List[str]) -> None:
        cache = self._cache
        if cache is None:
            # Nothing to fill; matches() probes per reference.
            return
        # Checked per candidate rather than against the whole index, which can be far
        # larger than the batch.
        seen: Set[str] = set()
        unknown: List[str] = []
        for urn in urns:
            if _lowercased_urn(urn) is None:
                continue
            for candidate in _casing_candidates(urn, self._platform_instance):
                if candidate in seen or cache.get(candidate) is not None:
                    continue
                seen.add(candidate)
                unknown.append(candidate)
        if not unknown:
            return

        existing = self._probe(unknown)
        if existing is None:
            return
        for candidate in unknown:
            cache.replace(candidate, [candidate] if candidate in existing else [])

    def _probe(self, candidates: List[str]) -> Optional[Set[str]]:
        """Which of `candidates` exist, or None on failure.

        None rather than an empty set: a failure recorded as "absent" would decline every
        later reference to the same entity for the rest of the run.
        """
        graph = self._graph
        if graph is None:
            return None
        try:
            entities = graph.get_entities(
                entity_name=_DATASET_ENTITY_TYPE,
                urns=candidates,
                aspects=[DatasetKeyClass.ASPECT_NAME],
            )
        except Exception as e:
            logger.warning(
                f"URN casing probe failed for {len(candidates)} URN(s): {e}",
                exc_info=True,
            )
            return None
        # get_entities drops entities with none of the requested aspects, so presence means
        # "exists". Not schemaMetadata: a schemaless dataset is exactly what this must find.
        return set(entities)


class UrnAliasResolver:
    """Resolves a dataset URN to the dataset URNs DataHub stores for it, ignoring case.

    Matching is delegated to the alias index, or to a casing probe when the server has no
    `aliases` aspect; which URN to pick out of either's answer is decided here.

    `cached` keeps what has been learned for the resolver's lifetime, which a bulk load
    requires — it has nowhere else to put a platform's URNs. Uncached, nothing is retained
    and every lookup queries: disk for round trips, and nothing at all to resolve with
    when there is no graph.
    """

    def __init__(
        self,
        graph: Optional["DataHubGraph"] = None,
        platform_instance: Optional[str] = None,
        cached: bool = True,
        shared_connection: Optional[ConnectionWrapper] = None,
    ) -> None:
        # Read once: the lookup and the table it reads have to agree.
        aliases_supported = self._aliases_supported()
        tablename = _ALIAS_INDEX_TABLE if aliases_supported else _CASING_PROBE_TABLE
        self._cache = UrnAliasCache(tablename, shared_connection) if cached else None
        # platform_instance is read only by the casing probe: the alias index keys on the
        # whole name lowercased, instance prefix included.
        self._lookup: _Lookup = (
            _AliasIndexLookup(graph, self._cache)
            if aliases_supported
            else _CasingProbeLookup(graph, platform_instance, self._cache)
        )

    def _aliases_supported(self) -> bool:
        # TODO: Detect whether the server computes the `aliases` aspect and fall back to
        # the casing probe when it does not. Assumed supported until that is decided.
        return True

    def add(self, urn: str) -> None:
        self._lookup.add(urn)

    def prefetch(self, urns: List[str]) -> None:
        """Learn what is not already known about `urns`, in one query; a no-op uncached."""
        self._lookup.prefetch(urns)

    def find_match(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case."""
        self._lookup.prefetch([urn])
        return self._lookup.matches(urn)

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
        # Safe on a shared connection: the dict flushes and leaves closing to its owner.
        if self._cache is not None:
            self._cache.close()
