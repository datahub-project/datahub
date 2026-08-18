import logging
import weakref
from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    List,
    MutableMapping,
    Optional,
    Protocol,
    Set,
)

from datahub.ingestion.graph.filters import RawSearchFilter, SearchFilterRule
from datahub.metadata.schema_classes import (
    DataHubUpgradeResultClass,
    DataHubUpgradeStateClass,
)
from datahub.metadata.urns import DataHubUpgradeUrn, DatasetUrn
from datahub.utilities.urn_alias.index import UrnAliasIndex, lowercased_urn
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

# The GMS backfill that computes `aliases` for datasets created before the aspect existed.
# The `-vN` suffix is the lowercasing-rule version, so this must track
# `AliasesUtils.DATASET_ALIASES_BACKFILL_UPGRADE_ID`.
_ALIASES_BACKFILL_UPGRADE_URN = str(DataHubUpgradeUrn("dataset-aliases-v1"))


class UrnLookup(Protocol):
    """How references are matched against the dataset URNs DataHub holds.

    A lookup owns the index: `add` records what a bulk load enumerated, `prefetch` goes and
    asks, `matches` answers from what is stored. All three key the index the same way,
    which is why they belong together — only the lookup knows what its rows mean.
    """

    def add(self, urn: str) -> None:
        """Record `urn` as stored, from a scroll that enumerated it."""

    def prefetch(self, urns: List[str]) -> None:
        """Learn what is not already known about `urns`, in as few queries as possible.

        Raises if a query fails, having recorded nothing.
        """

    def matches(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case, from what is already known."""


class AliasIndexLookup:
    """Matches by the `aliases.lowercasedUrn` GMS indexes: one search returns every stored
    casing of a name, including casings nothing could have guessed.

    Keys the index by the lowercased URN, so one row answers for every casing of a name.
    """

    def __init__(self, index: UrnAliasIndex, graph: "DataHubGraph") -> None:
        self._index = index
        self._graph = graph

    def add(self, urn: str) -> None:
        key = lowercased_urn(urn)
        if key is not None:
            self._index.add(key, urn)

    def matches(self, urn: str) -> List[str]:
        key = lowercased_urn(urn)
        if key is None:
            # Not a dataset, so it has no casing to reconcile.
            return []
        return self._index.get(key) or []

    def prefetch(self, urns: List[str]) -> None:
        # One key covers every casing of a name, so a batch of references collapses to
        # far fewer questions.
        keys: List[str] = []
        for urn in urns:
            key = lowercased_urn(urn)
            if key is None or key in keys or self._index.get(key) is not None:
                continue
            keys.append(key)
        if not keys:
            return

        for key, matches in self._search(keys).items():
            # The search is exhaustive, so `[]` here is a genuine absence.
            self._index.replace(key, matches)

    def _search(self, keys: List[str]) -> Dict[str, List[str]]:
        """Stored URNs for each of `keys`, from one search. Raises if the search fails."""
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
        stored_urns = list(
            self._graph.get_urns_by_filter(
                entity_types=[_DATASET_ENTITY_TYPE],
                extra_or_filters=or_filters,
            )
        )
        matches_by_key: Dict[str, List[str]] = {key: [] for key in keys}
        # Deduped: a scroll that repeated a urn would otherwise read as a casing collision.
        for stored_urn in dict.fromkeys(stored_urns):
            key = lowercased_urn(stored_urn)
            if key in matches_by_key:
                matches_by_key[key].append(stored_urn)
        return matches_by_key


class CasingProbeLookup:
    """Matches by guessing casings and checking whether each exact URN exists.

    The fallback where the `aliases` aspect is not maintained, so only guessable casings are
    findable — including among the rows a bulk load left.

    Keys the index by the exact URN, one row per spelling checked and `[]` where it does not
    exist, because that is all a guess establishes: finding `db.tbl` says nothing about
    `DB.TBL`. Keying by the name instead would answer for casings never tried, resolving a
    later reference to the wrong entity without even asking.

    Rows a bulk load leaves are keyed the same way, deliberately, even though its scroll saw
    every casing in the slice. Reading those by name would let a reference resolve to a
    casing no guess reaches — but only where the operator happened to preload, making the
    verdict depend on the recipe rather than on what DataHub holds. Preloading is a cost
    knob; it must not change the answer. Reaching an unguessable casing is what the
    `aliases` aspect is for.
    """

    def __init__(
        self,
        index: UrnAliasIndex,
        graph: "DataHubGraph",
        platform_instances: Collection[str] = (),
    ) -> None:
        self._index = index
        self._graph = graph
        # Instances cannot be recovered from a URN, only tested against one, so the
        # candidate builder is told which ones exist rather than parsing them out.
        self._platform_instances = list(platform_instances)

    def add(self, urn: str) -> None:
        self._index.add(urn, urn)

    def matches(self, urn: str) -> List[str]:
        if lowercased_urn(urn) is None:
            # Not a dataset: no casing to guess, and it must not be probed as one.
            return []
        return [
            candidate
            for candidate in self._candidates(urn)
            if self._index.get(candidate)
        ]

    def prefetch(self, urns: List[str]) -> None:
        # Checked per candidate rather than against the whole table, which can be far
        # larger than the batch.
        unchecked: List[str] = []
        for urn in urns:
            if lowercased_urn(urn) is None:
                continue
            for candidate in self._candidates(urn):
                if candidate in unchecked or self._index.get(candidate) is not None:
                    continue
                unchecked.append(candidate)
        if not unchecked:
            return

        existing = self._probe(unchecked)
        for candidate in unchecked:
            self._index.replace(candidate, [candidate] if candidate in existing else [])

    def _candidates(self, urn: str) -> List[str]:
        """The casings of `urn` worth probing for."""
        candidates = [urn]
        extras: List[Optional[str]] = [lowercased_urn(urn)]
        for platform_instance in self._platform_instances:
            extras.append(_instance_kept_candidate(urn, platform_instance))
        for candidate in extras:
            if candidate is not None and candidate not in candidates:
                candidates.append(candidate)
        return candidates

    def _probe(self, candidates: List[str]) -> Set[str]:
        """Which of `candidates` exist. Raises if the probe fails.

        Searched by urn like the alias lookup, so soft-deleted entities are dropped by the
        same server-side filter rather than a rule of our own.
        """
        or_filters: RawSearchFilter = [
            {
                "and": [
                    SearchFilterRule(
                        field=_URN_FIELD, condition="EQUAL", values=candidates
                    ).to_raw()
                ]
            }
        ]
        return set(
            self._graph.get_urns_by_filter(
                entity_types=[_DATASET_ENTITY_TYPE],
                extra_or_filters=or_filters,
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
    rest = name[len(prefix) :]
    return str(
        DatasetUrn(
            platform=dataset.platform,
            name=f"{platform_instance}.{rest.lower()}",
            env=dataset.env,
        )
    )


# Read once per graph: switching lookups under a live index would leave its rows keyed two
# ways.
_BACKFILLED: "MutableMapping[DataHubGraph, bool]" = weakref.WeakKeyDictionary()


def _aliases_backfilled(graph: "DataHubGraph") -> bool:
    """Whether the server has computed the dataset `aliases` the search reads.

    Until the backfill completes, a dataset it has not reached has no alias, and the search
    would record it as a settled absence.
    """
    backfilled = _BACKFILLED.get(graph)
    if backfilled is not None:
        return backfilled
    try:
        result = graph.get_aspect(
            _ALIASES_BACKFILL_UPGRADE_URN, DataHubUpgradeResultClass
        )
    except Exception as e:
        # A server that does not compute aliases answers 404, which is a marker of `None`
        # rather than an error, so a failed read says nothing about the aspect.
        logger.warning(f"Could not read the dataset aliases backfill marker: {e}")
        _BACKFILLED[graph] = True
        return True
    state = result.state if result is not None else "never run"
    backfilled = state == DataHubUpgradeStateClass.SUCCEEDED
    if not backfilled:
        logger.warning(
            f"Dataset aliases backfill {state}; resolving URN casing by probing candidate "
            "casings instead."
        )
    _BACKFILLED[graph] = backfilled
    return backfilled


def select_lookup(
    index: UrnAliasIndex,
    graph: "DataHubGraph",
    platform_instances: Collection[str] = (),
) -> UrnLookup:
    """The one way references are matched against this server, for every consumer of it.

    One per server, never both: each keys the index its own way, so a row written by one
    must not be read by the other. Which is why the choice turns only on the server, and
    not on whether this particular consumer is allowed to query — that is the resolver's
    business, and a consumer that may not query still fills and reads the same rows.
    """
    if _aliases_backfilled(graph):
        return AliasIndexLookup(index, graph)
    return CasingProbeLookup(index, graph, platform_instances)
