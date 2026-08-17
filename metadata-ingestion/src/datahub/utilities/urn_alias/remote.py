import logging
from typing import TYPE_CHECKING, Collection, Dict, List, Optional, Protocol, Set

from datahub.ingestion.graph.filters import RawSearchFilter, SearchFilterRule
from datahub.metadata.schema_classes import DatasetKeyClass
from datahub.metadata.urns import DatasetUrn
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


class RemoteUrnLookup(Protocol):
    """Goes and asks DataHub about URNs the index has no answer for.

    Each implementation records what it learns in the index itself, because only it knows
    what its answer proves: an exhaustive search can record an absence, a probe that
    guesses casings cannot.

    Records nothing at all when a query fails. Marking a real entity absent would decline
    every later reference to it for the rest of the run.
    """

    def fetch(self, urns: List[str]) -> None:
        """Learn what DataHub holds for `urns`, in as few queries as possible."""


class AliasSearchLookup:
    """Asks via the `aliases.lowercasedUrn` index: one search returns every stored casing,
    including casings nothing could have guessed.

    The search matches `urn` as well as the alias field: a dataset predating the `aliases`
    aspect carries no alias until the backfill reaches it, so it is findable only under
    its urn.

    Exhaustive, so a name the search does not return is genuinely absent and is recorded
    as such.
    """

    def __init__(self, graph: "DataHubGraph", index: UrnAliasIndex) -> None:
        self._graph = graph
        self._index = index

    def fetch(self, urns: List[str]) -> None:
        # One key covers every casing of a name, so a batch of references collapses to
        # far fewer questions.
        keys: List[str] = []
        seen: Set[str] = set()
        for urn in urns:
            key = lowercased_urn(urn)
            if key is None or key in seen:
                continue
            seen.add(key)
            keys.append(key)
        if not keys:
            return

        matches_by_key = self._search(keys)
        if matches_by_key is None:
            return
        for key, matches in matches_by_key.items():
            self._index.record_matches(key, matches)

    def _search(self, keys: List[str]) -> Optional[Dict[str, List[str]]]:
        """Stored URNs for each of `keys`, from one search, or None if the search failed.

        None rather than an empty result: the two must stay distinguishable, so a failure
        is never mistaken for "DataHub holds none of these".
        """
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
                self._graph.get_urns_by_filter(
                    entity_types=[_DATASET_ENTITY_TYPE],
                    extra_or_filters=or_filters,
                )
            )
        except Exception as e:
            logger.warning(
                f"URN alias lookup failed for {len(keys)} key(s): {e}", exc_info=True
            )
            return None

        matches_by_key: Dict[str, List[str]] = {key: [] for key in keys}
        # Deduped: a scroll that repeated a urn would otherwise read as a casing collision.
        for stored_urn in dict.fromkeys(stored_urns):
            key = lowercased_urn(stored_urn)
            if key in matches_by_key:
                matches_by_key[key].append(stored_urn)
        return matches_by_key


class CasingProbeLookup:
    """Asks by guessing casings and checking whether each exact URN exists.

    The fallback for a server that does not compute the `aliases` aspect, so only
    guessable casings are findable.

    Non-exhaustive, so a candidate that does not exist proves nothing about the name and
    is recorded nowhere. Not asking the same question twice is tracked here instead, which
    keeps a guess that came back empty out of the shared index entirely.
    """

    def __init__(
        self,
        graph: "DataHubGraph",
        index: UrnAliasIndex,
        platform_instances: Collection[str] = (),
    ) -> None:
        self._graph = graph
        self._index = index
        # Instances cannot be recovered from a URN, only tested against one, so the
        # candidate builder is told which ones exist rather than parsing them out.
        self._platform_instances = list(platform_instances)
        self._probed: Set[str] = set()

    def fetch(self, urns: List[str]) -> None:
        candidates: List[str] = []
        for urn in urns:
            for candidate in self._candidates(urn):
                if candidate in self._probed or candidate in candidates:
                    continue
                candidates.append(candidate)
        if not candidates:
            return

        existing = self._probe(candidates)
        if existing is None:
            return
        self._probed.update(candidates)
        for urn in existing:
            self._index.add(urn)

    def _candidates(self, urn: str) -> List[str]:
        """The casings of `urn` worth probing for."""
        lowercased = lowercased_urn(urn)
        if lowercased is None:
            # Not a dataset: no casing to guess, and it must not be probed as one.
            return []
        candidates = [urn]
        extras: List[Optional[str]] = [lowercased]
        for platform_instance in self._platform_instances:
            extras.append(_instance_kept_candidate(urn, platform_instance))
        for candidate in extras:
            if candidate is not None and candidate not in candidates:
                candidates.append(candidate)
        return candidates

    def _probe(self, candidates: List[str]) -> Optional[Set[str]]:
        """Which of `candidates` exist, or None if the probe failed."""
        try:
            entities = self._graph.get_entities(
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


def _server_computes_aliases(graph: "DataHubGraph") -> bool:
    # TODO: Detect whether the server computes the `aliases` aspect and fall back to the
    # casing probe when it does not. Assumed supported until that is decided.
    return True


def select_remote_lookup(
    graph: "DataHubGraph",
    index: UrnAliasIndex,
    platform_instances: Collection[str] = (),
) -> RemoteUrnLookup:
    """The way to ask this server about URNs it may hold under another casing."""
    if _server_computes_aliases(graph):
        return AliasSearchLookup(graph, index)
    return CasingProbeLookup(graph, index, platform_instances)
