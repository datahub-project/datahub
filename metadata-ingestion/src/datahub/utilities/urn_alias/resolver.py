import functools
import logging
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urn_alias.index import UrnAliasIndex, lowercased_urn
from datahub.utilities.urn_alias.remote import search_aliases
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


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

    Reads across the indexes loaded for it plus a scratch index, which is where per-URN
    searches put their answers.
    """

    def __init__(
        self,
        graph: "DataHubGraph",
        indexes: Iterable[UrnAliasIndex] = (),
        query_on_demand: bool = False,
        scratch: Optional[UrnAliasIndex] = None,
    ) -> None:
        self._graph = graph
        self._indexes = tuple(indexes)
        self._scratch = scratch if scratch is not None else UrnAliasIndex()
        # A policy of the consumer, not the server.
        self._query_on_demand = query_on_demand

    @property
    def _readable(self) -> Tuple[UrnAliasIndex, ...]:
        return (*self._indexes, self._scratch)

    def covered(self, urn: str) -> bool:
        """Whether a load already answered for `urn`, so a miss is an absence."""
        return any(index.covers(urn) for index in self._indexes)

    def prefetch(self, urns: List[str]) -> None:
        """Learn what is not already known about `urns`, in as few searches as possible."""
        if not self._query_on_demand:
            return
        # Answered two ways, and both count: a load that covers it, or any index already
        # holding its key. One key covers every casing, so a batch collapses to far fewer
        # questions.
        keys: List[str] = []
        for urn in urns:
            key = lowercased_urn(urn)
            if key is None or key in keys or self._knows(urn):
                continue
            keys.append(key)
        if not keys:
            return
        for key, matches in search_aliases(self._graph, keys).items():
            self._scratch.record(key, matches)

    def find_match(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case."""
        self.prefetch([urn])
        matches: List[str] = []
        for index in self._readable:
            for match in index.matches(urn):
                if match not in matches:
                    matches.append(match)
        return matches

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

    def _knows(self, urn: str) -> bool:
        return self.covered(urn) or any(index.knows(urn) for index in self._readable)


@functools.lru_cache(maxsize=None)
def _scratch_index(graph: "DataHubGraph") -> UrnAliasIndex:
    """The one index per server for what per-URN searches answered.

    Shared, so a question one consumer paid for is not asked twice. Carries no slice, so it
    settles only the URNs actually asked about.
    """
    return UrnAliasIndex()


def get_urn_alias_resolver(
    graph: "DataHubGraph",
    indexes: Iterable[UrnAliasIndex] = (),
    query_on_demand: bool = False,
) -> UrnAliasResolver:
    """The way to resolve URN casing against `graph`, over the regions already loaded.

    `indexes` come from `load_urn_alias_index`; a region whose load failed is simply absent.
    `query_on_demand` lets a consumer ask about references outside them — never inside, as
    those loads already answer.
    """
    return UrnAliasResolver(graph, indexes, query_on_demand, _scratch_index(graph))
