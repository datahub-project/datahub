import logging
from typing import TYPE_CHECKING, Collection, List, Optional

from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urn_alias.index import (
    CatalogSlice,
    UrnAliasIndex,
    covered_by,
    shared_index,
)
from datahub.utilities.urn_alias.remote import UrnLookup, select_lookup
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

    The one way in — `add` from a bulk load, `resolve` for a reference — but not the one
    doing the work: storing and matching belong to the lookup this server resolves by. What
    is left here is which URN to pick out of its answer, and which questions a completed
    scroll has already answered.
    """

    def __init__(
        self, index: UrnAliasIndex, lookup: UrnLookup, query_on_demand: bool = False
    ) -> None:
        self._index = index
        self._lookup = lookup
        # Whether this consumer's scope reaches past the catalogs it loaded. A policy of the
        # consumer, not of the server, which is why it sits here and not on the lookup: the
        # lookup is shared by everything resolving against one server and has to key the
        # index the same way for all of them.
        self._query_on_demand = query_on_demand

    def add(self, urn: str) -> None:
        """Record that DataHub holds `urn`, from a scroll that enumerated it."""
        self._lookup.add(urn)

    def record_slice_loaded(self, catalog_slice: CatalogSlice) -> None:
        """Record that `catalog_slice` was scrolled to completion, which makes a miss
        inside it an answer rather than a question.

        Never for a scroll that failed part way: its rows are still useful, but claiming
        coverage would turn every URN it never reached into a false absence.
        """
        if catalog_slice not in self._index.loaded_slices:
            self._index.loaded_slices.append(catalog_slice)

    def prefetch(self, urns: List[str]) -> None:
        """Learn what is not already known about `urns`, in as few queries as possible.

        A no-op for a consumer that may not query, which resolves from loaded rows alone.
        """
        if not self._query_on_demand:
            return
        # A reference inside a fully scrolled slice needs no query: that load answered it
        # already, whether or not it found the entity.
        unanswered = [
            urn for urn in urns if not covered_by(urn, self._index.loaded_slices)
        ]
        if unanswered:
            self._lookup.prefetch(unanswered)

    def find_match(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case."""
        self.prefetch([urn])
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


def get_urn_alias_resolver(
    graph: "DataHubGraph",
    query_on_demand: bool = False,
    platform_instances: Collection[str] = (),
) -> UrnAliasResolver:
    """The way to resolve URN casing against `graph`, and to fill what it resolves from.

    Reads the one index shared per DataHub instance, so it sees whatever any consumer has
    already loaded.

    `query_on_demand` is what decides whether this consumer may ask the server at all, for
    a caller whose scope reaches past the catalogs it loaded. It never fires inside a loaded
    slice either: coverage already answers every miss there. The two divide the work by
    scope rather than stacking on the same references.

    `platform_instances` is read only by the casing-probe fallback, which cannot recover an
    instance from a URN and has to be told which ones exist.

    Resolves from whatever filled the index, which is nothing unless
    `set_fill_urn_alias_index` was on before the loads it wants indexed.
    """
    index = shared_index(graph)
    # The lookup is chosen by what the server supports, never by `query_on_demand`: every
    # consumer of one graph shares the index, so all of them must key it the same way.
    lookup = select_lookup(index, graph, platform_instances)
    return UrnAliasResolver(index, lookup, query_on_demand)
