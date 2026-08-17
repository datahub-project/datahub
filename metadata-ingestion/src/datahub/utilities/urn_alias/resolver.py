import logging
from typing import TYPE_CHECKING, Collection, List, Optional

from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urn_alias.index import (
    CatalogSlice,
    UrnAliasIndex,
    covered_by,
    get_urn_alias_index,
)
from datahub.utilities.urn_alias.remote import RemoteUrnLookup, select_remote_lookup
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

    One lookup, satisfiable four ways: a stored match, a previously recorded absence, an
    absence inferred from the slice having been fully loaded, or by asking DataHub. Only
    the last reaches the network, and callers do not choose between them.

    Without a `remote` the resolver never queries — the index alone answers, and an
    unknown URN simply does not resolve.
    """

    def __init__(
        self, index: UrnAliasIndex, remote: Optional[RemoteUrnLookup] = None
    ) -> None:
        self._index = index
        self._remote = remote

    def prefetch(self, urns: List[str]) -> None:
        """Learn what the index cannot already answer about `urns`, in as few queries as
        possible. A no-op with no `remote`."""
        if self._remote is None:
            return
        unknown = [urn for urn in urns if self._index.lookup(urn) is None]
        if unknown:
            self._remote.fetch(unknown)

    def find_match(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case."""
        self.prefetch([urn])
        # None (still unknown, because there is no remote or its query failed) and [] (a
        # definite absence) both mean "no match" to a caller choosing a URN.
        return self._index.lookup(urn) or []

    def resolve(
        self,
        urn: str,
        prefer_lowercased: bool = False,
        within: Optional[Collection[CatalogSlice]] = None,
    ) -> Optional[str]:
        """The dataset URN DataHub stores for `urn`, or None without a single match.

        `prefer_lowercased` settles a collision between casings on the lowercase-named URN.

        `within` restricts the answer to entities inside the given slices. The index is
        shared, so without it a match can come from a slice the caller never loaded — fine
        for identity alone, but a caller that also needs the entity's columns would find
        them nowhere. Passing the slices it loaded keeps the two together.
        """
        matches = self.find_match(urn)
        if within is not None:
            matches = [match for match in matches if covered_by(match, within)]
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
    """The way to resolve URN casing against `graph`.

    Reads the one index shared per DataHub instance, so it sees whatever any consumer has
    already loaded.

    `query_on_demand` wires up the network fallback, for a caller whose scope reaches
    past the catalogs it loaded. It never fires inside a loaded slice: coverage already
    answers every miss there. The two divide the work by scope rather than stacking on
    the same references.

    `platform_instances` is read only by the casing-probe fallback, which cannot recover
    an instance from a URN and has to be told which ones exist; the index never needs them.
    """
    index = get_urn_alias_index(graph)
    remote = (
        select_remote_lookup(graph, index, platform_instances)
        if query_on_demand
        else None
    )
    return UrnAliasResolver(index, remote)
