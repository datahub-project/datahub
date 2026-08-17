import logging
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Collection, List, MutableMapping, Optional

from datahub.metadata.urns import DataPlatformUrn, DatasetUrn
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

_INDEX_TABLE = "urn_aliases"

# Sized to the distinct upstreams a source references rather than to the catalog: a BI
# tool names the same warehouse table across many charts, so the repeats answer from
# memory. Costs a few MB, well above the 900 a schema cache is tuned for.
_INDEX_CACHE_MAX_SIZE = 10_000


def lowercased_urn(urn: str) -> Optional[str]:
    """`urn` with the dataset name lowercased, or None for a non-dataset URN.

    Mirrors ``AliasesUtils.lowercaseDatasetUrn``, the value GMS indexes in ``aliases``.
    This is the index's key space: every casing of a name collapses onto one key.
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


@dataclass(frozen=True)
class CatalogSlice:
    """A region of DataHub's catalog that some load scrolled to completion.

    Recorded so a lookup that finds nothing inside it is an *answer* — DataHub does not
    hold this URN — rather than "we may simply never have fetched this part".

    Deliberately not a scope you look a URN's own attributes up by: ``platform_instance``
    cannot be recovered from a URN, because it is fused into the dataset name as a
    leading prefix with nothing marking where it ends. It can only be *tested* against
    one, which is what ``covers`` does.
    """

    platform: str
    platform_instance: Optional[str]
    env: str

    def covers(self, key: str) -> bool:
        """Whether this slice's load would have fetched `key`, a lowercased dataset URN.

        Tested on the lowercased form so the answer agrees with the key space lookups
        happen in.

        Not airtight, and cannot be: a name like ``prod_wh.db.tbl`` is indistinguishable
        between "instance prod_wh, table db.tbl" and "no instance, table literally called
        prod_wh.db.tbl", so a slice loaded for instance ``prod_wh`` claims to cover the
        second as well. It fails safe — the caller reads an absence, the reference is left
        UNRESOLVED, which is what happens today anyway.
        """
        try:
            dataset = DatasetUrn.from_string(key)
        except InvalidUrnError:
            return False
        if dataset.platform != str(DataPlatformUrn(self.platform)):
            return False
        if dataset.env != self.env:
            return False
        if self.platform_instance is None:
            # No instance filter on the scroll means every instance was loaded — not
            # that only instance-less datasets were.
            return True
        # A dataset in instance X always has the name `X.<rest>`.
        return dataset.name.startswith(f"{self.platform_instance.lower()}.")


def covered_by(urn: str, slices: Collection[CatalogSlice]) -> bool:
    """Whether `urn` falls inside any of `slices`.

    The URN in its stored casing: ``covers`` does the lowercasing, so callers pass
    whatever they hold.
    """
    key = lowercased_urn(urn)
    return key is not None and any(
        catalog_slice.covers(key) for catalog_slice in slices
    )


class UrnAliasIndex:
    """The dataset URNs DataHub holds, keyed case-insensitively, plus what has been loaded.

    Purely local: it never reaches the network, and works with no server connection at
    all. Going and asking DataHub is a capability layered above it (see ``remote.py``).

    Shared by every consumer against one DataHub instance, so a lookup can be answered by
    whichever load happened to run first — see ``get_urn_alias_index``.
    """

    def __init__(self, shared_connection: Optional[ConnectionWrapper] = None) -> None:
        # A list per key: two datasets differing only by case can both exist. Backed by
        # sqlite because a bulk load holds a whole platform's URNs for the run.
        self._urns_by_key: FileBackedDict[List[str]] = FileBackedDict(
            shared_connection=shared_connection,
            tablename=_INDEX_TABLE,
            cache_max_size=_INDEX_CACHE_MAX_SIZE,
        )
        # A handful of entries at most — one per completed scroll — so kept in memory.
        self._loaded_slices: List[CatalogSlice] = []

    def add(self, urn: str) -> None:
        """Record that DataHub holds `urn`, keeping any URNs already under its key."""
        key = lowercased_urn(urn)
        if key is None:
            return
        entry = self._urns_by_key.get(key)
        if entry is None:
            self._urns_by_key[key] = [urn]
            return
        if urn in entry:
            return
        # Reassigned rather than appended in place, so the store persists it.
        self._urns_by_key[key] = entry + [urn]

    def record_matches(self, key: str, urns: List[str]) -> None:
        """Make `urns` the complete set held under `key`; `[]` records a known absence.

        Only for an *exhaustive* answer. A non-exhaustive one (the casing probe, which
        can only find casings it guessed) must use ``add`` and record its misses nowhere.
        """
        self._urns_by_key[key] = list(dict.fromkeys(urns))

    def record_slice_loaded(self, catalog_slice: CatalogSlice) -> None:
        """Record that `catalog_slice` was scrolled to completion.

        Never call this for a scroll that failed part way: its rows are still useful, but
        claiming coverage would turn every URN it never reached into a false absence.
        """
        if catalog_slice not in self._loaded_slices:
            self._loaded_slices.append(catalog_slice)

    def lookup(self, urn: str) -> Optional[List[str]]:
        """The URNs DataHub holds for `urn` ignoring case, or None if we do not know.

        ``[]`` is an answer — DataHub does not hold this URN — and None is the absence of
        one. Keeping them apart is the whole point of recording coverage: without it,
        every miss has to be treated as a gap and sent to the server.
        """
        key = lowercased_urn(urn)
        if key is None:
            # Not a dataset, so it has no casing to reconcile. A definite answer, not a
            # gap: querying could not help.
            return []
        entry = self._urns_by_key.get(key)
        if entry is not None:
            # Copied so callers cannot mutate the store.
            return list(entry)
        if any(catalog_slice.covers(key) for catalog_slice in self._loaded_slices):
            return []
        return None

    def close(self) -> None:
        self._urns_by_key.close()


# Keyed weakly so an index lives exactly as long as the graph it describes, and a graph
# that goes away takes its index with it.
_INDEXES: "MutableMapping[DataHubGraph, UrnAliasIndex]" = weakref.WeakKeyDictionary()


def get_urn_alias_index(graph: "DataHubGraph") -> UrnAliasIndex:
    """The one index for `graph`, created on first use.

    One per DataHub instance rather than per consumer, platform or platform instance: a
    lowercased dataset URN already encodes platform, platform_instance and env, so a
    single key space serves every scope without them bleeding into each other.

    Note what sharing implies about freshness. Every pipeline builds its own graph today,
    so an index dies with the run; anything that reuses a graph is choosing to serve a
    snapshot taken when its first load ran, and coverage means later lookups will not
    re-check the server.
    """
    index = _INDEXES.get(graph)
    if index is None:
        index = UrnAliasIndex()
        _INDEXES[graph] = index
        # Nothing else owns the index's sqlite temp directory, and DataHubGraph has no
        # hook to hang its release off, so tie it to the graph's lifetime here.
        weakref.finalize(graph, index.close)
    return index
