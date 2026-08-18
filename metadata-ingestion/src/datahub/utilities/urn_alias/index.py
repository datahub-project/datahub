import logging
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Collection, List, MutableMapping, Optional

from datahub.metadata.urns import DataPlatformUrn, DatasetUrn
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

_TABLE = "urn_aliases"

# Sized to the distinct upstreams a source references rather than to the catalog: a BI
# tool names the same warehouse table across many charts, so the repeats answer from
# memory. Costs a few MB, well above the 900 a schema cache is tuned for.
_CACHE_MAX_SIZE = 10_000


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
        if not self.platform_instance:
            # No instance filter on the scroll means every instance was loaded — not
            # that only instance-less datasets were.
            return True
        # A dataset in instance X always has the name `X.<rest>`.
        return dataset.name.startswith(f"{self.platform_instance.lower()}.")


def covered_by(urn: str, slices: Collection[CatalogSlice]) -> bool:
    """Whether `urn` falls inside any of `slices`.

    Lowercased here before testing, so callers pass whatever casing they hold.
    """
    key = lowercased_urn(urn)
    return key is not None and any(
        catalog_slice.covers(key) for catalog_slice in slices
    )


class UrnAliasIndex:
    """Stores the dataset URNs a key resolves to, plus which catalog slices were fully
    loaded.

    The key is supplied by the lookup that fills it, which is the only thing that reads it
    back: the alias search keys by lowercased URN, the casing probe by exact URN. One
    lookup is active per server, so only one kind of key is ever in here.

    Purely local, shared per DataHub instance, and reached only through UrnAliasResolver.
    """

    def __init__(self) -> None:
        # Backed by sqlite: a bulk load holds a whole platform's URNs for the pipeline's
        # lifetime, roughly 500 bytes per dataset on disk.
        self._urns_by_key: FileBackedDict[List[str]] = FileBackedDict(
            tablename=_TABLE, cache_max_size=_CACHE_MAX_SIZE
        )
        # One entry per completed scroll at most, so kept in memory.
        self.loaded_slices: List[CatalogSlice] = []

    def add(self, key: str, urn: str) -> None:
        """Add `urn` under `key`, keeping any URNs already there."""
        entry = self._urns_by_key.get(key)
        if entry is None:
            self._urns_by_key[key] = [urn]
        elif urn not in entry:
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


# Keyed weakly so an index lives exactly as long as the graph it describes, and a graph
# that goes away takes its index with it.
_INDEXES: "MutableMapping[DataHubGraph, UrnAliasIndex]" = weakref.WeakKeyDictionary()

# Off by default: filling costs a sqlite round trip and a few hundred bytes per dataset,
# and most bulk loads want schemas for SQL parsing and never read the index.
_FILL_URN_ALIAS_INDEX = False


def set_fill_urn_alias_index(value: bool) -> None:
    """Declare whether bulk loads should index the URNs they scroll.

    Must precede the first bulk load: a load already cached is never repeated, so setting
    this afterwards leaves that slice unindexed.
    """
    global _FILL_URN_ALIAS_INDEX
    _FILL_URN_ALIAS_INDEX = value


def should_fill_urn_alias_index() -> bool:
    """Whether bulk loads index what they scroll."""
    return _FILL_URN_ALIAS_INDEX


def shared_index(graph: "DataHubGraph") -> UrnAliasIndex:
    """The one index for `graph`, created on first use. Reach it through a resolver.

    One per DataHub instance rather than per consumer, platform or platform instance: a
    dataset URN already encodes platform, platform_instance and env, so a single key space
    serves every scope without them bleeding into each other.

    Every pipeline builds its own graph today, so an index dies with the run; anything that
    reuses a graph is choosing to serve a snapshot taken when its first load ran.
    """
    index = _INDEXES.get(graph)
    if index is None:
        index = UrnAliasIndex()
        _INDEXES[graph] = index
        # Nothing else owns the index's sqlite temp directory, and DataHubGraph has no
        # hook to hang its release off, so tie it to the graph's lifetime here.
        weakref.finalize(graph, index.close)
    return index
