import logging
from dataclasses import dataclass
from typing import Collection, List, Optional

from datahub.metadata.urns import DataPlatformUrn, DatasetUrn
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.urns.error import InvalidUrnError

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
    """A region of DataHub's catalog, and the identity of the index one load fills.

    An index exists only for a scroll that finished, so a miss inside its slice is an
    answer rather than a gap. Not a scope to look a URN up by: ``platform_instance`` is
    fused into the dataset name with nothing marking where it ends, so it can only be
    tested against one — see ``covers``.
    """

    platform: str
    platform_instance: Optional[str]
    env: str

    def covers(self, key: str) -> bool:
        """Whether this slice's load would have fetched `key`, a lowercased dataset URN.

        Over-claims on the instance prefix: ``prod_wh.db.tbl`` may be instance ``prod_wh``
        or a table literally named that, and this claims both. See
        docs/dev_guides/lineage_urn_casing.md.
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
    """The dataset URNs each name resolves to, for one region of the catalog.

    Keyed by the lowercased URN, the value GMS indexes as a dataset's alias, so one row
    answers for every casing of a name. ``catalog_slice`` is the region a completed scroll
    filled it from, and None for the scratch index of per-URN answers, which covers nothing.
    """

    def __init__(self, catalog_slice: Optional[CatalogSlice] = None) -> None:
        self.catalog_slice = catalog_slice
        # Backed by sqlite: a bulk load holds a whole platform's URNs for the pipeline's
        # lifetime, roughly 500 bytes per dataset on disk.
        self._urns_by_key: FileBackedDict[List[str]] = FileBackedDict(
            tablename=_TABLE, cache_max_size=_CACHE_MAX_SIZE
        )

    def covers(self, urn: str) -> bool:
        """Whether this index's load fetched `urn`, so a miss here is an absence."""
        return self.catalog_slice is not None and covered_by(urn, [self.catalog_slice])

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

    def matches(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case, from what is known."""
        return self._entry(urn) or []

    def knows(self, urn: str) -> bool:
        """Whether this index answers for `urn`. A recorded absence counts."""
        return self._entry(urn) is not None

    def record(self, key: str, urns: List[str]) -> None:
        """Make `urns` the complete set stored under `key`; `[]` records known absent."""
        self._urns_by_key[key] = list(dict.fromkeys(urns))

    def _entry(self, urn: str) -> Optional[List[str]]:
        key = lowercased_urn(urn)
        # Copied so callers cannot mutate the store.
        entry = self._urns_by_key.get(key) if key is not None else None
        return list(entry) if entry is not None else None

    def close(self) -> None:
        self._urns_by_key.close()
