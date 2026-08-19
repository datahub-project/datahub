import logging
from typing import TYPE_CHECKING, List, Optional

from datahub.metadata.urns import DatasetUrn
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

_TABLE = "urn_aliases"

# Where GMS started maintaining the dataset `aliases` aspect resolution reads.
_MIN_CLOUD_VERSION = (2, 2, 0)
_MIN_OSS_VERSION = (1, 8, 0)


def lowercased_urn(urn: str) -> Optional[str]:
    """`urn` with the dataset name lowercased, or None for a non-dataset URN."""
    # Mirrors AliasesUtils.lowercaseDatasetUrn, the value GMS indexes in `aliases`.
    try:
        dataset = DatasetUrn.from_string(urn)
    except InvalidUrnError:
        return None
    lowercased = dataset.name.lower()
    if lowercased == dataset.name:
        return urn
    return str(DatasetUrn(platform=dataset.platform, name=lowercased, env=dataset.env))


def _has_lowercased_name(urn: str) -> bool:
    return lowercased_urn(urn) == urn


class UrnAliasResolver:
    """Resolves a dataset URN to the URN DataHub stores for it, ignoring case.

    Keyed by the lowercased URN, the value GMS indexes as a dataset's alias, so one row
    answers for every casing of a name.

    With `graph`, an unknown name is fetched. Without it the resolver answers from what it
    holds alone, so a miss is an absence — which is only true of one filled by a bulk load
    that ran to completion, and is why `provide_urn_alias_resolver` returns None otherwise.
    """

    def __init__(self, graph: "Optional[DataHubGraph]" = None) -> None:
        # Backed by sqlite: a bulk load holds a whole platform's URNs for the pipeline's
        # lifetime, roughly 500 bytes per dataset on disk.
        self._graph = graph
        self._urns_by_key: FileBackedDict[List[str]] = FileBackedDict(tablename=_TABLE)

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

    def find_match(self, urn: str) -> List[str]:
        """Stored URNs matching `urn` ignoring case. Raises if a fetch fails."""
        key = lowercased_urn(urn)
        if key is None:
            # Not a dataset, so it has no casing to reconcile.
            return []
        entry = self._urns_by_key.get(key)
        if entry is None:
            if self._graph is None:
                return []
            # The search is exhaustive, so `[]` is a genuine absence and is recorded as
            # one — the same name is not asked about twice.
            entry = self._graph.get_dataset_urns_ignoring_case(key)
            self._urns_by_key[key] = entry
        return entry

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
        self._urns_by_key.close()


def maintains_dataset_aliases(graph: "DataHubGraph") -> bool:
    """Whether the server maintains the dataset `aliases` aspect resolution reads.

    The gate on the whole feature: without aliases there is no way to reach a stored
    casing, and approximating one would report healthy lineage as broken.
    """
    config = graph.server_config
    minimum = _MIN_CLOUD_VERSION if config.is_datahub_cloud else _MIN_OSS_VERSION
    return config.is_version_at_least(*minimum)
