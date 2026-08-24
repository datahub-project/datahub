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
# Documented as a prerequisite in docs/dev_guides/lineage_urn_casing.md; keep both in sync.
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


class UrnAliasResolver:
    """An index of the casings DataHub stores for a dataset name.

    Answers which stored URNs a reference matches, not which one it means.

    Rows are keyed by the lowercased URN, the value GMS indexes as a dataset's alias, so one
    row answers for every casing of a name.

    With `graph`, a row comes from an exhaustive search for that key, so a miss is an
    absence. Without one the rows come from a bulk load, whose miss means only that the load
    did not hold it — the caller asks a graph-backed resolver instead.
    """

    def __init__(self, graph: "Optional[DataHubGraph]" = None) -> None:
        # Backed by sqlite rather than memory: a bulk load holds a whole platform for the
        # pipeline's lifetime.
        self._graph = graph
        self._urns_by_key: FileBackedDict[List[str]] = FileBackedDict(tablename=_TABLE)

    def add(self, urn: str) -> None:
        """Record `urn` as stored in DataHub. For the bulk load only."""
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
        if entry is not None:
            return entry
        if self._graph is None:
            return []
        # Exhaustive, so `[]` is a real absence and is cached as one.
        entry = self._graph.get_dataset_urns_ignoring_case(key)
        self._urns_by_key[key] = entry
        return entry

    def urn_count(self) -> int:
        """How many names are held; one key answers for every casing of a name."""
        return len(self._urns_by_key)

    def close(self) -> None:
        self._urns_by_key.close()


def maintains_dataset_aliases(graph: "DataHubGraph") -> bool:
    """Whether the server maintains the dataset `aliases` aspect resolution reads.

    Gates the whole feature: without aliases a stored casing is unreachable.
    """
    config = graph.server_config
    minimum = _MIN_CLOUD_VERSION if config.is_datahub_cloud else _MIN_OSS_VERSION
    try:
        return config.is_version_at_least(*minimum)
    except ValueError:
        # A build off a git SHA reports an unparseable version, so it is treated as too
        # old. The caller's report warning carries the version read and the one needed.
        logger.debug(
            f"Cannot parse the DataHub server version {config.service_version!r}.",
            exc_info=True,
        )
        return False


def required_server_version(graph: "DataHubGraph") -> str:
    """The lowest server version that maintains the dataset `aliases` aspect."""
    config = graph.server_config
    if config.is_datahub_cloud:
        return f"DataHub Cloud {'.'.join(map(str, _MIN_CLOUD_VERSION))}"
    return f"DataHub {'.'.join(map(str, _MIN_OSS_VERSION))}"
