import logging
from typing import TYPE_CHECKING, Dict, List

from datahub.ingestion.graph.filters import RawSearchFilter, SearchFilterRule
from datahub.utilities.urn_alias.index import lowercased_urn

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# The `aliases` aspect's search field. Filter values are OR'd, so one query carries a batch.
_LOWERCASED_URN_FIELD = "lowercasedUrn"

_URN_FIELD = "urn"

_DATASET_ENTITY_TYPE = "dataset"

# Where GMS started maintaining the dataset `aliases` aspect this reads.
_MIN_CLOUD_VERSION = (2, 2, 0)
_MIN_OSS_VERSION = (1, 8, 0)


def search_aliases(graph: "DataHubGraph", keys: List[str]) -> Dict[str, List[str]]:
    """Every stored casing of each of `keys`, from one search. Raises if it fails.

    Exhaustive, so an empty list for a key is a genuine absence. Searched by urn as well as
    by alias: GMS writes aliases asynchronously, so one that has not landed yet is findable
    only under its own urn.
    """
    or_filters: RawSearchFilter = [
        {
            "and": [
                SearchFilterRule(field=field, condition="EQUAL", values=keys).to_raw()
            ]
        }
        for field in (_LOWERCASED_URN_FIELD, _URN_FIELD)
    ]
    stored_urns = graph.get_urns_by_filter(
        entity_types=[_DATASET_ENTITY_TYPE], extra_or_filters=or_filters
    )
    matches_by_key: Dict[str, List[str]] = {key: [] for key in keys}
    # Deduped: a scroll that repeated a urn would otherwise read as a casing collision.
    for stored_urn in dict.fromkeys(stored_urns):
        key = lowercased_urn(stored_urn)
        if key in matches_by_key:
            matches_by_key[key].append(stored_urn)
    return matches_by_key


def gms_maintains_urn_aliases(graph: "DataHubGraph") -> bool:
    """Whether the server maintains the dataset `aliases` aspect this lookup reads.

    The gate on the whole feature: without aliases there is no way to reach a stored
    casing, and approximating one would report healthy lineage as broken.
    """
    config = graph.server_config
    minimum = _MIN_CLOUD_VERSION if config.is_datahub_cloud else _MIN_OSS_VERSION
    return config.is_version_at_least(*minimum)
