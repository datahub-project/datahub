import logging
from typing import TYPE_CHECKING, List

from datahub.ingestion.graph.filters import RawSearchFilter, SearchFilterRule

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


def search_aliases(graph: "DataHubGraph", key: str) -> List[str]:
    """Every stored casing of `key`, a lowercased dataset URN. Raises if the search fails.

    Exhaustive, so an empty list is a genuine absence. Searched by urn as well as by alias:
    GMS writes aliases asynchronously, so one that has not landed yet is findable only
    under its own urn.
    """
    or_filters: RawSearchFilter = [
        {
            "and": [
                SearchFilterRule(field=field, condition="EQUAL", values=[key]).to_raw()
            ]
        }
        for field in (_LOWERCASED_URN_FIELD, _URN_FIELD)
    ]
    stored_urns = graph.get_urns_by_filter(
        entity_types=[_DATASET_ENTITY_TYPE], extra_or_filters=or_filters
    )
    # Deduped: a scroll that repeated a urn would otherwise read as a casing collision.
    return list(dict.fromkeys(stored_urns))


def gms_maintains_urn_aliases(graph: "DataHubGraph") -> bool:
    """Whether the server maintains the dataset `aliases` aspect this lookup reads.

    The gate on the whole feature: without aliases there is no way to reach a stored
    casing, and approximating one would report healthy lineage as broken.
    """
    config = graph.server_config
    minimum = _MIN_CLOUD_VERSION if config.is_datahub_cloud else _MIN_OSS_VERSION
    return config.is_version_at_least(*minimum)
