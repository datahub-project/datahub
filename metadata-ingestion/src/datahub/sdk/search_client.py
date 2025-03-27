from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    Optional,
)

from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub.metadata.urns import Urn
from datahub.sdk.search_filters import Filter, _EntityTypeFilter

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


def _has_any_field_filter(
    filter: Optional[Filter], field_name: str
) -> Optional[List[str]]:
    if filter is None:
        return None
    # TODO
    pass


def compile_filters(
    filter: Optional[Filter],
) -> Optional[List[Dict[str, List[RawSearchFilterRule]]]]:
    # TODO: Not every filter type is supported for every entity type.
    # If we can detect issues with the filters at compile time, we should
    # raise an error.

    # TODO: Make it clear that if no entity type filters are provided,
    # we will default to searching a predefined set of entity types.

    if filter is None:
        return None

    initial_filters = filter.compile()
    return [
        {"and": [rule.to_raw() for rule in andClause["and"]]}
        for andClause in initial_filters
    ]


class SearchClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def get_urns(
        self,
        query: Optional[str] = None,
        filter: Optional[Filter] = None,
    ) -> Iterable[Urn]:
        entity_types = _has_any_field_filter(
            filter, _EntityTypeFilter.ENTITY_TYPE_FIELD
        )

        # TODO: Add better limit / pagination support.
        for urn in self._client._graph.get_urns_by_filter(
            query=query,
            status=None,
            extra_or_filters=compile_filters(filter),
        ):
            yield Urn.from_string(urn)
