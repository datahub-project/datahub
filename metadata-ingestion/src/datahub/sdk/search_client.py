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
from datahub.sdk.search_filters import Filter

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


def compile_filters(
    filter: Optional[Filter],
) -> Optional[List[Dict[str, List[RawSearchFilterRule]]]]:
    # TODO: Not every filter type is supported for every entity type.
    # If we can detect issues with the filters at compile time, we should
    # raise an error.

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
        # TODO: Add better limit / pagination support.
        for urn in self._client._graph.get_urns_by_filter(
            query=query,
            extra_or_filters=compile_filters(filter),
        ):
            yield Urn.from_string(urn)
