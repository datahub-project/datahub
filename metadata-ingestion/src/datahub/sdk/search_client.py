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


class SearchClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    @classmethod
    def _compile_filters(
        cls, filter: Optional[Filter]
    ) -> Optional[List[Dict[str, List[RawSearchFilterRule]]]]:
        # TODO: not every filter type is supported for every entity type
        # if we can detect issues with the filters at compile time, we should
        # raise an error

        if filter is None:
            return None

        initial_filters = filter.compile()
        return [
            {"and": [rule.to_raw() for rule in andClause["and"]]}
            for andClause in initial_filters
        ]

    def get_urns(
        self,
        query: Optional[str] = None,
        filter: Optional[Filter] = None,
    ) -> Iterable[Urn]:
        for urn in self._client._graph.get_urns_by_filter(
            query=query,
            extra_or_filters=self._compile_filters(filter),
        ):
            yield Urn.from_string(urn)
