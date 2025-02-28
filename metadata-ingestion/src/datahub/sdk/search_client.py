from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
)

from datahub.metadata.urns import Urn
from datahub.sdk.search_filters import Filter

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class SearchClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def get_urns(
        self,
        query: Optional[str] = None,
        filter: Optional[Filter] = None,
    ) -> Iterable[Urn]:
        # TODO: not every filter type is supported for every entity type
        # if we can detect issues with the filters at compile time, we should
        # raise an error

        compiled_filters = None
        if filter:
            initial_filters = filter.compile()
            compiled_filters = [
                {"and": [rule.to_raw() for rule in andClause["and"]]}
                for andClause in initial_filters
            ]

        for urn in self._client._graph.get_urns_by_filter(
            query=query,
            extra_or_filters=compiled_filters,
        ):
            yield Urn.from_string(urn)
