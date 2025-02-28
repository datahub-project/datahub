from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
)

from datahub.sdk.search_filters import Filter

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class SearchClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def __call__(
        self,
        query: Optional[str] = None,
        filter: Optional[Filter] = None,
        # gql_fields: str = "urn",
    ) -> Iterable[Any]:
        compiled_filters = None
        if filter:
            initial_filters = filter.compile()
            compiled_filters = [
                {"and": [rule.to_raw() for rule in andClause["and"]]}
                for andClause in initial_filters
            ]

        return self._client._graph.get_urns_by_filter(
            # TODO: add entity types as a standard filter
            query=query,
            extra_or_filters=compiled_filters,
        )
