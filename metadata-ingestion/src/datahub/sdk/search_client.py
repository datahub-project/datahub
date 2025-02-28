from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Sequence,
)

from datahub.metadata.schema_classes import EntityTypeName
from datahub.metadata.urns import Urn
from datahub.sdk.search_filters import Filter

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class SearchClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def __call__(
        self,
        query: Optional[str] = None,
        entity_types: Optional[Sequence[EntityTypeName]] = None,
        filter: Optional[Filter] = None,
    ) -> Iterable[Urn]:
        compiled_filters = None
        if filter:
            initial_filters = filter.compile()
            compiled_filters = [
                {"and": [rule.to_raw() for rule in andClause["and"]]}
                for andClause in initial_filters
            ]
        # TODO: only some entity types are actually searchable; we should limit
        # the allowed set of entity types here?

        # TODO: not every filter type is supported for every entity type
        # if we can detect issues with the query at compile time, we should
        # raise an error

        for urn in self._client._graph.get_urns_by_filter(
            query=query,
            entity_types=entity_types,
            extra_or_filters=compiled_filters,
        ):
            yield Urn.from_string(urn)
