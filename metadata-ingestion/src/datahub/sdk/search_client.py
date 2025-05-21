from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from datahub.ingestion.graph.filters import RawSearchFilter, RemovedStatusFilter
from datahub.metadata.urns import Urn
from datahub.sdk.search_filters import (
    Filter,
    FilterDsl,
    _EntityTypeFilter,
    _OrFilters,
    _StatusFilter,
)

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


_FilterType = TypeVar("_FilterType", bound=Filter)


def _typed_dfs(
    filter: Optional[_FilterType], type: Type[_FilterType]
) -> Optional[List[_FilterType]]:
    if filter is None:
        return None

    found: Optional[List[_FilterType]] = None
    for f in filter.dfs():
        if isinstance(f, type):
            if found is None:
                found = []
            found.append(f)
    return found


def compile_filters(
    filter: Optional[Filter],
) -> Tuple[Optional[List[str]], RawSearchFilter]:
    # TODO: Not every filter type is supported for every entity type.
    # If we can detect issues with the filters at compile time, we should
    # raise an error.

    existing_soft_deleted_filter = _typed_dfs(filter, _StatusFilter)
    if existing_soft_deleted_filter is None:
        soft_deleted_filter = FilterDsl.soft_deleted(
            RemovedStatusFilter.NOT_SOFT_DELETED
        )
        if filter is None:
            filter = soft_deleted_filter
        else:
            filter = FilterDsl.and_(filter, soft_deleted_filter)

    # This should be safe - if filter were None coming in, then we would replace it
    # with the soft-deleted filter.
    assert filter is not None

    initial_filters = filter.compile()

    compiled_filters: RawSearchFilter = [
        {"and": [rule.to_raw() for rule in andClause["and"]]}
        for andClause in initial_filters
    ]

    entity_types = compute_entity_types(initial_filters)

    return entity_types, compiled_filters


def compute_entity_types(
    filters: _OrFilters,
) -> Optional[List[str]]:
    found_filters = False
    found_positive_filters = False
    entity_types: List[str] = []
    for ands in filters:
        for clause in ands["and"]:
            if clause.field == _EntityTypeFilter.ENTITY_TYPE_FIELD:
                found_filters = True
                if not clause.negated:
                    found_positive_filters = True

                entity_types.extend(clause.values)

    if not found_filters:
        # If we didn't find any filters, use None so we use the default set.
        return None

    if not found_positive_filters:
        # If we only found negated filters, then it's probably a query like
        # "find me all entities except for dashboards". In that case, we
        # still want to use the default set.
        return None

    return entity_types


class SearchClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def get_urns(
        self,
        query: Optional[str] = None,
        filter: Optional[Filter] = None,
    ) -> Iterable[Urn]:
        # TODO: Add better limit / pagination support.
        types, compiled_filters = compile_filters(filter)
        for urn in self._client._graph.get_urns_by_filter(
            query=query,
            status=None,
            extra_or_filters=compiled_filters,
            entity_types=types,
        ):
            yield Urn.from_string(urn)
