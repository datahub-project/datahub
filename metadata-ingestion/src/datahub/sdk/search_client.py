from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Optional,
    Union,
)

import datahub.sdk.search_filters as filters
from datahub.sdk.search_filters import Filter

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class FilterDsl:
    @staticmethod
    def and_(*args: "Filter") -> filters.And:
        return filters.And(and_=list(args))

    @staticmethod
    def or_(*args: "Filter") -> filters.Or:
        return filters.Or(or_=list(args))

    @staticmethod
    def not_(arg: "Filter") -> filters.Not:
        return filters.Not(not_=arg)

    @staticmethod
    def platform(platform: Union[str, List[str]]) -> filters.Platform:
        return filters.Platform(
            platform=[platform] if isinstance(platform, str) else platform
        )

    @staticmethod
    def domain(domain: Union[str, List[str]]) -> filters.Domain:
        return filters.Domain(domain=[domain] if isinstance(domain, str) else domain)

    @staticmethod
    def env(env: Union[str, List[str]]) -> filters.Env:
        return filters.Env(env=[env] if isinstance(env, str) else env)

    @staticmethod
    def has_custom_property(key: str, value: str) -> filters.CustomCondition:
        return filters.CustomCondition(
            field="customProperties",
            condition="EQUAL",
            values=[f"{key}={value}"],
        )

    # TODO add custom filter


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
