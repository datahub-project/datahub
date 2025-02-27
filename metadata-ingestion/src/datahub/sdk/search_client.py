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
    def and_(*args: "Filter") -> filters._And:
        return filters._And(and_=list(args))

    @staticmethod
    def or_(*args: "Filter") -> filters._Or:
        return filters._Or(or_=list(args))

    @staticmethod
    def not_(arg: "Filter") -> filters._Not:
        return filters._Not(not_=arg)

    @staticmethod
    def platform(platform: Union[str, List[str]]) -> filters._PlatformFilter:
        return filters._PlatformFilter(
            platform=[platform] if isinstance(platform, str) else platform
        )

    @staticmethod
    def domain(domain: Union[str, List[str]]) -> filters._DomainFilter:
        return filters._DomainFilter(
            domain=[domain] if isinstance(domain, str) else domain
        )

    @staticmethod
    def env(env: Union[str, List[str]]) -> filters._EnvFilter:
        return filters._EnvFilter(env=[env] if isinstance(env, str) else env)

    @staticmethod
    def has_custom_property(key: str, value: str) -> filters._CustomCondition:
        return filters._CustomCondition(
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
