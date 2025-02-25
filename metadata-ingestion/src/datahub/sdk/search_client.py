from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Optional,
    Sequence,
    TypedDict,
    Union,
)

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.configuration.pydantic_migration_helpers import PYDANTIC_VERSION_2
from datahub.ingestion.graph.filters import SearchFilterRule

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


AndSearchFilterRule = TypedDict("AndSearchFilterRule", {"and": List[SearchFilterRule]})
FullSearchFilterRule = TypedDict


class _BaseFilter(ConfigModel):
    class Config:
        allow_population_by_field_name = True
        populate_by_name = True

    @abc.abstractmethod
    def compile(self) -> FullSearchFilterRule:
        pass


class Platform(_BaseFilter):
    platform: List[str]

    # TODO: Add validator to convert string -> list of strings


class Domain(_BaseFilter):
    domain: List[str]


class Env(_BaseFilter):
    env: List[str]


class CustomCondition(_BaseFilter):
    """Represents a single field condition"""

    field: str
    operator: str
    values: List[str]


class And(_BaseFilter):
    """Represents an AND conjunction of filters"""

    and_: Sequence["Filter"] = pydantic.Field(alias="and")


class Or(_BaseFilter):
    """Represents an OR conjunction of filters"""

    or_: Sequence["Filter"] = pydantic.Field(alias="or")


class Not(_BaseFilter):
    """Represents a NOT filter"""

    not_: "Filter" = pydantic.Field(alias="not")


# TODO: With pydantic 2, we can use a RootModel with a
# discriminated union to make the error messages more informative.
Filter = Union[
    And,
    Or,
    Not,
    Platform,
    Domain,
    Env,
    CustomCondition,
]


# Required to resolve forward references
if PYDANTIC_VERSION_2:
    And.model_rebuild()  # type: ignore
    Or.model_rebuild()  # type: ignore
    Not.model_rebuild()  # type: ignore
else:
    And.update_forward_refs()
    Or.update_forward_refs()
    Not.update_forward_refs()


def load_filters(obj: Any) -> Filter:
    if PYDANTIC_VERSION_2:
        return pydantic.TypeAdapter(Filter).validate_python(obj)  # type: ignore
    else:
        return pydantic.parse_obj_as(Filter, obj)


class FilterDsl:
    @staticmethod
    def platform(platform: Union[str, List[str]]) -> Platform:
        return Platform(platform=[platform] if isinstance(platform, str) else platform)

    @staticmethod
    def domain(domain: Union[str, List[str]]) -> Domain:
        return Domain(domain=[domain] if isinstance(domain, str) else domain)

    @staticmethod
    def env(env: Union[str, List[str]]) -> Env:
        return Env(env=[env] if isinstance(env, str) else env)

    # TODO add custom filter
    # TODO custom properties filter

    @staticmethod
    def and_(*args: "Filter") -> And:
        return And(and_=list(args))

    @staticmethod
    def or_(*args: "Filter") -> Or:
        return Or(or_=list(args))

    @staticmethod
    def not_(arg: "Filter") -> Not:
        return Not(not_=arg)


class SearchClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def __call__(
        self,
        query: Optional[str] = None,
        filters: Optional[Filter] = None,
        # gql_fields: str = "urn",
    ) -> Iterable[Any]:
        # TODO compile filters

        compiled_filters = self._compile_filters(filters)

        return self._client._graph.get_urns_by_filter(
            # TODO: add entity types as a standard filter
            query=query,
            extra_or_filters=compiled_filters,
        )
