from __future__ import annotations

import abc
import json
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Iterator,
    List,
    Optional,
    Sequence,
    TypedDict,
    Union,
)

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.configuration.pydantic_migration_helpers import (
    PYDANTIC_SUPPORTS_CALLABLE_DISCRIMINATOR,
    PYDANTIC_VERSION_2,
)
from datahub.ingestion.graph.client import flexible_entity_type_to_graphql
from datahub.ingestion.graph.filters import (
    FilterOperator,
    RemovedStatusFilter,
    SearchFilterRule,
    _get_status_filter,
)
from datahub.metadata.schema_classes import EntityTypeName
from datahub.metadata.urns import ContainerUrn, DataPlatformUrn, DomainUrn

_AndSearchFilterRule = TypedDict(
    "_AndSearchFilterRule", {"and": List[SearchFilterRule]}
)
_OrFilters = List[_AndSearchFilterRule]


class _BaseFilter(ConfigModel):
    class Config:
        # We can't wrap this in a TYPE_CHECKING block because the pydantic plugin
        # doesn't recognize it properly. So unfortunately we'll need to live
        # with the deprecation warning w/ pydantic v2.
        allow_population_by_field_name = True
        if PYDANTIC_VERSION_2:
            populate_by_name = True

    @abc.abstractmethod
    def compile(self) -> _OrFilters: ...

    def dfs(self) -> Iterator[_BaseFilter]:
        yield self

    @classmethod
    def _field_discriminator(cls) -> str:
        if cls is _BaseFilter:
            raise ValueError("Cannot get discriminator for _BaseFilter")
        if PYDANTIC_VERSION_2:
            fields: dict = cls.model_fields  # type: ignore
        else:
            fields = cls.__fields__  # type: ignore

        # Assumes that there's only one field name per filter.
        # If that's not the case, this method should be overridden.
        if len(fields.keys()) != 1:
            raise ValueError(
                f"Found multiple fields that could be the discriminator for this filter: {list(fields.keys())}"
            )
        name, field = next(iter(fields.items()))
        return field.alias or name  # type: ignore


class _EntityTypeFilter(_BaseFilter):
    """Filter for specific entity types.

    If no entity type filter is specified, we will search all entity types in the
    default search set, mirroring the behavior of the DataHub UI.
    """

    ENTITY_TYPE_FIELD: ClassVar[str] = "_entityType"

    entity_type: List[str] = pydantic.Field(
        description="The entity type to filter on. Can be 'dataset', 'chart', 'dashboard', 'corpuser', 'dataProduct', etc.",
    )

    def _build_rule(self) -> SearchFilterRule:
        return SearchFilterRule(
            field=self.ENTITY_TYPE_FIELD,
            condition="EQUAL",
            values=[flexible_entity_type_to_graphql(t) for t in self.entity_type],
        )

    def compile(self) -> _OrFilters:
        return [{"and": [self._build_rule()]}]


class _EntitySubtypeFilter(_BaseFilter):
    entity_subtype: List[str] = pydantic.Field(
        description="The entity subtype to filter on. Can be 'Table', 'View', 'Source', etc. depending on the native platform's concepts.",
    )

    @pydantic.validator("entity_subtype", pre=True)
    def validate_entity_subtype(cls, v: str) -> List[str]:
        return [v] if not isinstance(v, list) else v

    def _build_rule(self) -> SearchFilterRule:
        return SearchFilterRule(
            field="typeNames",
            condition="EQUAL",
            values=self.entity_subtype,
        )

    def compile(self) -> _OrFilters:
        return [{"and": [self._build_rule()]}]


class _StatusFilter(_BaseFilter):
    """Filter for the status of entities during search.

    If not explicitly specified, the NOT_SOFT_DELETED status filter will be applied.
    """

    status: RemovedStatusFilter

    def _build_rule(self) -> Optional[SearchFilterRule]:
        return _get_status_filter(self.status)

    def compile(self) -> _OrFilters:
        rule = self._build_rule()
        if rule:
            return [{"and": [rule]}]
        else:
            # Our boolean algebra logic requires something here - returning [] would cause errors.
            return FilterDsl.true().compile()


class _PlatformFilter(_BaseFilter):
    platform: List[str]
    # TODO: Add validator to convert string -> list of strings

    @pydantic.validator("platform", each_item=True)
    def validate_platform(cls, v: str) -> str:
        # Subtle - we use the constructor instead of the from_string method
        # because coercion is acceptable here.
        return str(DataPlatformUrn(v))

    def _build_rule(self) -> SearchFilterRule:
        return SearchFilterRule(
            field="platform.keyword",
            condition="EQUAL",
            values=self.platform,
        )

    def compile(self) -> _OrFilters:
        return [{"and": [self._build_rule()]}]


class _DomainFilter(_BaseFilter):
    domain: List[str]

    @pydantic.validator("domain", each_item=True)
    def validate_domain(cls, v: str) -> str:
        return str(DomainUrn.from_string(v))

    def _build_rule(self) -> SearchFilterRule:
        return SearchFilterRule(
            field="domains",
            condition="EQUAL",
            values=self.domain,
        )

    def compile(self) -> _OrFilters:
        return [{"and": [self._build_rule()]}]


class _ContainerFilter(_BaseFilter):
    container: List[str]
    direct_descendants_only: bool = pydantic.Field(
        default=False,
        description="If true, only entities that are direct descendants of the container will be returned.",
    )

    @pydantic.validator("container", each_item=True)
    def validate_container(cls, v: str) -> str:
        return str(ContainerUrn.from_string(v))

    @classmethod
    def _field_discriminator(cls) -> str:
        return "container"

    def _build_rule(self) -> SearchFilterRule:
        if self.direct_descendants_only:
            return SearchFilterRule(
                field="container",
                condition="EQUAL",
                values=self.container,
            )
        else:
            return SearchFilterRule(
                field="browsePathV2",
                condition="CONTAIN",
                values=self.container,
            )

    def compile(self) -> _OrFilters:
        return [{"and": [self._build_rule()]}]


class _EnvFilter(_BaseFilter):
    # Note that not all entity types have an env (e.g. dashboards / charts).
    # If the env filter is specified, these will be excluded.
    env: List[str]

    def compile(self) -> _OrFilters:
        return [
            # For most entity types, we look at the origin field.
            {
                "and": [
                    SearchFilterRule(
                        field="origin",
                        condition="EQUAL",
                        values=self.env,
                    ),
                ]
            },
            # For containers, we now have an "env" property as of
            # https://github.com/datahub-project/datahub/pull/11214
            # Prior to this, we put "env" in the customProperties. But we're
            # not bothering with that here.
            {
                "and": [
                    SearchFilterRule(
                        field="env",
                        condition="EQUAL",
                        values=self.env,
                    ),
                ]
            },
        ]


class _CustomCondition(_BaseFilter):
    """Represents a single field condition."""

    field: str
    condition: FilterOperator
    values: List[str]

    def compile(self) -> _OrFilters:
        rule = SearchFilterRule(
            field=self.field,
            condition=self.condition,
            values=self.values,
        )
        return [{"and": [rule]}]

    @classmethod
    def _field_discriminator(cls) -> str:
        return "_custom"


class _And(_BaseFilter):
    """Represents an AND conjunction of filters."""

    and_: Sequence["Filter"] = pydantic.Field(alias="and")
    # TODO: Add validator to ensure that the "and" field is not empty

    def compile(self) -> _OrFilters:
        # The "and" operator must be implemented by doing a Cartesian product
        # of the OR clauses.
        # Example 1:
        # (A or B) and (C or D) ->
        # (A and C) or (A and D) or (B and C) or (B and D)
        # Example 2:
        # (A or B) and (C or D) and (E or F) ->
        # (A and C and E) or (A and C and F) or (A and D and E) or (A and D and F) or
        # (B and C and E) or (B and C and F) or (B and D and E) or (B and D and F)

        # Start with the first filter's OR clauses
        result = self.and_[0].compile()

        # For each subsequent filter
        for filter in self.and_[1:]:
            new_result = []
            # Get its OR clauses
            other_clauses = filter.compile()

            # Create Cartesian product
            for existing_clause in result:
                for other_clause in other_clauses:
                    # Merge the AND conditions from both clauses
                    new_result.append(self._merge_ands(existing_clause, other_clause))

            result = new_result

        return result

    @classmethod
    def _merge_ands(
        cls, a: _AndSearchFilterRule, b: _AndSearchFilterRule
    ) -> _AndSearchFilterRule:
        return {
            "and": [
                *a["and"],
                *b["and"],
            ]
        }

    def dfs(self) -> Iterator[_BaseFilter]:
        yield self
        for filter in self.and_:
            yield from filter.dfs()


class _Or(_BaseFilter):
    """Represents an OR conjunction of filters."""

    or_: Sequence["Filter"] = pydantic.Field(alias="or")
    # TODO: Add validator to ensure that the "or" field is not empty

    def compile(self) -> _OrFilters:
        merged_filter = []
        for filter in self.or_:
            merged_filter.extend(filter.compile())
        return merged_filter

    def dfs(self) -> Iterator[_BaseFilter]:
        yield self
        for filter in self.or_:
            yield from filter.dfs()


class _Not(_BaseFilter):
    """Represents a NOT filter."""

    not_: "Filter" = pydantic.Field(alias="not")

    @pydantic.validator("not_", pre=False)
    def validate_not(cls, v: "Filter") -> "Filter":
        inner_filter = v.compile()
        if len(inner_filter) != 1:
            raise ValueError(
                "Cannot negate a filter with multiple OR clauses [not yet supported]"
            )
        return v

    def compile(self) -> _OrFilters:
        # TODO: Eventually we'll want to implement a full DNF normalizer.
        # https://en.wikipedia.org/wiki/Disjunctive_normal_form#Conversion_to_DNF

        inner_filter = self.not_.compile()
        assert len(inner_filter) == 1  # validated above

        # ¬(A and B) -> (¬A) OR (¬B)
        and_filters = inner_filter[0]["and"]
        final_filters: _OrFilters = []
        for rule in and_filters:
            final_filters.append({"and": [rule.negate()]})

        return final_filters

    def dfs(self) -> Iterator[_BaseFilter]:
        yield self
        yield from self.not_.dfs()


def _filter_discriminator(v: Any) -> Optional[str]:
    if isinstance(v, _BaseFilter):
        return v._field_discriminator()

    if not isinstance(v, dict):
        return None

    keys = list(v.keys())
    if len(keys) == 1:
        return keys[0]
    elif set(keys).issuperset({"container"}):
        return _ContainerFilter._field_discriminator()
    elif set(keys).issuperset({"field", "condition"}):
        return _CustomCondition._field_discriminator()

    return None


def _parse_and_like_filter(value: Any) -> Any:
    # Do not parse if filter is already of type and/or/not or a custom condition
    # also do not parse container filter if direct_descendants_only is specified
    if (
        isinstance(value, dict)
        and not set(value.keys()).intersection(
            {"and", "or", "not", "field", "condition", "direct_descendants_only"}
        )
        and len(value) > 1
    ):
        return {"and": [{k: v} for k, v in value.items()]}

    return value


if TYPE_CHECKING or not PYDANTIC_SUPPORTS_CALLABLE_DISCRIMINATOR:
    # The `not TYPE_CHECKING` bit is required to make the linter happy,
    # since we currently only run mypy with pydantic v1.
    Filter = Union[
        _And,
        _Or,
        _Not,
        _EntityTypeFilter,
        _EntitySubtypeFilter,
        _StatusFilter,
        _PlatformFilter,
        _DomainFilter,
        _ContainerFilter,
        _EnvFilter,
        _CustomCondition,
    ]

    _And.update_forward_refs()
    _Or.update_forward_refs()
    _Not.update_forward_refs()
else:
    from pydantic import Discriminator, Tag

    def _parse_json_from_string(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        else:
            return value

    # TODO: Once we're fully on pydantic 2, we can use a RootModel here.
    # That way we'd be able to attach methods to the Filter type.
    # e.g. replace load_filters(...) with Filter.load(...)
    Filter = Annotated[
        Annotated[
            Union[
                Annotated[_And, Tag(_And._field_discriminator())],
                Annotated[_Or, Tag(_Or._field_discriminator())],
                Annotated[_Not, Tag(_Not._field_discriminator())],
                Annotated[
                    _EntityTypeFilter, Tag(_EntityTypeFilter._field_discriminator())
                ],
                Annotated[
                    _EntitySubtypeFilter,
                    Tag(_EntitySubtypeFilter._field_discriminator()),
                ],
                Annotated[_StatusFilter, Tag(_StatusFilter._field_discriminator())],
                Annotated[_PlatformFilter, Tag(_PlatformFilter._field_discriminator())],
                Annotated[_DomainFilter, Tag(_DomainFilter._field_discriminator())],
                Annotated[
                    _ContainerFilter, Tag(_ContainerFilter._field_discriminator())
                ],
                Annotated[_EnvFilter, Tag(_EnvFilter._field_discriminator())],
                Annotated[
                    _CustomCondition, Tag(_CustomCondition._field_discriminator())
                ],
            ],
            Discriminator(_filter_discriminator),
        ],
        pydantic.BeforeValidator(_parse_and_like_filter),
        pydantic.BeforeValidator(_parse_json_from_string),
    ]

    # Required to resolve forward references to "Filter"
    _And.model_rebuild()  # type: ignore
    _Or.model_rebuild()  # type: ignore
    _Not.model_rebuild()  # type: ignore


def load_filters(obj: Any) -> Filter:
    if PYDANTIC_VERSION_2:
        return pydantic.TypeAdapter(Filter).validate_python(obj)  # type: ignore
    else:
        return pydantic.parse_obj_as(Filter, obj)  # type: ignore


# We need FilterDsl for two reasons:
# 1. To provide wrapper methods around lots of filters while avoid bloating the
#    yaml spec.
# 2. Pydantic models in general don't support positional arguments, making the
#    calls feel repetitive (e.g. Platform(platform=...)).
#    See https://github.com/pydantic/pydantic/issues/6792
#    We also considered using dataclasses / pydantic dataclasses, but
#    ultimately decided that they didn't quite suit our requirements,
#    particularly with regards to the field aliases for and/or/not.
class FilterDsl:
    @staticmethod
    def and_(*args: "Filter") -> _And:
        return _And(and_=list(args))

    @staticmethod
    def or_(*args: "Filter") -> _Or:
        return _Or(or_=list(args))

    @staticmethod
    def not_(arg: "Filter") -> _Not:
        return _Not(not_=arg)

    @staticmethod
    def true() -> "Filter":
        return _CustomCondition(
            field="urn",
            condition="EXISTS",
            values=[],
        )

    @staticmethod
    def false() -> "Filter":
        return FilterDsl.not_(FilterDsl.true())

    @staticmethod
    def entity_type(
        entity_type: Union[EntityTypeName, Sequence[EntityTypeName]],
    ) -> _EntityTypeFilter:
        return _EntityTypeFilter(
            entity_type=(
                [entity_type] if isinstance(entity_type, str) else list(entity_type)
            )
        )

    @staticmethod
    def entity_subtype(
        entity_subtype: Union[str, Sequence[str]],
    ) -> _EntitySubtypeFilter:
        return _EntitySubtypeFilter(
            entity_subtype=entity_subtype,
        )

    @staticmethod
    def platform(platform: Union[str, Sequence[str]], /) -> _PlatformFilter:
        return _PlatformFilter(
            platform=[platform] if isinstance(platform, str) else platform
        )

    # TODO: Add a platform_instance filter

    @staticmethod
    def domain(domain: Union[str, Sequence[str]], /) -> _DomainFilter:
        return _DomainFilter(domain=[domain] if isinstance(domain, str) else domain)

    @staticmethod
    def container(
        container: Union[str, Sequence[str]],
        /,
        *,
        direct_descendants_only: bool = False,
    ) -> _ContainerFilter:
        return _ContainerFilter(
            container=[container] if isinstance(container, str) else container,
            direct_descendants_only=direct_descendants_only,
        )

    @staticmethod
    def env(env: Union[str, Sequence[str]], /) -> _EnvFilter:
        return _EnvFilter(env=[env] if isinstance(env, str) else env)

    @staticmethod
    def has_custom_property(key: str, value: str) -> _CustomCondition:
        return _CustomCondition(
            field="customProperties",
            condition="EQUAL",
            values=[f"{key}={value}"],
        )

    @staticmethod
    def soft_deleted(status: RemovedStatusFilter) -> _StatusFilter:
        return _StatusFilter(status=status)

    # TODO: Add a soft-deletion status filter
    # TODO: add a container / browse path filter
    # TODO add shortcut for custom filters

    @staticmethod
    def custom_filter(
        field: str, condition: FilterOperator, values: Sequence[str]
    ) -> _CustomCondition:
        return _CustomCondition(
            field=field,
            condition=condition,
            values=values,
        )
