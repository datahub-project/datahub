from __future__ import annotations

import abc
from typing import (
    Any,
    List,
    Sequence,
    TypedDict,
    Union,
)

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.configuration.pydantic_migration_helpers import PYDANTIC_VERSION_2
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.urns import DataPlatformUrn, DomainUrn

AndSearchFilterRule = TypedDict("AndSearchFilterRule", {"and": List[SearchFilterRule]})
OrFilters = List[AndSearchFilterRule]


class _BaseFilter(ConfigModel):
    class Config:
        allow_population_by_field_name = True
        populate_by_name = True

    @abc.abstractmethod
    def compile(self) -> OrFilters:
        pass


class Platform(_BaseFilter):
    platform: List[str]
    # TODO: Add validator to convert string -> list of strings

    def _build_rule(self) -> SearchFilterRule:
        return SearchFilterRule(
            field="platform.keyword",
            condition="EQUAL",
            values=[DataPlatformUrn(platform).urn() for platform in self.platform],
        )

    def compile(self) -> OrFilters:
        return [{"and": [self._build_rule()]}]


class Domain(_BaseFilter):
    domain: List[str]

    def _build_rule(self) -> SearchFilterRule:
        for domain in self.domain:
            assert DomainUrn.from_string(domain)
        return SearchFilterRule(
            field="domains",
            condition="EQUAL",
            values=self.domain,
        )

    def compile(self) -> OrFilters:
        return [{"and": [self._build_rule()]}]


class Env(_BaseFilter):
    # Note that not all entity types have an env (e.g. dashboards / charts).
    # If the env filter is specified, these will be excluded.
    env: List[str]

    def compile(self) -> OrFilters:
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


class CustomCondition(_BaseFilter):
    """Represents a single field condition"""

    field: str
    condition: str
    values: List[str]

    def compile(self) -> OrFilters:
        rule = SearchFilterRule(
            field=self.field,
            condition=self.condition,
            values=self.values,
        )
        return [{"and": [rule]}]


class And(_BaseFilter):
    """Represents an AND conjunction of filters"""

    and_: Sequence["Filter"] = pydantic.Field(alias="and")
    # TODO: Add validator to ensure that the "and" field is not empty

    def compile(self) -> OrFilters:
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
        cls, a: AndSearchFilterRule, b: AndSearchFilterRule
    ) -> AndSearchFilterRule:
        return {
            "and": [
                *a["and"],
                *b["and"],
            ]
        }


class Or(_BaseFilter):
    """Represents an OR conjunction of filters"""

    or_: Sequence["Filter"] = pydantic.Field(alias="or")
    # TODO: Add validator to ensure that the "or" field is not empty

    def compile(self) -> OrFilters:
        merged_filter = []
        for filter in self.or_:
            merged_filter.extend(filter.compile())
        return merged_filter


class Not(_BaseFilter):
    """Represents a NOT filter"""

    not_: "Filter" = pydantic.Field(alias="not")

    def compile(self) -> OrFilters:
        # TODO: Eventually we'll want to implement a full DNF normalizer.
        # https://en.wikipedia.org/wiki/Disjunctive_normal_form#Conversion_to_DNF

        inner_filter = self.not_.compile()
        if len(inner_filter) != 1:
            raise ValueError("Cannot negate a filter with multiple OR clauses")

        # ¬(A and B) -> (¬A) OR (¬B)
        and_filters = inner_filter[0]["and"]
        final_filters: OrFilters = []
        for rule in and_filters:
            final_filters.append({"and": [rule.negate()]})

        return final_filters


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
        return pydantic.parse_obj_as(Filter, obj)  # type: ignore
