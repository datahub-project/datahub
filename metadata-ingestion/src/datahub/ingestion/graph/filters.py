import dataclasses
import enum
import warnings
from typing import Dict, List, Literal, Optional, Union

from typing_extensions import TypeAlias

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.errors import SearchFilterWarning
from datahub.utilities.urns.urn import guess_entity_type

RawSearchFilterRule: TypeAlias = Dict[str, Union[str, bool, List[str]]]

# This is a list of OR filters, each of which is a list of AND filters.
# This can be put directly into the orFilters parameter in GraphQL.
RawSearchFilter: TypeAlias = List[Dict[Literal["and"], List[RawSearchFilterRule]]]

# Mirrors our GraphQL enum: https://docs.datahub.com/docs/graphql/enums#filteroperator
FilterOperator: TypeAlias = Literal[
    "CONTAIN",
    "EQUAL",
    "IEQUAL",
    "IN",
    "EXISTS",
    "GREATER_THAN",
    "GREATER_THAN_OR_EQUAL_TO",
    "LESS_THAN",
    "LESS_THAN_OR_EQUAL_TO",
    "START_WITH",
    "END_WITH",
    "DESCENDANTS_INCL",
    "ANCESTORS_INCL",
    "RELATED_INCL",
]


@dataclasses.dataclass
class SearchFilterRule:
    field: str
    condition: FilterOperator
    values: List[str]
    negated: bool = False

    def to_raw(self) -> RawSearchFilterRule:
        rule: RawSearchFilterRule = {
            "field": self.field,
            "condition": self.condition,
            "values": self.values,
        }
        if self.negated:
            rule["negated"] = True
        return rule

    def negate(self) -> "SearchFilterRule":
        return SearchFilterRule(
            field=self.field,
            condition=self.condition,
            values=self.values,
            negated=not self.negated,
        )


class RemovedStatusFilter(enum.Enum):
    """Filter for the status of entities during search."""

    NOT_SOFT_DELETED = "NOT_SOFT_DELETED"
    """Search only entities that have not been marked as deleted."""

    ALL = "ALL"
    """Search all entities, including deleted entities."""

    ONLY_SOFT_DELETED = "ONLY_SOFT_DELETED"
    """Search only soft-deleted entities."""


def generate_filter(
    platform: Optional[str],
    platform_instance: Optional[str],
    env: Optional[str],
    container: Optional[str],
    status: Optional[RemovedStatusFilter],
    extra_filters: Optional[List[RawSearchFilterRule]],
    extra_or_filters: Optional[RawSearchFilter] = None,
) -> RawSearchFilter:
    """
    Generate a search filter based on the provided parameters.
    :param platform: The platform to filter by.
    :param platform_instance: The platform instance to filter by.
    :param env: The environment to filter by.
    :param container: The container to filter by.
    :param status: The status to filter by.
    :param extra_filters: Extra AND filters to apply.
    :param extra_or_filters: Extra OR filters to apply. These are combined with
    the AND filters using an OR at the top level.
    """
    and_filters: List[RawSearchFilterRule] = []

    # Platform filter.
    if platform:
        and_filters.append(_get_platform_filter(platform).to_raw())

    # Platform instance filter.
    if platform_instance:
        and_filters.append(
            _get_platform_instance_filter(platform, platform_instance).to_raw()
        )

    # Browse path v2 filter.
    if container:
        and_filters.append(_get_container_filter(container).to_raw())

    # Status filter.
    if status:
        status_filter = _get_status_filter(status)
        if status_filter:
            and_filters.append(status_filter.to_raw())

    # Extra filters.
    if extra_filters:
        and_filters += extra_filters

    or_filters: RawSearchFilter = [{"and": and_filters}]

    # Env filter
    if env:
        env_filters = _get_env_filters(env)
        # This matches ALL the and_filters and at least one of the envOrConditions.
        or_filters = [
            {"and": and_filter["and"] + [extraCondition]}
            for extraCondition in env_filters
            for and_filter in or_filters
        ]

    # Extra OR filters are distributed across the top level and lists.
    if extra_or_filters:
        new_or_filters: RawSearchFilter = []
        for and_filter in or_filters:
            for extra_or_filter in extra_or_filters:
                if isinstance(extra_or_filter, dict) and "and" in extra_or_filter:
                    new_or_filters.append(
                        {"and": and_filter["and"] + extra_or_filter["and"]}
                    )
                else:
                    # Hack for backwards compatibility.
                    # We have some code that erroneously passed a List[RawSearchFilterRule]
                    # instead of a List[Dict["and", List[RawSearchFilterRule]]].
                    warnings.warn(
                        "Passing a List[RawSearchFilterRule] to extra_or_filters is deprecated. "
                        "Please pass a List[Dict[str, List[RawSearchFilterRule]]] instead.",
                        SearchFilterWarning,
                        stacklevel=3,
                    )
                    new_or_filters.append(
                        {"and": and_filter["and"] + [extra_or_filter]}  # type: ignore
                    )
        or_filters = new_or_filters

    return or_filters


def _get_env_filters(env: str) -> List[RawSearchFilterRule]:
    # The env filter is a bit more tricky since it's not always stored
    # in the same place in ElasticSearch.
    return [
        # For most entity types, we look at the origin field.
        {
            "field": "origin",
            "value": env,
            "condition": "EQUAL",
        },
        # For containers, we look at the customProperties field.
        # For any containers created after https://github.com/datahub-project/datahub/pull/8027,
        # we look for the "env" property. Otherwise, we use the "instance" property.
        {
            "field": "customProperties",
            "value": f"env={env}",
        },
        {
            "field": "customProperties",
            "value": f"instance={env}",
        },
        {
            "field": "env",
            "value": env,
        },
        # Note that not all entity types have an env (e.g. dashboards / charts).
        # If the env filter is specified, these will be excluded.
    ]


def _get_status_filter(status: RemovedStatusFilter) -> Optional[SearchFilterRule]:
    if status == RemovedStatusFilter.NOT_SOFT_DELETED:
        # Subtle: in some cases (e.g. when the dataset doesn't have a status aspect), the
        # removed field is simply not present in the ElasticSearch document. Ideally this
        # would be a "removed" : "false" filter, but that doesn't work. Instead, we need to
        # use a negated filter.
        return SearchFilterRule(
            field="removed",
            values=["true"],
            condition="EQUAL",
            negated=True,
        )

    elif status == RemovedStatusFilter.ONLY_SOFT_DELETED:
        return SearchFilterRule(
            field="removed",
            values=["true"],
            condition="EQUAL",
        )

    elif status == RemovedStatusFilter.ALL:
        # We don't need to add a filter for this case.
        return None
    else:
        raise ValueError(f"Invalid status filter: {status}")


def _get_container_filter(container: str) -> SearchFilterRule:
    # Warn if container is not a fully qualified urn.
    # TODO: Change this once we have a first-class container urn type.
    if guess_entity_type(container) != "container":
        raise ValueError(f"Invalid container urn: {container}")

    return SearchFilterRule(
        field="browsePathV2",
        values=[container],
        condition="CONTAIN",
    )


def _get_platform_instance_filter(
    platform: Optional[str], platform_instance: str
) -> SearchFilterRule:
    if platform:
        # Massage the platform instance into a fully qualified urn, if necessary.
        platform_instance = make_dataplatform_instance_urn(platform, platform_instance)

    # Warn if platform_instance is not a fully qualified urn.
    # TODO: Change this once we have a first-class data platform instance urn type.
    if guess_entity_type(platform_instance) != "dataPlatformInstance":
        raise ValueError(f"Invalid data platform instance urn: {platform_instance}")

    return SearchFilterRule(
        field="platformInstance",
        condition="EQUAL",
        values=[platform_instance],
    )


def _get_platform_filter(platform: str) -> SearchFilterRule:
    return SearchFilterRule(
        field="platform.keyword",
        condition="EQUAL",
        values=[make_data_platform_urn(platform)],
    )
