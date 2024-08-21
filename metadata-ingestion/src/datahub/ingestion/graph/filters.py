import enum
from typing import Any, Dict, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.utilities.urns.urn import guess_entity_type

SearchFilterRule = Dict[str, Any]


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
    status: RemovedStatusFilter,
    extra_filters: Optional[List[SearchFilterRule]],
    extra_or_filters: Optional[List[SearchFilterRule]] = None,
) -> List[Dict[str, List[SearchFilterRule]]]:
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
    and_filters: List[SearchFilterRule] = []

    # Platform filter.
    if platform:
        and_filters.append(_get_platform_filter(platform))

    # Platform instance filter.
    if platform_instance:
        and_filters.append(_get_platform_instance_filter(platform, platform_instance))

    # Browse path v2 filter.
    if container:
        and_filters.append(_get_container_filter(container))

    # Status filter.
    status_filter = _get_status_filter(status)
    if status_filter:
        and_filters.append(status_filter)

    # Extra filters.
    if extra_filters:
        and_filters += extra_filters

    or_filters: List[Dict[str, List[SearchFilterRule]]] = [{"and": and_filters}]

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
        or_filters = [
            {"and": and_filter["and"] + [extra_or_filter]}
            for extra_or_filter in extra_or_filters
            for and_filter in or_filters
        ]

    return or_filters


def _get_env_filters(env: str) -> List[SearchFilterRule]:
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
        # Note that not all entity types have an env (e.g. dashboards / charts).
        # If the env filter is specified, these will be excluded.
    ]


def _get_status_filter(status: RemovedStatusFilter) -> Optional[SearchFilterRule]:
    if status == RemovedStatusFilter.NOT_SOFT_DELETED:
        # Subtle: in some cases (e.g. when the dataset doesn't have a status aspect), the
        # removed field is simply not present in the ElasticSearch document. Ideally this
        # would be a "removed" : "false" filter, but that doesn't work. Instead, we need to
        # use a negated filter.
        return {
            "field": "removed",
            "values": ["true"],
            "condition": "EQUAL",
            "negated": True,
        }

    elif status == RemovedStatusFilter.ONLY_SOFT_DELETED:
        return {
            "field": "removed",
            "values": ["true"],
            "condition": "EQUAL",
        }

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

    return {
        "field": "browsePathV2",
        "values": [container],
        "condition": "CONTAIN",
    }


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

    return {
        "field": "platformInstance",
        "values": [platform_instance],
        "condition": "EQUAL",
    }


def _get_platform_filter(platform: str) -> SearchFilterRule:
    return {
        "field": "platform.keyword",
        "values": [make_data_platform_urn(platform)],
        "condition": "EQUAL",
    }
