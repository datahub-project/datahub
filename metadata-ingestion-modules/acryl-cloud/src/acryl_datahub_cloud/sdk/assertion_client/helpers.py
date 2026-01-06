"""Helper utilities for assertion clients."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import _AssertionPublic
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import _AssertionInput
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.errors import ItemNotFoundError
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn
from datahub.sdk.search_filters import FilterDsl

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

# TODO: Replace __datahub_system with the actual datahub system user https://linear.app/acryl-data/issue/OBS-1351/auditstamp-actor-hydration-pattern-for-sdk-calls
DEFAULT_CREATED_BY = CorpUserUrn.from_string("urn:li:corpuser:__datahub_system")


def _merge_field(
    input_field_value: Any,
    input_field_name: str,
    validated_assertion_input: _AssertionInput,
    validated_existing_assertion: _AssertionPublic,
    existing_entity_value: Optional[Any] = None,  # TODO: Can we do better than Any?
) -> Any:
    """Merge the input field value with any existing entity value or default value.

    The merge logic is as follows:
    - If the input is None, use the existing value
    - If the input is not None, use the input value
    - If the input is an empty list or empty string, still use the input value (falsy values can be used to unset fields)
    - If the input is a non-empty list or non-empty string, use the input value
    - If the input is None and the existing value is None, use the default value from _AssertionInput

    Args:
        input_field_value: The value of the field in the input e.g. passed to the function.
        input_field_name: The name of the field in the input.
        validated_assertion_input: The *validated* input to the function.
        validated_existing_assertion: The *validated* existing assertion from the DataHub instance.
        existing_entity_value: The value of the field in the existing entity from the DataHub instance, directly retrieved from the entity.

        Returns:
            The merged value of the field.

    """
    if input_field_value is None:  # Input value default
        if existing_entity_value is not None:  # Existing entity value set
            return existing_entity_value
        elif (
            getattr(validated_existing_assertion, input_field_name) is None
        ):  # Validated existing value not set
            return getattr(validated_assertion_input, input_field_name)
        else:  # Validated existing value set
            return getattr(validated_existing_assertion, input_field_name)
    else:  # Input value set
        return input_field_value


def _print_experimental_warning() -> None:
    print(
        "Warning: The assertions client is experimental and under heavy development. Expect breaking changes."
    )


def _validate_required_field(
    field_value: Optional[Any], field_name: str, context: str
) -> None:
    """Validate that a required field is not None and raise SDKUsageError if it is."""
    if field_value is None:
        raise SDKUsageError(f"{field_name} is required {context}")


def resolve_updated_by(
    updated_by: Optional[Union[str, CorpUserUrn]],
) -> Union[str, CorpUserUrn]:
    """Resolve the updated_by field, using DEFAULT_CREATED_BY if not provided.

    Args:
        updated_by: The updated_by value from the user input, or None.

    Returns:
        The resolved updated_by value.
    """
    if updated_by is None:
        logger.warning(
            f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
        )
        return DEFAULT_CREATED_BY
    return updated_by


def retrieve_assertion_and_monitor_by_urn(
    client: "DataHubClient",
    assertion_urn: Union[str, AssertionUrn],
    dataset_urn: Union[str, DatasetUrn],
) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
    """Retrieve the assertion and monitor entities from the DataHub instance by URN.

    Args:
        client: The DataHub client.
        assertion_urn: The assertion URN.
        dataset_urn: The dataset URN.

    Returns:
        A tuple of (assertion entity or None, monitor URN, monitor entity or None).
    """
    urn: AssertionUrn = (
        assertion_urn
        if isinstance(assertion_urn, AssertionUrn)
        else AssertionUrn.from_string(assertion_urn)
    )
    _dataset_urn: DatasetUrn = (
        dataset_urn
        if isinstance(dataset_urn, DatasetUrn)
        else DatasetUrn.from_string(dataset_urn)
    )

    # Get assertion entity
    maybe_assertion_entity: Optional[Assertion] = None
    try:
        entity = client.entities.get(urn)
        if entity is not None:
            assert isinstance(entity, Assertion)
            maybe_assertion_entity = entity
    except ItemNotFoundError:
        pass

    # Get monitor entity by searching for monitors where assertionUrn equals the assertion urn
    monitor_urn: Optional[MonitorUrn] = None
    maybe_monitor_entity: Optional[Monitor] = None
    try:
        # Search for monitor entities with assertionUrn matching the assertion urn
        monitor_filter = FilterDsl.and_(
            FilterDsl.entity_type("monitor"),
            FilterDsl.custom_filter("assertionUrn", "EQUAL", [str(urn)]),
        )
        monitor_urns = list(client.search.get_urns(filter=monitor_filter))

        if monitor_urns:
            # Log if there are multiple monitors found, because this is unexpected
            if len(monitor_urns) > 1:
                logger.warning(
                    f"Multiple monitors found for assertion {urn}, which should never happen: {monitor_urns}"
                )
            # Use the first matching monitor
            monitor_urn = MonitorUrn.from_string(str(monitor_urns[0]))
            entity = client.entities.get(monitor_urn)
            if entity is not None:
                assert isinstance(entity, Monitor)
                maybe_monitor_entity = entity
    except ItemNotFoundError as e:
        logger.debug(f"Could not find monitor for assertion {urn}: {e}")
        pass

    # If no monitor found via search, fall back to creating a new monitor with the dataset urn and assertion urn
    if monitor_urn is None:
        monitor_urn = Monitor._ensure_id(id=(_dataset_urn, urn))

    return maybe_assertion_entity, monitor_urn, maybe_monitor_entity
