import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Union

from typing_extensions import TypeAlias

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud._sdk_extras.entities.assertion import Assertion
from acryl_datahub_cloud._sdk_extras.entities.subscription import Subscription
from datahub.emitter.enum_helpers import get_enum_options
from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import SdkUsageError
from datahub.metadata.urns import AssertionUrn, CorpGroupUrn, CorpUserUrn, DatasetUrn
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

SubscriberInputType: TypeAlias = Union[CorpUserUrn, CorpGroupUrn]


ASSERTION_RELATED_ENTITY_CHANGE_TYPES = {
    models.EntityChangeTypeClass.ASSERTION_PASSED,
    models.EntityChangeTypeClass.ASSERTION_FAILED,
    models.EntityChangeTypeClass.ASSERTION_ERROR,
}

ALL_EXISTING_ENTITY_CHANGE_TYPES = {
    getattr(models.EntityChangeTypeClass, attr)
    for attr in dir(models.EntityChangeTypeClass)
    if not attr.startswith("_")
    and isinstance(getattr(models.EntityChangeTypeClass, attr), str)
}


class SubscriptionClient:
    """
    A client for managing subscriptions to entity changes in Acryl DataHub Cloud.

    Subscriptions can be created at two granularity levels for different entity change types:
    - Dataset level: Affects all assertions associated with the dataset
    - Assertion level: Affects only the specific assertion and overrides any dataset-level subscriptions

    Notes:
    - This implementation currently returns low-level Subscription entities from the entities
      submodule. In future versions, this may be replaced with a higher-level Subscription abstraction
      for improved usability.
    - The client is designed to work with both datasets and assertions, but currently only supports
      datasets. Assertions will be supported in future versions.
    - Only ENTITY_CHANGE subscription types is supported.
    - No notificationConfig is set
    - This client is experimental and under heavy development. Expect breaking changes.
    """

    def __init__(self, client: DataHubClient):
        self.client = client
        _print_experimental_warning()

    def subscribe(
        self,
        *,
        urn: Union[str, DatasetUrn, AssertionUrn],
        subscriber_urn: SubscriberInputType,
        entity_change_types: Optional[List[str]] = None,
    ) -> None:
        """
        Create a subscription to receive notifications for entity changes.

        Args:
            urn: The URN (string or URN object) of the dataset or assertion to subscribe to.
                 For datasets: subscription applies to all assertions on the dataset.
                 For assertions: subscription applies only to that specific assertion.
            subscriber_urn: The URN of the user or group that will receive notifications.
            entity_change_types: Specific change types to subscribe to. If None, defaults are:
                                - Dataset: all existing change types
                                - Assertion: assertion-related types (ASSERTION_PASSED,
                                  ASSERTION_FAILED, ASSERTION_ERROR)

        Returns:
            None

        Raises:
            SdkUsageError: If URN format is invalid, entity not found, or empty change types list.
            SdkUsageError: For assertion subscription - if non-assertion-related change types
                          are provided (only ASSERTION_PASSED, ASSERTION_FAILED, ASSERTION_ERROR allowed).
        """
        _print_experimental_warning()

        # Parse URN string if needed
        parsed_urn = self._maybe_parse_urn(urn)

        dataset_urn: DatasetUrn
        assertion_urn: Optional[AssertionUrn]
        dataset_urn, assertion_urn = (
            (parsed_urn, None)
            if isinstance(parsed_urn, DatasetUrn)
            else self._fetch_dataset_from_assertion(parsed_urn)
        )

        logger.info(
            f"Subscribing to dataset={dataset_urn} assertion={assertion_urn} for subscriber={subscriber_urn} with change types: {entity_change_types}"
        )

        # Get entity change types (use all if none provided)
        entity_change_type_strs = self._get_entity_change_types(
            assertion_scope=assertion_urn is not None,
            entity_change_types=entity_change_types,
        )

        existing_subscriptions = self.client.resolve.subscription(  # type: ignore[attr-defined]
            entity_urn=dataset_urn.urn(),
            actor_urn=subscriber_urn.urn(),
        )
        if not existing_subscriptions:
            # new subscription
            subscription = Subscription(
                info=models.SubscriptionInfoClass(
                    entityUrn=dataset_urn.urn(),
                    actorUrn=subscriber_urn.urn(),
                    actorType=CorpUserUrn.ENTITY_TYPE
                    if isinstance(subscriber_urn, CorpUserUrn)
                    else CorpGroupUrn.ENTITY_TYPE,
                    types=[
                        models.SubscriptionTypeClass.ENTITY_CHANGE,
                    ],
                    entityChangeTypes=self._merge_entity_change_types(
                        existing_change_types=None,
                        new_change_type_strs=entity_change_type_strs,
                        new_assertion_urn=assertion_urn,
                    ),
                    createdOn=self._create_audit_stamp(),
                    updatedOn=self._create_audit_stamp(),
                ),
            )
            self.client.entities.upsert(subscription)
            logger.info(f"Subscription created: {subscription.urn}")
            return
        elif len(existing_subscriptions) == 1:
            # update existing subscription
            subscription = existing_subscriptions[0]
            logger.info(
                f"Found existing subscription to be updated: {subscription.urn}"
            )
            subscription.info.entityChangeTypes = self._merge_entity_change_types(
                existing_change_types=subscription.info.entityChangeTypes,
                new_change_type_strs=entity_change_type_strs,
                new_assertion_urn=assertion_urn,
            )
            subscription.info.updatedOn = self._create_audit_stamp()
            self.client.entities.upsert(subscription)
            logger.info(f"Subscription updated: {subscription.urn}")
            return
        else:
            raise SdkUsageError(
                f"We have a mesh here - {len(existing_subscriptions)} subscriptions found for dataset={dataset_urn} assertion={assertion_urn} and subscriber={subscriber_urn}!"
            )

    def list_subscriptions(
        self,
        *,
        urn: Union[str, DatasetUrn, AssertionUrn],
        entity_change_types: Optional[List[models.EntityChangeTypeClass]] = None,
        subscriber_urn: Optional[SubscriberInputType] = None,
    ) -> List[Subscription]:
        """
        Retrieve existing subscriptions for a dataset or assertion.

        Args:
            urn: The URN of the dataset or assertion to query subscriptions for.
            entity_change_types: Optional filter to return only subscriptions for specific
                                change types. If None, returns subscriptions for all change types.
            subscriber_urn: Optional filter to return only subscriptions for a specific user
                           or group. If None, returns subscriptions for all subscribers.

        Returns:
            List[Subscription]: List of matching subscription objects.
        """
        _print_experimental_warning()
        logger.info(
            f"Listing subscriptions for {urn} with change types: {entity_change_types} and subscriber: {subscriber_urn}"
        )
        # TODO: Implement the actual logic to retrieve subscriptions.
        return [Subscription(**{})]

    def unsubscribe(
        self,
        *,
        urn: Union[str, DatasetUrn, AssertionUrn],
        subscriber_urn: Optional[SubscriberInputType] = None,
        entity_change_types: Optional[List[models.EntityChangeTypeClass]] = None,
    ) -> None:
        """
        Remove existing subscriptions for a dataset or assertion.

        Args:
            urn: The URN of the dataset or assertion to unsubscribe from.
            subscriber_urn: Optional filter to unsubscribe only the specified user or group.
                           If None, removes subscriptions for all subscribers.
            entity_change_types: Optional filter to remove only subscriptions for specific
                                change types. If None, removes subscriptions for all change types.
        """
        _print_experimental_warning()
        logger.info(
            f"Unsubscribing from {urn} for {subscriber_urn} with change types: {entity_change_types}"
        )

    def _get_entity_change_types(
        self,
        assertion_scope: bool,
        entity_change_types: Optional[List[str]] = None,
    ) -> List[str]:
        """Get entity change types with validation and defaults.

        Args:
            assertion_scope: True for assertion subscriptions, False for dataset subscriptions.
            entity_change_types: Specific change types to validate. If None, returns defaults.

        Returns:
            List of validated entity change types. Defaults are:
            - Dataset: all available change types
            - Assertion: assertion-related types (ASSERTION_PASSED, ASSERTION_FAILED, ASSERTION_ERROR)

        Raises:
            SdkUsageError: If entity_change_types is an empty list.
            SdkUsageError: If invalid change types provided or assertion scope receives
                          non-assertion change types.
        """
        if entity_change_types is not None:
            if len(entity_change_types) == 0:
                raise SdkUsageError("Entity change types cannot be an empty list.")

            all_options = get_enum_options(models.EntityChangeTypeClass)
            if any([ect not in all_options for ect in entity_change_types]):
                raise SdkUsageError(
                    f"Invalid entity change types provided: {entity_change_types}. "
                    f"Valid options are: {all_options}"
                )

            # For assertion scope, validate that only assertion-related change types are provided
            if assertion_scope:
                invalid_types = [
                    ect
                    for ect in entity_change_types
                    if ect not in ASSERTION_RELATED_ENTITY_CHANGE_TYPES
                ]
                if invalid_types:
                    raise SdkUsageError(
                        f"For assertion subscriptions, only assertion-related change types are allowed. "
                        f"Invalid types: {invalid_types}. "
                        f"Valid types: {list(ASSERTION_RELATED_ENTITY_CHANGE_TYPES)}"
                    )

            return entity_change_types

        # If no specific change types are provided, return defaults based on scope
        if assertion_scope:
            return list(ASSERTION_RELATED_ENTITY_CHANGE_TYPES)
        else:
            return list(ALL_EXISTING_ENTITY_CHANGE_TYPES)

    def _create_audit_stamp(self) -> models.AuditStampClass:
        """Create an audit stamp with current timestamp and default actor."""
        return models.AuditStampClass(
            make_ts_millis(datetime.now(tz=timezone.utc)),
            actor=DEFAULT_ACTOR_URN,  # TODO: Replace with actual actor URN from token if available
        )

    def _merge_entity_change_types(
        self,
        existing_change_types: Optional[List[models.EntityChangeDetailsClass]],
        new_change_type_strs: List[str],
        new_assertion_urn: Optional[AssertionUrn] = None,
    ) -> List[models.EntityChangeDetailsClass]:
        """Merge existing entity change types with new ones, avoiding duplicates.

        Args:
            existing_change_types: Existing entity change types from the subscription
            new_change_type_strs: New entity change type strings to add
            new_assertion_urn: Optional Assertion URN to associate with the new change types

        Returns:
            List of EntityChangeDetailsClass with merged change types
        """
        existing_change_type_str_map_filters: Dict[
            str, Optional[models.EntityChangeDetailsFilterClass]
        ] = (
            {
                # ect.entityChangeType: Union[str, EntityChangeTypeClass]; EntityChangeTypeClass is cosmetic, just a decorator
                str(ect.entityChangeType): ect.filter
                for ect in existing_change_types
            }
            if existing_change_types
            else {}
        )

        # Combine existing and new change types (avoid duplicates)
        all_change_types = set(existing_change_type_str_map_filters.keys()).union(
            set(new_change_type_strs)
        )

        return [
            models.EntityChangeDetailsClass(
                entityChangeType=ect,
                filter=self._merge_entity_change_types_filter(
                    existing_filter=existing_change_type_str_map_filters.get(ect),
                    # Apply new assertion URN to change types that are being newly added or re-specified
                    new_assertion_urn=new_assertion_urn
                    if ect in new_change_type_strs
                    else None,
                ),
            )
            for ect in all_change_types
        ]

    def _merge_entity_change_types_filter(
        self,
        existing_filter: Optional[models.EntityChangeDetailsFilterClass],
        new_assertion_urn: Optional[AssertionUrn] = None,
    ) -> Optional[models.EntityChangeDetailsFilterClass]:
        """Merge existing filter with new assertion URN if provided.

        Args:
            existing_filter: Existing filter from the subscription
            new_assertion_urn: New assertion URN to add to the filter

        Returns:
            Merged filter with new assertion URN if provided, otherwise returns existing filter.
        """
        if not existing_filter:
            # if new assertion, create a new filter with it, otherwise return None
            return (
                models.EntityChangeDetailsFilterClass(
                    includeAssertions=[new_assertion_urn.urn()]
                )
                if new_assertion_urn
                else None
            )

        if not new_assertion_urn:
            # If no new assertion URN, just return the existing filter, None or whatever it is
            return existing_filter

        assert existing_filter is not None and new_assertion_urn is not None

        if (
            existing_filter.includeAssertions is None
            or len(existing_filter.includeAssertions) == 0
        ):
            # An existing filter with empty includeAssertions is weird, but we handle it just in case
            existing_filter.includeAssertions = [new_assertion_urn.urn()]
            return existing_filter

        assert len(existing_filter.includeAssertions) > 0

        if new_assertion_urn.urn() not in existing_filter.includeAssertions:
            # Only added if not present already
            existing_filter.includeAssertions.append(new_assertion_urn.urn())

        return existing_filter

    def _maybe_parse_urn(
        self, urn: Union[str, DatasetUrn, AssertionUrn]
    ) -> Union[DatasetUrn, AssertionUrn]:
        """Parse URN string into appropriate URN object if needed.

        Args:
            urn: String URN or URN object (DatasetUrn or AssertionUrn)

        Returns:
            Parsed URN object (DatasetUrn or AssertionUrn)

        Raises:
            SdkUsageError: If the URN string format is unsupported entity type
        """
        if isinstance(urn, (DatasetUrn, AssertionUrn)):
            return urn

        # Try to determine URN type from string format
        if ":dataset:" in urn:
            return DatasetUrn.from_string(urn)
        elif ":assertion:" in urn:
            return AssertionUrn.from_string(urn)
        else:
            raise SdkUsageError(
                f"Unsupported URN type. Only dataset and assertion URNs are supported, got: {urn}"
            )

    def _fetch_dataset_from_assertion(
        self, assertion_urn: AssertionUrn
    ) -> Tuple[DatasetUrn, AssertionUrn]:
        assertion = self.client.entities.get(assertion_urn)
        if assertion is None:
            raise SdkUsageError(f"Assertion {assertion_urn} not found.")

        assert isinstance(assertion, Assertion), (
            f"Expected Assertion entity type for assertion urn={assertion_urn}"
        )
        return (assertion.dataset, assertion_urn)


def _print_experimental_warning() -> None:
    print(
        "Warning: The subscriptions client is experimental and under heavy development. Expect breaking changes."
    )
