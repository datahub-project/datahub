import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple, Union

from typing_extensions import TypeAlias

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.subscription import Subscription
from datahub.emitter.enum_helpers import get_enum_options
from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import SdkUsageError
from datahub.metadata.urns import AssertionUrn, CorpGroupUrn, CorpUserUrn, DatasetUrn
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

SubscriberInputType: TypeAlias = Union[CorpUserUrn, CorpGroupUrn, str]


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
        entity_change_types: Optional[
            Sequence[Union[str, models.EntityChangeTypeClass]]
        ] = None,
    ) -> None:
        """
        Create a subscription to receive notifications for entity changes.

        Args:
            urn: The URN (string or URN object) of the dataset or assertion to subscribe to.
                 For datasets: subscription applies to all assertions on the dataset.
                 For assertions: subscription applies only to that specific assertion.
            subscriber_urn: The URN of the user or group that will receive notifications.
                           Can be a string (valid corpuser or corpGroup URN) or URN object.
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

        # Parse subscriber URN string if needed
        parsed_subscriber_urn = self._maybe_parse_subscriber_urn(subscriber_urn)

        dataset_urn: DatasetUrn
        assertion_urn: Optional[AssertionUrn]
        dataset_urn, assertion_urn = (
            (parsed_urn, None)
            if isinstance(parsed_urn, DatasetUrn)
            else self._fetch_dataset_from_assertion(parsed_urn)
        )

        logger.info(
            f"Subscribing to dataset={dataset_urn} assertion={assertion_urn} for subscriber={parsed_subscriber_urn} with change types: {entity_change_types}"
        )

        # Get entity change types (use all if none provided)
        entity_change_type_strs = self._get_entity_change_types(
            assertion_scope=assertion_urn is not None,
            entity_change_types=entity_change_types,
        )

        existing_subscriptions = self.client.resolve.subscription(  # type: ignore[attr-defined]
            entity_urn=dataset_urn.urn(),
            actor_urn=parsed_subscriber_urn.urn(),
        )
        if not existing_subscriptions:
            # new subscription
            subscription = Subscription(
                info=models.SubscriptionInfoClass(
                    entityUrn=dataset_urn.urn(),
                    actorUrn=parsed_subscriber_urn.urn(),
                    actorType=CorpUserUrn.ENTITY_TYPE
                    if isinstance(parsed_subscriber_urn, CorpUserUrn)
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
            subscription_urn = existing_subscriptions[0]
            existing_subscription_entity = self.client.entities.get(subscription_urn)
            assert isinstance(existing_subscription_entity, Subscription), (
                f"Expected Subscription entity type for subscription urn={subscription_urn}"
            )
            logger.info(
                f"Found existing subscription to be updated: {existing_subscription_entity.urn}"
            )
            existing_subscription_entity.info.entityChangeTypes = self._merge_entity_change_types(
                existing_change_types=existing_subscription_entity.info.entityChangeTypes,
                new_change_type_strs=entity_change_type_strs,
                new_assertion_urn=assertion_urn,
            )
            existing_subscription_entity.info.updatedOn = self._create_audit_stamp()
            self.client.entities.upsert(existing_subscription_entity)
            logger.info(f"Subscription updated: {existing_subscription_entity.urn}")
            return
        else:
            raise SdkUsageError(
                f"We have a mesh here - {len(existing_subscriptions)} subscriptions found for dataset={dataset_urn} assertion={assertion_urn} and subscriber={parsed_subscriber_urn}!"
            )

    def list_subscriptions(
        self,
        *,
        urn: Union[str, DatasetUrn, AssertionUrn],
        entity_change_types: Optional[
            Sequence[Union[str, models.EntityChangeTypeClass]]
        ] = None,
        subscriber_urn: Optional[SubscriberInputType] = None,
    ) -> List[Subscription]:
        """
        Retrieve existing subscriptions for a dataset or assertion.

        Args:
            urn: The URN of the dataset or assertion to query subscriptions for.
            entity_change_types: Optional filter to return only subscriptions for specific
                                change types. If None, returns subscriptions for all change types.
            subscriber_urn: Optional filter to return only subscriptions for a specific user
                           or group. Can be a string (valid corpuser or corpGroup URN) or URN object.
                           If None, returns subscriptions for all subscribers.

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
        subscriber_urn: SubscriberInputType,
        entity_change_types: Optional[
            List[Union[str, models.EntityChangeTypeClass]]
        ] = None,
    ) -> None:
        """
        Remove subscriptions for entity change notifications.

        This method supports selective unsubscription based on subscriber and change types.
        The behavior varies depending on whether the target is a dataset or assertion:

        **Dataset unsubscription:**
        - Removes specified change types from the subscription
        - If no change types specified, removes all existing change types
        - Deletes entire subscription if no change types remain

        **Assertion unsubscription:**
        - Removes assertion from specified change type filters
        - If no change types specified, removes assertion from all assertion-related change types
          (ASSERTION_PASSED, ASSERTION_FAILED, ASSERTION_ERROR)
        - Deletes change type if no assertions remain in filter
          (prevents assertion-level subscription from silently upgrading to dataset-level)
        - Deletes entire subscription if no change types remain

        **Warning behavior:**
        - If entity_change_types is explicitly provided:
          * Warns about change types that don't exist in the subscription
          * For assertions: warns about change types that don't include the assertion in their filter
        - If entity_change_types is None (using defaults), no warnings are logged

        Args:
            urn: URN (string or URN object) of the dataset or assertion to unsubscribe from.
            subscriber_urn: User or group URN to unsubscribe.
                           Can be a string (valid corpuser or corpGroup URN) or URN object.
            entity_change_types: Specific change types to remove. If None, defaults are:
                                - Dataset: all existing change types in the subscription
                                - Assertion: assertion-related types (ASSERTION_PASSED,
                                  ASSERTION_FAILED, ASSERTION_ERROR)

        Returns:
            None

        Raises:
            SdkUsageError: If URN format is invalid, entity not found, or empty change types list.
            SdkUsageError: For assertion unsubscription - if not assertion-related change types
                          (ASSERTION_PASSED, ASSERTION_FAILED, ASSERTION_ERROR) are provided.

        Note:
            This method is experimental and may change in future versions.
        """
        _print_experimental_warning()

        # Parse URN string if needed
        parsed_urn = self._maybe_parse_urn(urn)

        # Parse subscriber URN string if needed
        parsed_subscriber_urn = self._maybe_parse_subscriber_urn(subscriber_urn)

        dataset_urn: DatasetUrn
        assertion_urn: Optional[AssertionUrn]
        dataset_urn, assertion_urn = (
            (parsed_urn, None)
            if isinstance(parsed_urn, DatasetUrn)
            else self._fetch_dataset_from_assertion(parsed_urn)
        )

        logger.info(
            f"Unsubscribing from dataset={dataset_urn}{f' assertion={assertion_urn}' if assertion_urn else ''} for subscriber={parsed_subscriber_urn} with change types: {entity_change_types}"
        )

        # Find existing subscription
        existing_subscription_urns = self.client.resolve.subscription(  # type: ignore[attr-defined]
            entity_urn=dataset_urn.urn(),
            actor_urn=parsed_subscriber_urn.urn(),
        )

        if not existing_subscription_urns:
            logger.info(
                f"No subscription found for dataset={dataset_urn} and subscriber={parsed_subscriber_urn}"
            )
            return
        elif len(existing_subscription_urns) > 1:
            raise SdkUsageError(
                f"Multiple subscriptions found for dataset={dataset_urn} and subscriber={parsed_subscriber_urn}. "
                f"Expected at most 1, got {len(existing_subscription_urns)}"
            )

        subscription_urn = existing_subscription_urns[0]
        subscription_entity = self.client.entities.get(subscription_urn)
        assert isinstance(subscription_entity, Subscription), (
            f"Expected Subscription entity type for subscription urn={subscription_urn}"
        )
        logger.info(
            f"Found existing subscription to be updated: {subscription_entity.urn}"
        )

        # Get the change types to remove (validated input or defaults)
        change_types_to_remove = self._get_entity_change_types(
            assertion_scope=assertion_urn is not None,
            entity_change_types=entity_change_types,
        )

        # Remove the specified change types
        if subscription_entity.info.entityChangeTypes is None:
            raise SdkUsageError(
                f"Subscription {subscription_entity.urn} has no change types to remove"
            )
        # Determine if we should warn about missing items (only when user explicitly provided change types)
        warn_if_missing = entity_change_types is not None
        updated_change_types = self._remove_change_types(
            subscription_entity.info.entityChangeTypes,
            change_types_to_remove,
            assertion_urn_to_remove=assertion_urn,
            warn_if_missing=warn_if_missing,
        )

        # If no change types remain, delete the subscription
        if not updated_change_types:
            logger.info(
                f"No change types remain, deleting subscription: {subscription_entity.urn}"
            )
            self.client.entities.delete(subscription_entity.urn)
            return

        # Update the subscription with remaining change types
        subscription_entity.info.entityChangeTypes = updated_change_types
        subscription_entity.info.updatedOn = self._create_audit_stamp()
        self.client.entities.upsert(subscription_entity)
        logger.info(f"Subscription updated: {subscription_entity.urn}")

    def _get_entity_change_types(
        self,
        assertion_scope: bool,
        entity_change_types: Optional[
            Sequence[Union[str, models.EntityChangeTypeClass]]
        ] = None,
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

            # Convert all entity change types to strings
            entity_change_type_strs = [str(ect) for ect in entity_change_types]

            all_options = get_enum_options(models.EntityChangeTypeClass)
            if any([ect not in all_options for ect in entity_change_type_strs]):
                raise SdkUsageError(
                    f"Invalid entity change types provided: {entity_change_type_strs}. "
                    f"Valid options are: {all_options}"
                )

            # For assertion scope, validate that only assertion-related change types are provided
            if assertion_scope:
                invalid_types = [
                    ect
                    for ect in entity_change_type_strs
                    if ect not in ASSERTION_RELATED_ENTITY_CHANGE_TYPES
                ]
                if invalid_types:
                    raise SdkUsageError(
                        f"For assertion subscriptions, only assertion-related change types are allowed. "
                        f"Invalid types: {invalid_types}. "
                        f"Valid types: {list(ASSERTION_RELATED_ENTITY_CHANGE_TYPES)}"
                    )

            return entity_change_type_strs

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
            existing_change_types: Existing entity change types from the subscription.
                                   Can be None when creating a new subscription.
            new_change_type_strs: New entity change type strings to add
            new_assertion_urn: Optional Assertion URN to associate with the new change types

        Returns:
            List of EntityChangeDetailsClass with merged change types

        Note:
            This method does not modify existing_change_types in-place; it returns a new list.
        """
        assert len(new_change_type_strs) > 0, (
            "new_change_type_strs cannot be empty, worse case we have the default values"
        )

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

    def _remove_change_types_filter(
        self,
        entity_change_type: str,
        existing_filter: Optional[models.EntityChangeDetailsFilterClass],
        assertion_urn_to_remove: Optional[AssertionUrn] = None,
        warn_if_missing: bool = True,
    ) -> Optional[models.EntityChangeDetailsFilterClass]:
        """Remove assertion URN from existing filter.

        Args:
            entity_change_type: The entity change type, used only for better logging messages
            existing_filter: Existing filter from the subscription
            assertion_urn_to_remove: Assertion URN to remove from the filter
            warn_if_missing: Whether to log warning if assertion is not found in filter

        Returns:
            Updated filter with assertion URN removed, or None if no assertions remain
            (indicating the entire change type should be removed).
        """
        if not existing_filter:
            # No filter means dataset-level subscription, assertion is not included
            if assertion_urn_to_remove and warn_if_missing:
                logger.warning(
                    f"Assertion {assertion_urn_to_remove.urn()} is not included in entity change type '{entity_change_type}' and will be ignored"
                )
            return existing_filter

        if not assertion_urn_to_remove:
            # If no assertion URN to remove, just return the existing filter
            return existing_filter

        if (
            existing_filter.includeAssertions is None
            or len(existing_filter.includeAssertions) == 0
        ):
            # Empty includeAssertions means dataset-level subscription, assertion is not included
            if warn_if_missing:
                logger.warning(
                    f"Assertion {assertion_urn_to_remove.urn()} is not included in entity change type '{entity_change_type}' and will be ignored"
                )
            return existing_filter

        assertion_urn_str = assertion_urn_to_remove.urn()
        if assertion_urn_str not in existing_filter.includeAssertions:
            # Assertion not found in filter
            if warn_if_missing:
                logger.warning(
                    f"Assertion {assertion_urn_str} is not included in entity change type '{entity_change_type}' and will be ignored"
                )
            return existing_filter

        # Remove the assertion from the list
        updated_assertions = [
            urn for urn in existing_filter.includeAssertions if urn != assertion_urn_str
        ]

        if not updated_assertions:
            # No assertions remain, return None to indicate change type should be removed
            return None

        # Return updated filter with remaining assertions
        existing_filter.includeAssertions = updated_assertions
        return existing_filter

    def _remove_change_types(
        self,
        existing_change_types: List[models.EntityChangeDetailsClass],
        change_types_to_remove: List[str],
        assertion_urn_to_remove: Optional[AssertionUrn] = None,
        warn_if_missing: bool = True,
    ) -> List[models.EntityChangeDetailsClass]:
        """Remove specified change types from subscription, returning a new list.

        For dataset unsubscription, removes entire change types.
        For assertion unsubscription, removes assertion from change type filters,
        and removes entire change type if no assertions remain.

        Args:
            existing_change_types: Current entity change types from the subscription.
                                  Never None since this method is only called for existing subscriptions.
            change_types_to_remove: List of change type strings to remove (must not be empty)
            assertion_urn_to_remove: Optional assertion URN to remove from filters.
                                   If None, performs dataset-level removal (removes entire change types).
            warn_if_missing: Whether to log warnings about missing change types or assertions.

        Returns:
            New list of EntityChangeDetailsClass with specified change types or assertions removed

        Note:
            This method does not modify existing_change_types in-place; it returns a new list.
        """
        assert len(change_types_to_remove) > 0, (
            "change_types_to_remove cannot be empty, worse case we have the default values"
        )
        assert len(existing_change_types) > 0, (
            "Subscription must have at least one change type (no model restriction but business rule)"
        )

        change_types_to_remove_set = set(change_types_to_remove)
        existing_change_types_set = {
            str(ect.entityChangeType) for ect in existing_change_types
        }

        # Warn about change types that don't exist in the subscription
        nonexistent_change_types = (
            change_types_to_remove_set - existing_change_types_set
        )
        if nonexistent_change_types and warn_if_missing:
            logger.warning(
                f"The following change types do not exist in the subscription and will be ignored: {sorted(nonexistent_change_types)}"
            )

        if assertion_urn_to_remove is None:
            # Dataset-level removal: remove entire change types
            return [
                ect
                for ect in existing_change_types
                if str(ect.entityChangeType) not in change_types_to_remove_set
            ]
        else:
            # Assertion-level removal: remove assertion from filters
            result = []
            for ect in existing_change_types:
                if str(ect.entityChangeType) not in change_types_to_remove_set:
                    # Keep change types not being removed
                    result.append(ect)
                else:
                    # Remove assertion from this change type's filter
                    updated_filter = self._remove_change_types_filter(
                        entity_change_type=str(ect.entityChangeType),
                        existing_filter=ect.filter,
                        assertion_urn_to_remove=assertion_urn_to_remove,
                        warn_if_missing=warn_if_missing,
                    )
                    if updated_filter is not None:
                        # Keep change type with updated filter
                        result.append(
                            models.EntityChangeDetailsClass(
                                entityChangeType=ect.entityChangeType,
                                filter=updated_filter,
                            )
                        )
                    # If updated_filter is None, the change type is removed entirely
            return result

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

    def _maybe_parse_subscriber_urn(
        self, subscriber_urn: SubscriberInputType
    ) -> Union[CorpUserUrn, CorpGroupUrn]:
        """Parse subscriber URN string into appropriate URN object if needed.

        Args:
            subscriber_urn: String URN or URN object (CorpUserUrn or CorpGroupUrn)

        Returns:
            Parsed URN object (CorpUserUrn or CorpGroupUrn)

        Raises:
            SdkUsageError: If the URN string format is invalid or unsupported
        """
        if isinstance(subscriber_urn, (CorpUserUrn, CorpGroupUrn)):
            return subscriber_urn

        # Try to determine URN type from string format
        if ":corpuser:" in subscriber_urn:
            return CorpUserUrn.from_string(subscriber_urn)
        elif ":corpGroup:" in subscriber_urn:
            return CorpGroupUrn.from_string(subscriber_urn)
        else:
            raise SdkUsageError(
                f"Unsupported subscriber URN type. Only corpuser and corpGroup URNs are supported, got: {subscriber_urn}"
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
