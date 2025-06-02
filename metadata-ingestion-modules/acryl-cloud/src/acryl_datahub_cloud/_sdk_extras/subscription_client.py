import logging
from datetime import datetime, timezone
from typing import List, Optional, Union

from typing_extensions import TypeAlias

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud._sdk_extras.entities.subscription import Subscription
from datahub.emitter.enum_helpers import get_enum_options
from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import SdkUsageError
from datahub.metadata.urns import AssertionUrn, CorpGroupUrn, CorpUserUrn, DatasetUrn
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

SubscriberInputType: TypeAlias = Union[CorpUserUrn, CorpGroupUrn]


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
        urn: Union[DatasetUrn, AssertionUrn],
        subscriber_urn: SubscriberInputType,
        entity_change_types: Optional[List[str]] = None,
    ) -> None:
        """
        Create a subscription to receive notifications for entity changes.

        Args:
            urn: The URN of the dataset or assertion to subscribe to.
                 For datasets: subscription applies to all assertions on the dataset.
                 For assertions: subscription applies only to that specific assertion.
            subscriber_urn: The URN of the user or group that will receive notifications.
            entity_change_types: Specific change types to subscribe to (e.g., UPSERT, DELETE).
                                If None, subscribes to all available change types.
                                We validate that provided values match values in models.EntityChangeTypeClass.

        Returns:
            Subscription: The created subscription object.
        """
        _print_experimental_warning()
        logger.info(
            f"Subscribing to {urn} for {subscriber_urn} with change types: {entity_change_types}"
        )

        assert isinstance(urn, DatasetUrn), "AssertionUrn is not supported yet."

        # Get entity change types (use all if none provided)
        entity_change_type_strs = self._get_entity_change_types(entity_change_types)

        existing_subscriptions = self.client.resolve.subscription(  # type: ignore[attr-defined]
            entity_urn=urn.urn(),
            actor_urn=subscriber_urn.urn(),
        )
        if not existing_subscriptions:
            # new subscription
            subscription = Subscription(
                info=models.SubscriptionInfoClass(
                    entityUrn=urn.urn(),
                    actorUrn=subscriber_urn.urn(),
                    actorType=CorpUserUrn.ENTITY_TYPE
                    if isinstance(subscriber_urn, CorpUserUrn)
                    else CorpGroupUrn.ENTITY_TYPE,
                    types=[
                        models.SubscriptionTypeClass.ENTITY_CHANGE,
                    ],
                    entityChangeTypes=[
                        models.EntityChangeDetailsClass(
                            entityChangeType=ect,
                            filter=None,
                        )
                        for ect in entity_change_type_strs
                    ],
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
                subscription.info.entityChangeTypes, entity_change_type_strs
            )
            subscription.info.updatedOn = self._create_audit_stamp()
            self.client.entities.upsert(subscription)
            logger.info(f"Subscription updated: {subscription.urn}")
            return
        else:
            raise SdkUsageError(
                f"We have a mesh here - {len(existing_subscriptions)} subscriptions found for dataset={urn} and subscriber={subscriber_urn}!"
            )

    def list_subscriptions(
        self,
        *,
        urn: Union[DatasetUrn, AssertionUrn],
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
        urn: Union[DatasetUrn, AssertionUrn],
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
        entity_change_types: Optional[List[str]] = None,
    ) -> List[str]:
        """Get entity change types. If provided, returns as-is. If None, returns all possible values.

        Args:
            entity_change_types: Optional list of entity change types. If None,
                                 all possible values will be returned. Raises SdkUsageError if empty list `[]`.
                                 We also validate that provided values match values in models.EntityChangeTypeClass
                                 and if not, SdkUsageError is raised.

        Returns:
            List of entity change types.
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
            return entity_change_types

        # If no specific change types are provided, return all available change types
        return [
            getattr(models.EntityChangeTypeClass, attr)
            for attr in dir(models.EntityChangeTypeClass)
            if not attr.startswith("_")
            and isinstance(getattr(models.EntityChangeTypeClass, attr), str)
        ]

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
    ) -> List[models.EntityChangeDetailsClass]:
        """Merge existing entity change types with new ones, avoiding duplicates.

        Args:
            existing_change_types: Existing entity change types from the subscription
            new_change_type_strs: New entity change type strings to add

        Returns:
            List of EntityChangeDetailsClass with merged change types
        """
        existing_change_type_strs = set()
        if existing_change_types:
            existing_change_type_strs = {
                ect.entityChangeType for ect in existing_change_types
            }

        # Combine existing and new change types (avoid duplicates)
        all_change_types = existing_change_type_strs.union(set(new_change_type_strs))

        return [
            models.EntityChangeDetailsClass(
                entityChangeType=ect,
                filter=None,
            )
            for ect in all_change_types
        ]


def _print_experimental_warning() -> None:
    print(
        "Warning: The subscriptions client is experimental and under heavy development. Expect breaking changes."
    )
