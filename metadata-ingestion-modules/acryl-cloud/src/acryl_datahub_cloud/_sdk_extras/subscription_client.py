import logging
from typing import List, Optional, Union

from typing_extensions import TypeAlias

from acryl_datahub_cloud._sdk_extras.entities.subscription import Subscription
from datahub.metadata.schema_classes import EntityChangeTypeClass
from datahub.metadata.urns import AssertionUrn, CorpGroupUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

SubscriberInputType: TypeAlias = Union[CorpUserUrn, CorpGroupUrn]


class SubscriptionClient:
    """
    A client for managing subscriptions to entity changes in Acryl DataHub Cloud.

    Subscriptions can be created at two granularity levels for different entity change types:
    - Dataset level: Affects all assertions associated with the dataset
    - Assertion level: Affects only the specific assertion and overrides any dataset-level subscriptions

    Note: This implementation currently returns low-level Subscription entities from the entities
    submodule. In future versions, this may be replaced with a higher-level Subscription abstraction
    for improved usability.
    """

    def __init__(self, client: DataHubClient):
        self.client = client
        _print_experimental_warning()

    def subscribe(
        self,
        *,
        urn: Union[DatasetUrn, AssertionUrn],
        subscriber_urn: SubscriberInputType,
        entity_change_types: Optional[List[EntityChangeTypeClass]] = None,
    ) -> Subscription:
        """
        Create a subscription to receive notifications for entity changes.

        Args:
            urn: The URN of the dataset or assertion to subscribe to.
                 For datasets: subscription applies to all assertions on the dataset.
                 For assertions: subscription applies only to that specific assertion.
            subscriber_urn: The URN of the user or group that will receive notifications.
            entity_change_types: Specific change types to subscribe to (e.g., UPSERT, DELETE).
                                If None, subscribes to all available change types.

        Returns:
            Subscription: The created subscription object.
        """
        _print_experimental_warning()
        logger.info(
            f"Subscribing to {urn} for {subscriber_urn} with change types: {entity_change_types}"
        )
        # TODO: Implement the actual subscription logic here.
        return Subscription(**{})

    def list_subscriptions(
        self,
        *,
        urn: Union[DatasetUrn, AssertionUrn],
        entity_change_types: Optional[List[EntityChangeTypeClass]] = None,
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
        entity_change_types: Optional[List[EntityChangeTypeClass]] = None,
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


def _print_experimental_warning() -> None:
    print(
        "Warning: The subscriptions client is experimental and under heavy development. Expect breaking changes."
    )
