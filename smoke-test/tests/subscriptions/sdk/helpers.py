"""Helper functions and constants for SDK subscription tests."""

import logging
import uuid
from typing import Any, Dict, List, Optional, Set

from datahub.ingestion.graph.client import DataHubGraph
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import get_nested_value

logger = logging.getLogger(__name__)


GRAPHQL_GET_SUBSCRIPTION = """
    query getSubscription($input: GetSubscriptionInput!) {
        getSubscription(input: $input) {
            subscription {
                actorUrn
                subscriptionUrn
                subscriptionTypes
                entityChangeTypes {
                    entityChangeType
                    filter {
                        includeAssertions
                    }
                }
            }
        }
    }
"""


def get_subscription_for_entity(
    graph_client: DataHubGraph, entity_urn: str
) -> Optional[Dict[str, Any]]:
    """Fetch subscription for an entity via GraphQL.

    Args:
        graph_client: The DataHub graph client
        entity_urn: The URN of the entity to get subscription for

    Returns:
        The subscription data if found, None otherwise
    """
    result = graph_client.execute_graphql(
        query=GRAPHQL_GET_SUBSCRIPTION,
        variables={"input": {"entityUrn": entity_urn}},
    )
    return get_nested_value(result, "getSubscription.subscription")


def verify_subscription_exists(
    graph_client: DataHubGraph,
    entity_urn: str,
    expected_actor_urn: Optional[str] = None,
) -> Dict[str, Any]:
    """Verify a subscription exists for an entity.

    Call wait_for_subscription_sync() before this function to handle
    eventual consistency.

    Args:
        graph_client: The DataHub graph client
        entity_urn: The URN of the entity to check
        expected_actor_urn: Optional expected actor URN to verify

    Returns:
        The subscription data

    Raises:
        AssertionError: If subscription is not found or actor URN doesn't match
    """
    subscription = get_subscription_for_entity(graph_client, entity_urn)
    if subscription is None:
        raise AssertionError(f"Subscription not found for entity {entity_urn}")

    if expected_actor_urn and subscription.get("actorUrn") != expected_actor_urn:
        raise AssertionError(
            f"Actor URN mismatch: expected {expected_actor_urn}, "
            f"got {subscription.get('actorUrn')}"
        )
    return subscription


def verify_subscription_removed(graph_client: DataHubGraph, entity_urn: str) -> None:
    """Verify a subscription has been removed.

    Call wait_for_subscription_sync() before this function to handle
    eventual consistency.

    Args:
        graph_client: The DataHub graph client
        entity_urn: The URN of the entity to check

    Raises:
        AssertionError: If subscription still exists
    """
    subscription = get_subscription_for_entity(graph_client, entity_urn)
    if subscription is not None:
        raise AssertionError(f"Subscription still exists for entity {entity_urn}")


def extract_entity_change_types(subscription: Dict[str, Any]) -> Set[str]:
    """Extract the set of entity change type strings from a subscription.

    Args:
        subscription: The subscription data from GraphQL

    Returns:
        Set of entity change type strings (e.g., {"DEPRECATED", "OWNER_ADDED"})
    """
    entity_change_types = subscription.get("entityChangeTypes", [])
    return {ect["entityChangeType"] for ect in entity_change_types}


def extract_assertion_filters(
    subscription: Dict[str, Any], entity_change_type: str
) -> Optional[List[str]]:
    """Extract assertion URNs from filter for a specific change type.

    Args:
        subscription: The subscription data from GraphQL
        entity_change_type: The change type to look for (e.g., "ASSERTION_FAILED")

    Returns:
        List of assertion URNs if filter exists, None otherwise
    """
    entity_change_types = subscription.get("entityChangeTypes", [])
    for ect in entity_change_types:
        if ect["entityChangeType"] == entity_change_type:
            filter_data = ect.get("filter")
            if filter_data:
                return filter_data.get("includeAssertions")
    return None


def wait_for_subscription_sync() -> None:
    """Wait for subscription operations to sync.

    Uses the default 120-second timeout since subscriptions rely on
    search indexing which may take longer to become consistent.
    """
    wait_for_writes_to_sync()


def generate_unique_test_id() -> str:
    """Generate a unique identifier for test isolation.

    Returns a short UUID string suitable for use in display names and URNs.
    """
    return str(uuid.uuid4())[:8]
