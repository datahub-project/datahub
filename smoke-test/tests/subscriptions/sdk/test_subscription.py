"""
Integration tests for subscription SDK methods.

Tests the following methods from SubscriptionClient:
- subscribe (dataset-level and assertion-level)
- unsubscribe (dataset-level and assertion-level)

These tests verify both that the API calls succeed AND that data is actually
persisted to the server by querying via GraphQL after each operation.
"""

import logging

import pytest

import datahub.metadata.schema_classes as models
from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.subscriptions.sdk.helpers import (
    extract_assertion_filters,
    extract_entity_change_types,
    generate_unique_test_id,
    verify_subscription_exists,
    verify_subscription_removed,
    wait_for_subscription_sync,
)
from tests.utils import assert_error_contains_keywords

logger = logging.getLogger(__name__)


# =============================================================================
# Dataset Subscription Tests
# =============================================================================


def test_subscribe_dataset_basic(
    test_data: dict,
    datahub_client: DataHubClient,
    admin_user_urn: str,
    graph_client: DataHubGraph,
) -> None:
    """Test basic dataset subscription with default change types.

    Verifies subscription is persisted via GraphQL.
    """
    dataset_urn = test_data["dataset_urn"]

    try:
        datahub_client.subscriptions.subscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
            skip_actor_exists_check=True,
        )
        wait_for_subscription_sync()
        logger.info(f"Successfully subscribed to dataset: {dataset_urn}")

        # Verify subscription via GraphQL (with retry for eventual consistency)
        subscription = verify_subscription_exists(
            graph_client, dataset_urn, expected_actor_urn=admin_user_urn
        )
        change_types = extract_entity_change_types(subscription)
        assert len(change_types) > 0, "Should have at least one change type"
        logger.info(f"Verified subscription via GraphQL: {change_types}")

    finally:
        datahub_client.subscriptions.unsubscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
        )
        wait_for_subscription_sync()
        logger.info(f"Successfully unsubscribed from dataset: {dataset_urn}")


def test_subscribe_dataset_specific_change_types(
    isolated_dataset: str,
    datahub_client: DataHubClient,
    admin_user_urn: str,
    graph_client: DataHubGraph,
) -> None:
    """Test dataset subscription with specific change types.

    Creates a subscription with only specific change types and verifies
    the subscription is persisted correctly via GraphQL.

    Uses an isolated dataset to ensure no state leakage from other tests.
    """
    dataset_urn = isolated_dataset
    specific_types = [
        models.EntityChangeTypeClass.DEPRECATED,
        models.EntityChangeTypeClass.OWNER_ADDED,
    ]

    try:
        datahub_client.subscriptions.subscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
            entity_change_types=specific_types,
            skip_actor_exists_check=True,
        )
        wait_for_subscription_sync()
        logger.info(
            f"Successfully subscribed to dataset with specific types: {dataset_urn}"
        )

        # Verify subscription via GraphQL matches the requested change types
        subscription = verify_subscription_exists(
            graph_client, dataset_urn, expected_actor_urn=admin_user_urn
        )
        change_types = extract_entity_change_types(subscription)

        # Verify the exact change types we requested are present
        expected_types = {"DEPRECATED", "OWNER_ADDED"}
        assert change_types == expected_types, (
            f"Expected change types {expected_types}, got {change_types}"
        )
        logger.info(f"Verified subscription change types via GraphQL: {change_types}")

    finally:
        datahub_client.subscriptions.unsubscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
        )
        wait_for_subscription_sync()
        logger.info(f"Successfully unsubscribed from dataset: {dataset_urn}")


# =============================================================================
# Assertion Subscription Tests
# =============================================================================


def test_subscribe_assertion_basic(
    isolated_assertion: tuple[str, str],
    datahub_client: DataHubClient,
    admin_user_urn: str,
    graph_client: DataHubGraph,
) -> None:
    """Test basic assertion-level subscription with default change types.

    Creates a subscription to a specific assertion and verifies the subscription
    is persisted correctly via GraphQL. Assertion subscriptions are stored at the
    dataset level with assertion URN in the filter.

    Uses an isolated dataset/assertion to ensure no state leakage from other tests.
    """
    dataset_urn, assertion_urn = isolated_assertion

    try:
        datahub_client.subscriptions.subscribe(
            urn=assertion_urn,
            subscriber_urn=admin_user_urn,
            skip_actor_exists_check=True,
        )
        wait_for_subscription_sync()
        logger.info(f"Successfully subscribed to assertion: {assertion_urn}")

        # Verify subscription via GraphQL - assertion subscriptions are stored at dataset level
        subscription = verify_subscription_exists(
            graph_client, dataset_urn, expected_actor_urn=admin_user_urn
        )
        change_types = extract_entity_change_types(subscription)

        # Verify default assertion change types are present
        expected_types = {"ASSERTION_PASSED", "ASSERTION_FAILED", "ASSERTION_ERROR"}
        assert change_types == expected_types, (
            f"Expected change types {expected_types}, got {change_types}"
        )

        # Verify the assertion URN is in the filter for each change type
        for change_type in expected_types:
            assertion_filter = extract_assertion_filters(subscription, change_type)
            assert assertion_filter is not None, (
                f"Expected assertion filter for {change_type}"
            )
            assert assertion_urn in assertion_filter, (
                f"Expected {assertion_urn} in filter for {change_type}, "
                f"got {assertion_filter}"
            )
        logger.info(f"Verified assertion subscription via GraphQL: {change_types}")

    finally:
        datahub_client.subscriptions.unsubscribe(
            urn=assertion_urn,
            subscriber_urn=admin_user_urn,
        )
        wait_for_subscription_sync()
        logger.info(f"Successfully unsubscribed from assertion: {assertion_urn}")


def test_subscribe_assertion_specific_change_types(
    isolated_assertion: tuple[str, str],
    datahub_client: DataHubClient,
    admin_user_urn: str,
    graph_client: DataHubGraph,
) -> None:
    """Test assertion subscription with specific change types.

    Creates an assertion subscription with only ASSERTION_FAILED type
    and verifies the subscription is persisted correctly via GraphQL.

    Uses an isolated dataset/assertion to ensure no state leakage from other tests.
    """
    dataset_urn, assertion_urn = isolated_assertion
    specific_types = [models.EntityChangeTypeClass.ASSERTION_FAILED]

    try:
        datahub_client.subscriptions.subscribe(
            urn=assertion_urn,
            subscriber_urn=admin_user_urn,
            entity_change_types=specific_types,
            skip_actor_exists_check=True,
        )
        wait_for_subscription_sync()
        logger.info(
            f"Successfully subscribed to assertion with specific types: {assertion_urn}"
        )

        # Verify subscription via GraphQL
        subscription = verify_subscription_exists(
            graph_client, dataset_urn, expected_actor_urn=admin_user_urn
        )
        change_types = extract_entity_change_types(subscription)

        # Verify only ASSERTION_FAILED is present
        expected_types = {"ASSERTION_FAILED"}
        assert change_types == expected_types, (
            f"Expected change types {expected_types}, got {change_types}"
        )

        # Verify the assertion URN is in the filter
        assertion_filter = extract_assertion_filters(subscription, "ASSERTION_FAILED")
        assert assertion_filter is not None, (
            "Expected assertion filter for ASSERTION_FAILED"
        )
        assert assertion_urn in assertion_filter, (
            f"Expected {assertion_urn} in filter, got {assertion_filter}"
        )
        logger.info(f"Verified assertion subscription via GraphQL: {change_types}")

    finally:
        datahub_client.subscriptions.unsubscribe(
            urn=assertion_urn,
            subscriber_urn=admin_user_urn,
        )
        wait_for_subscription_sync()
        logger.info(f"Successfully unsubscribed from assertion: {assertion_urn}")


# =============================================================================
# Unsubscribe Tests
# =============================================================================


def test_unsubscribe_nonexistent_is_noop(
    test_data: dict,
    datahub_client: DataHubClient,
    admin_user_urn: str,
) -> None:
    """Test that unsubscribing from non-existent subscription is a no-op.

    Attempts to unsubscribe from an entity that has no subscription
    and verifies no error is raised.
    """
    dataset_urn = test_data["dataset_urn"]

    # Unsubscribe should not raise an error even if no subscription exists
    datahub_client.subscriptions.unsubscribe(
        urn=dataset_urn,
        subscriber_urn=admin_user_urn,
    )
    wait_for_subscription_sync()

    logger.info("Unsubscribe from nonexistent subscription succeeded (no-op)")


# =============================================================================
# Error Case Tests
# =============================================================================


def test_subscribe_invalid_urn_raises_error(
    datahub_client: DataHubClient,
    admin_user_urn: str,
) -> None:
    """Test that subscribing with an invalid URN raises SdkUsageError."""
    invalid_urn = "urn:li:invalid:something"

    with pytest.raises(SdkUsageError) as exc_info:
        datahub_client.subscriptions.subscribe(
            urn=invalid_urn,
            subscriber_urn=admin_user_urn,
            skip_actor_exists_check=True,
        )

    assert_error_contains_keywords(exc_info, "urn", "unsupported")


def test_subscribe_empty_change_types_raises_error(
    test_data: dict,
    datahub_client: DataHubClient,
    admin_user_urn: str,
) -> None:
    """Test that subscribing with empty change types list raises SdkUsageError."""
    dataset_urn = test_data["dataset_urn"]

    with pytest.raises(SdkUsageError) as exc_info:
        datahub_client.subscriptions.subscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
            entity_change_types=[],
            skip_actor_exists_check=True,
        )

    assert_error_contains_keywords(exc_info, "empty")


def test_subscribe_assertion_with_non_assertion_change_types_raises_error(
    test_assertion: str,
    datahub_client: DataHubClient,
    admin_user_urn: str,
) -> None:
    """Test that assertion subscription with non-assertion change types raises error."""
    assertion_urn = test_assertion

    with pytest.raises(SdkUsageError) as exc_info:
        datahub_client.subscriptions.subscribe(
            urn=assertion_urn,
            subscriber_urn=admin_user_urn,
            entity_change_types=[models.EntityChangeTypeClass.DEPRECATED],
            skip_actor_exists_check=True,
        )

    assert_error_contains_keywords(exc_info, "assertion")


# =============================================================================
# End-to-End Lifecycle Test
# =============================================================================


def test_subscription_lifecycle(
    isolated_dataset: str,
    datahub_client: DataHubClient,
    admin_user_urn: str,
    graph_client: DataHubGraph,
) -> None:
    """Test subscription lifecycle: create -> update -> delete.

    This test exercises the basic subscription workflow: creating a
    subscription, updating it by adding change types, partially
    unsubscribing, and finally fully unsubscribing. Each step is verified
    via GraphQL to ensure the data matches expectations.

    Uses an isolated dataset to ensure no state leakage from other tests.
    """
    dataset_urn = isolated_dataset
    test_id = generate_unique_test_id()
    logger.info(f"Starting lifecycle test {test_id}")

    try:
        # Step 1: Create initial subscription with DEPRECATED
        logger.info("Step 1: Creating initial subscription")
        datahub_client.subscriptions.subscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
            entity_change_types=[models.EntityChangeTypeClass.DEPRECATED],
            skip_actor_exists_check=True,
        )
        wait_for_subscription_sync()

        # Verify Step 1
        subscription = verify_subscription_exists(
            graph_client, dataset_urn, expected_actor_urn=admin_user_urn
        )
        change_types = extract_entity_change_types(subscription)
        assert change_types == {"DEPRECATED"}, (
            f"Step 1: Expected {{'DEPRECATED'}}, got {change_types}"
        )
        logger.info("Step 1 completed: Initial subscription verified")

        # Step 2: Add OWNER_ADDED change type (merge)
        logger.info("Step 2: Updating subscription with additional change type")
        datahub_client.subscriptions.subscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
            entity_change_types=[models.EntityChangeTypeClass.OWNER_ADDED],
            skip_actor_exists_check=True,
        )
        wait_for_subscription_sync()

        # Verify Step 2 - should have both change types
        subscription = verify_subscription_exists(
            graph_client, dataset_urn, expected_actor_urn=admin_user_urn
        )
        change_types = extract_entity_change_types(subscription)
        assert change_types == {"DEPRECATED", "OWNER_ADDED"}, (
            f"Step 2: Expected {{'DEPRECATED', 'OWNER_ADDED'}}, got {change_types}"
        )
        logger.info("Step 2 completed: Additional change type verified")

        # Step 3: Partial unsubscribe (remove DEPRECATED)
        logger.info("Step 3: Partial unsubscribe (remove DEPRECATED)")
        datahub_client.subscriptions.unsubscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
            entity_change_types=[models.EntityChangeTypeClass.DEPRECATED],
        )
        wait_for_subscription_sync()

        # Verify Step 3 - should only have OWNER_ADDED
        subscription = verify_subscription_exists(
            graph_client, dataset_urn, expected_actor_urn=admin_user_urn
        )
        change_types = extract_entity_change_types(subscription)
        assert change_types == {"OWNER_ADDED"}, (
            f"Step 3: Expected {{'OWNER_ADDED'}}, got {change_types}"
        )
        logger.info("Step 3 completed: Partial unsubscribe verified")

        logger.info(f"Lifecycle test {test_id} completed successfully")

    finally:
        # Complete cleanup
        logger.info("Cleanup: Complete unsubscribe")
        datahub_client.subscriptions.unsubscribe(
            urn=dataset_urn,
            subscriber_urn=admin_user_urn,
        )
        wait_for_subscription_sync()

        # Verify cleanup - subscription should be removed
        verify_subscription_removed(graph_client, dataset_urn)
        logger.info("Cleanup completed and verified")
